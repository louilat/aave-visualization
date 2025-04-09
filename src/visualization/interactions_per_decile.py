from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import requests
import numpy as np


def get_interactions_count(start: datetime, stop: datetime) -> DataFrame:
    interactions_count = DataFrame()
    events_list = ["supply", "borrow", "withdraw", "repay"]
    day = start
    while day <= stop:
        month = day.ctime()[4:7]
        day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
        for event in events_list:
            resp = requests.get(
                url=f"https://aavedata.lab.groupe-genes.fr/events/{event}",
                params={"date": day_str},
                verify=False,
            )
            events = pd.json_normalize(resp.json())
            if event in ["supply", "borrow"]:
                events = events[["onBehalfOf"]].rename(
                    columns={"onBehalfOf": "user_address"}
                )
            else:
                events = events[["user"]].rename(columns={"user": "user_address"})
            events["count"] = 1
            interactions_count = pd.concat((interactions_count, events))
            interactions_count = interactions_count.groupby(
                "user_address", as_index=False
            ).agg({"count": "sum"})
        day += timedelta(days=1)
    return interactions_count


def interactions_per_decile(
    snapshot_day: datetime, interactions_count: DataFrame
) -> DataFrame:
    # Fetch data corresponding to snapshot day
    month = snapshot_day.ctime()[4:7]
    day_str = "-".join(
        [snapshot_day.strftime("%Y"), month, snapshot_day.strftime("%d")]
    )
    resp = requests.get(
        url="https://aavedata.lab.groupe-genes.fr/users-balances",
        params={"date": day_str},
        verify=False,
    )
    balances = pd.json_normalize(resp.json())
    resp = requests.get(
        url="https://aavedata.lab.groupe-genes.fr/reserves",
        params={"date": day_str},
        verify=False,
    )
    reserves = pd.json_normalize(resp.json())
    balances = balances.merge(
        reserves[
            [
                "underlyingAsset",
                "liquidityIndex",
                "variableBorrowIndex",
                "underlyingTokenPriceUSD",
                "reserveLiquidationThreshold",
            ]
        ],
        how="left",
        on="underlyingAsset",
    )
    balances["currentATokenBalanceUSD"] = (
        balances.scaledATokenBalance.apply(int)
        / 10**balances.decimals
        * balances.liquidityIndex.apply(int)
        * 1e-27
        * balances.underlyingTokenPriceUSD
    )
    balances["currentVariableDebtUSD"] = (
        balances.scaledVariableDebt.apply(int)
        / 10**balances.decimals
        * balances.variableBorrowIndex.apply(int)
        * 1e-27
        * balances.underlyingTokenPriceUSD
    )

    # Groupby user to get total deposit/borrow per user
    balances = balances.groupby("user_address", as_index=False).agg(
        {"currentATokenBalanceUSD": "sum", "currentVariableDebtUSD": "sum"}
    )
    balances = balances[balances.currentATokenBalanceUSD > 100]

    # Compute deciles of deposits
    balances["decile"] = None
    for k in range(10):
        balances["decile"] = np.where(
            (
                balances.currentATokenBalanceUSD
                >= np.quantile(balances.currentATokenBalanceUSD, k / 10)
            )
            & (
                balances.currentATokenBalanceUSD
                <= np.quantile(balances.currentATokenBalanceUSD, (k + 1) / 10)
            ),
            k + 1,
            balances.decile,
        )

    # Merge balances with interactions_count and create categories
    balances = balances.merge(interactions_count, how="left", on="user_address")
    balances["count"] = balances["count"].fillna(0)
    balances["category"] = np.select(
        condlist=[
            balances["count"] < 6,
            (balances["count"] >= 6) & (balances["count"] < 6 * 30.5 / 7),
            (balances["count"] >= 6 * 30.5 / 7) & (balances["count"] < 6 * 30.5),
            (balances["count"] >= 6 * 30.5) & (balances["count"] < 6 * 30.5 * 5),
            (balances["count"] >= 6 * 30.5 * 5),
        ],
        choicelist=[
            "< 1/month",
            "1/month to 1/week",
            "1/week to 1/day",
            "1/day to 5/day",
            "> 5/day",
        ],
        default=np.nan,
    )

    # Groupby decile-category and count
    balances = balances.groupby(["decile", "category"], as_index=False).agg(
        {"count": "count"}
    )
    balances["count_ratio"] = balances["count"] / balances.groupby("decile")[
        "count"
    ].transform("sum")
    return balances
