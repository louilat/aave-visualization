from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import requests
import numpy as np


def get_hf_per_decile(client_s3, snapshot_day: datetime):
    users_emodes = DataFrame()
    day = datetime(2023, 1, 27)
    print("Starting emodes collection...")
    while day <= snapshot_day:
        day_str_ = day.strftime("%Y-%m-%d")
        active_emodes = pd.read_csv(
            client_s3.get_object(
                Bucket="projet-datalab-group-jprat",
                Key=f"aave-raw-datasource/daily-users-balances/users_balances_snapshot_date={day_str_}/active_users_emodes.csv",
            )["Body"]
        )
        users_emodes = (
            pd.concat((users_emodes, active_emodes))
            .sort_values("snapshot_block")
            .reset_index(drop=True)
            .drop_duplicates(subset="active_user_address", keep="last")
        )
        day += timedelta(days=1)
    print("   Done!")

    print("Treating day: ", snapshot_day)
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
    balances = balances.merge(
        users_emodes[["active_user_address", "emode"]],
        how="left",
        left_on="user_address",
        right_on="active_user_address",
    )
    balances["liqthr"] = np.where(
        balances.emode == 0,
        balances.reserveLiquidationThreshold * 1e-4,
        0.95,
    )
    assert pd.isna(balances.liqthr).sum() == 0
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
    balances["hf_numerator"] = balances.liqthr * balances.currentATokenBalanceUSD
    balances = balances.groupby("user_address", as_index=False).agg(
        {
            "currentATokenBalanceUSD": "sum",
            "currentVariableDebtUSD": "sum",
            "hf_numerator": "sum",
        }
    )
    balances = balances[balances.currentATokenBalanceUSD > 100]
    balances["hf"] = np.where(
        balances.currentVariableDebtUSD == 0,
        1e6,
        balances.hf_numerator
        / np.where(
            balances.currentVariableDebtUSD != 0,
            balances.currentVariableDebtUSD,
            np.nan,
        ),
    )
    balances["category"] = np.select(
        condlist=[
            balances.hf < 1,
            (balances.hf >= 1) & (balances.hf < 1.25),
            (balances.hf >= 1.25) & (balances.hf < 1.50),
            (balances.hf >= 1.50) & (balances.hf < 2.00),
            (balances.hf >= 2) & (balances.hf < 3),
            (balances.hf >= 3) & (balances.hf < 4),
            (balances.hf >= 4) & (balances.hf < 5),
            (balances.hf >= 5) & (balances.hf < 100),
            (balances.hf >= 100),
        ],
        choicelist=[
            "[0, 1[",
            "[1, 1.25[",
            "[1.25, 1.50[",
            "[1.50, 2.00[",
            "[2.00, 3.00[",
            "[3.00, 4.00[",
            "[4.00, 5.00[",
            "[5.00, 100[",
            "[100, +inf[",
        ],
        default=np.nan,
    )

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
    balances = balances.groupby(["decile", "category"], as_index=False).agg(
        {"hf": "count"}
    )
    balances["hf_ratio"] = balances.hf / balances.groupby("decile")["hf"].transform(
        "sum"
    )

    return balances
