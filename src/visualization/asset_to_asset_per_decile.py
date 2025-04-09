from datetime import datetime
import pandas as pd
from pandas import DataFrame
import requests
import numpy as np


def _update_asset_to_asset_output(
    user_balances: DataFrame, asset_to_asset: DataFrame
) -> None:
    for i, debt_row in user_balances.iterrows():
        for j, collateral_row in user_balances.iterrows():
            asset_to_asset.loc[
                (collateral_row["underlyingAsset"], debt_row["underlyingAsset"]), "debt"
            ] += debt_row["currentVariableDebtUSD"] * collateral_row["collateralRatio"]


def asset_to_asset_repartition_per_decile(
    snapshot_day: datetime, reserves_symbol: DataFrame
) -> DataFrame:
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
    reserves = reserves.merge(reserves_symbol, how="left", on="underlyingAsset")
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

    balances["total_deposits"] = balances.groupby("user_address")[
        "currentATokenBalanceUSD"
    ].transform("sum")
    balances = balances[balances.total_deposits > 100].copy()

    balances["quantile_"] = np.nan
    for k in range(10):
        balances["quantile_"] = np.where(
            (balances.total_deposits >= np.quantile(balances.total_deposits, k / 10))
            & (
                balances.total_deposits
                <= np.quantile(balances.total_deposits, (k + 1) / 10)
            ),
            k + 1,
            balances.quantile_,
        )
    debt_users_mask = (
        balances.groupby("user_address")["currentVariableDebtUSD"].transform("sum")
        > 100
    )
    debt_users_balances = balances[debt_users_mask].copy()
    debt_users_balances["collateralRatio"] = (
        debt_users_balances.currentATokenBalanceUSD
        / debt_users_balances.groupby("user_address")[
            "currentATokenBalanceUSD"
        ].transform("sum")
    )

    asset_to_asset_output = DataFrame()
    for k in range(10):
        print("Treating decile: ", k + 1)
        ata = (
            reserves[["symbol", "underlyingAsset"]]
            .merge(
                reserves[["symbol", "underlyingAsset"]],
                how="cross",
                suffixes=["From", "To"],
            )
            .set_index(["underlyingAssetFrom", "underlyingAssetTo"])
        )
        ata["debt"] = 0
        decile_debt_users = debt_users_balances[
            debt_users_balances.quantile_ == (k + 1)
        ].copy()
        for user in decile_debt_users.user_address.unique().tolist():
            user_balances = decile_debt_users[decile_debt_users.user_address == user]
            _update_asset_to_asset_output(user_balances, ata)
        ata["decile"] = k + 1
        ata["debt_ratio"] = ata.debt / ata.debt.sum()
        ata["category"] = ata.symbolFrom + "=>" + ata.symbolTo
        asset_to_asset_output = pd.concat((asset_to_asset_output, ata))
    return asset_to_asset_output
