from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import requests


def _update_asset_to_asset_output(user_balances: DataFrame, asset_to_asset: DataFrame):
    for i, debt_row in user_balances.iterrows():
        for j, collateral_row in user_balances.iterrows():
            asset_to_asset.loc[
                (collateral_row["underlyingAsset"], debt_row["underlyingAsset"]), "debt"
            ] += debt_row["currentVariableDebtUSD"] * collateral_row["collateralRatio"]


def asset_to_asset_repartition(
    start: datetime, stop: datetime, reserves_symbol: DataFrame
) -> DataFrame:
    asset_to_asset_output = DataFrame()

    day = start
    while day <= stop:
        print("Treating day: ", day)
        month = day.ctime()[4:7]
        day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
        day_str_ = day.strftime("%Y-%m-%d")
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
        for user in debt_users_balances.user_address.unique().tolist():
            user_balances = debt_users_balances[
                debt_users_balances.user_address == user
            ]
            _update_asset_to_asset_output(user_balances, ata)
        ata["day"] = day

        asset_to_asset_output = pd.concat((asset_to_asset_output, ata))
        day += timedelta(days=1)
    asset_to_asset_output["debt_ratio"] = (
        asset_to_asset_output.debt
        / asset_to_asset_output.groupby("day")["debt"].transform("sum")
    )
    asset_to_asset_output["category"] = (
        asset_to_asset_output.symbolFrom + "=>" + asset_to_asset_output.symbolTo
    )
    return asset_to_asset_output
