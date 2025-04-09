from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import requests
import numpy as np


def users_repartition(start: datetime, stop: datetime) -> tuple[DataFrame, DataFrame]:
    day = start
    deposit_categories = DataFrame()
    debt_categories = DataFrame()
    while day <= stop:
        print("Treating day: ", day)
        month = day.ctime()[4:7]
        day_str = "-".join([day.strftime("%Y"), month, day.strftime("%d")])
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
        balances = balances.groupby("user_address", as_index=False).agg(
            {"currentATokenBalanceUSD": "sum", "currentVariableDebtUSD": "sum"}
        )
        balances = balances[balances.currentATokenBalanceUSD > 100]
        balances["deposit_category"] = np.select(
            condlist=[
                balances.currentATokenBalanceUSD < 1000,
                (balances.currentATokenBalanceUSD >= 1000)
                & (balances.currentATokenBalanceUSD < 10000),
                (balances.currentATokenBalanceUSD >= 10000)
                & (balances.currentATokenBalanceUSD < 100000),
                (balances.currentATokenBalanceUSD >= 100000)
                & (balances.currentATokenBalanceUSD < 1000000),
                balances.currentATokenBalanceUSD >= 1000000,
            ],
            choicelist=[
                "less than 1000",
                "between 1000 and 10000",
                "between 10000 and 100000",
                "between 100000 and 1e6",
                "more than 1e6",
            ],
            default=np.nan,
        )
        balances["debt_category"] = np.select(
            condlist=[
                balances.currentVariableDebtUSD < 1000,
                (balances.currentVariableDebtUSD >= 1000)
                & (balances.currentVariableDebtUSD < 10000),
                (balances.currentVariableDebtUSD >= 10000)
                & (balances.currentVariableDebtUSD < 100000),
                (balances.currentVariableDebtUSD >= 100000)
                & (balances.currentVariableDebtUSD < 1000000),
                balances.currentVariableDebtUSD >= 1000000,
            ],
            choicelist=[
                "less than 1000",
                "between 1000 and 10000",
                "between 10000 and 100000",
                "between 100000 and 1e6",
                "more than 1e6",
            ],
            default=np.nan,
        )
        deposits = balances.groupby("deposit_category", as_index=False).agg(
            {"user_address": "count"}
        )
        debts = balances.groupby("debt_category", as_index=False).agg(
            {"user_address": "count"}
        )
        deposits["day"] = day
        debts["day"] = day
        deposit_categories = pd.concat((deposit_categories, deposits))
        debt_categories = pd.concat((debt_categories, debts))
        day += timedelta(days=1)
    deposit_categories = deposit_categories.rename(columns={"user_address": "count"})
    debt_categories = debt_categories.rename(columns={"user_address": "count"})
    return deposit_categories, debt_categories
