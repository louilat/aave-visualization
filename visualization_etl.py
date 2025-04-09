import boto3
from datetime import datetime, timedelta
import pandas as pd
import os

from visualization.deposits_and_debt_over_time import users_repartition
from visualization.health_factor_over_time import get_hf_over_time
from visualization.asset_to_asset_over_time import asset_to_asset_repartition
from visualization.heath_factor_per_decile import get_hf_per_decile
from visualization.asset_to_asset_per_decile import (
    asset_to_asset_repartition_per_decile,
)
from visualization.interactions_per_decile import (
    get_interactions_count,
    interactions_per_decile,
)


start = datetime(2023, 1, 27)
stop = datetime(2025, 3, 1)

client_s3 = boto3.client(
    "s3",
    endpoint_url="https://" + "minio-simple.lab.groupe-genes.fr",
    aws_access_key_id=os.environ["ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["SECRET_ACCESS_KEY"],
    verify=False,
)

reserves_symbol = pd.read_csv(
    client_s3.get_object(
        Bucket="projet-datalab-group-jprat",
        Key="aave-raw-datasource/daily-users-balances/users_balances_snapshot_date=2025-02-05/reserves_data.csv",
    )["Body"]
)[["underlyingAsset", "symbol"]]


print("STEP 1: Users deposits/debt repartition over time")

deposits_rep, debt_rep = users_repartition(start=start, stop=stop)

deposits_rep.to_csv("dataviz_outputs/deposits_repartition_over_time.csv")
debt_rep.to_csv("dataviz_outputs/debt_repartition_over_time.csv")

print("STEP 2: Health Factor over time")

hf_over_time = get_hf_over_time(client_s3=client_s3, start=start, stop=stop)
hf_over_time.to_csv("dataviz_outputs/hf_over_time.csv")

print("STEP 3: Asset to asset repartion over time")

ata_rep_over_time = asset_to_asset_repartition(
    start=start, stop=stop, reserves_symbol=reserves_symbol
)
ata_rep_over_time.to_csv("dataviz_outputs/ata_rep_over_time.csv")

print("STEP 4: Health Factor per decile")

hf_per_decile = get_hf_per_decile(client_s3=client_s3, snapshot_day=stop)
hf_per_decile.to_csv("dataviz_outputs/hf_per_decile.csv")

print("STEP 5: Asset to asset repartition per decile")

ata_rep_per_decile = asset_to_asset_repartition_per_decile(
    snapshot_day=stop, reserves_symbol=reserves_symbol
)
ata_rep_per_decile.to_csv("dataviz_outputs/ata_rep_per_decile.csv")

print("STEP 6: Interactions per decile")

interactions_count = get_interactions_count(
    start=stop - timedelta(days=6 * 31), stop=stop
)

inter_per_decile = interactions_per_decile(
    snapshot_day=stop, interactions_count=interactions_count
)
inter_per_decile.to_csv("dataviz_outputs/inter_per_decile.csv")

print("Done!")
