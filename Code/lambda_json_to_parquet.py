"""
Lambda ETL (replaces Glue jobs - Glue Jobs/Crawlers are blocked on this AWS account).

Triggered by S3 uploads to the raw bucket:
  youtube/raw_statistics_reference_data/*.json  -> cleaned_statistics_reference_data
  youtube/raw_statistics/region=xx/*.csv         -> raw_statistics (cleansed)
Writes Parquet to the cleansed bucket, partitioned by region,
and registers tables in the Glue Data Catalog (yt_cleansed).

Needs the AWSSDKPandas-Python312 layer (pandas + pyarrow + awswrangler).
"""
import json
import os
import re
import urllib.parse

import awswrangler as wr
import boto3
import pandas as pd

s3 = boto3.client("s3")

CLEANSED_ROOT = os.environ["S3_CLEANSED_ROOT"]  # s3://<cleansed-bucket>/youtube/
GLUE_DB = os.environ["GLUE_DB_CLEANSED"]


def get_region(key):
    m = re.search(r"region=([a-zA-Z]{2})", key)
    if m:
        return m.group(1).lower()
    return os.path.basename(key)[:2].lower()  # CA_category_id.json / CAvideos.csv


def process_json(bucket, key, region):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    data = json.loads(body.decode("utf-8-sig"))

    # Flatten items[] -> id, snippet.title, snippet.assignable, snippet.channelId
    df = pd.json_normalize(data["items"])
    df = df.rename(columns={
        "id": "category_id",
        "snippet.title": "category_title",
        "snippet.assignable": "assignable",
        "snippet.channelId": "channel_id",
    })
    df["category_id"] = df["category_id"].astype("int64")
    df["region"] = region
    df = df[["category_id", "category_title", "assignable", "channel_id", "region"]]

    wr.s3.to_parquet(
        df=df,
        path=CLEANSED_ROOT + "cleaned_statistics_reference_data/",
        dataset=True,
        database=GLUE_DB,
        table="cleaned_statistics_reference_data",
        partition_cols=["region"],
        mode="overwrite_partitions",
    )
    return len(df)


def process_csv(bucket, key, region):
    # description has line breaks inside quotes -> pandas handles this
    df = wr.s3.read_csv(f"s3://{bucket}/{key}", encoding="utf-8", encoding_errors="replace")
    raw_rows = len(df)

    df["trending_date"] = pd.to_datetime(df["trending_date"], format="%y.%d.%m", errors="coerce").dt.date
    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce", utc=True).dt.tz_localize(None)
    df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    for c in ["views", "likes", "dislikes", "comment_count"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ["comments_disabled", "ratings_disabled", "video_error_or_removed"]:
        df[c] = df[c].astype(str).str.lower().eq("true")

    df = (
        df.drop(columns=["thumbnail_link"], errors="ignore")
          .dropna(subset=["video_id", "trending_date"])
          .drop_duplicates(subset=["video_id", "trending_date"])
    )
    df["region"] = region

    wr.s3.to_parquet(
        df=df,
        path=CLEANSED_ROOT + "raw_statistics/",
        dataset=True,
        database=GLUE_DB,
        table="raw_statistics",
        partition_cols=["region"],
        mode="overwrite_partitions",
        dtype={"trending_date": "date"},
    )
    return raw_rows, len(df)


def lambda_handler(event, context):
    results = []
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        region = get_region(key)

        if key.endswith(".json"):
            rows = process_json(bucket, key, region)
            print(f"JSON {key}: {rows} categories -> region={region}")
            results.append({"file": key, "rows": rows})
        elif key.endswith(".csv"):
            raw_rows, clean_rows = process_csv(bucket, key, region)
            print(f"CSV {key}: {raw_rows} raw -> {clean_rows} clean rows, region={region}")
            results.append({"file": key, "raw": raw_rows, "clean": clean_rows})

    return {"statusCode": 200, "body": results}
