"""
Glue Job 1: raw CSV (landing) -> cleansed Parquet, partitioned by region.

Raw layout:  s3://<raw>/youtube/raw_statistics/region=ca/CAvideos.csv
The region=xx folders become a `region` column automatically.
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "TARGET_DB", "TARGET_TABLE"]
)

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# ---------- Extract ----------
# description column contains line breaks -> multiLine is required
raw = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(args["SOURCE_PATH"])
)
print(f"Raw rows: {raw.count()}")

# ---------- Transform ----------
df = (
    raw
    .withColumn("category_id", F.col("category_id").cast("int"))
    .withColumn("views", F.col("views").cast("long"))
    .withColumn("likes", F.col("likes").cast("long"))
    .withColumn("dislikes", F.col("dislikes").cast("long"))
    .withColumn("comment_count", F.col("comment_count").cast("long"))
    # trending_date looks like 17.14.11 -> yy.dd.MM
    .withColumn("trending_date", F.to_date("trending_date", "yy.dd.MM"))
    .withColumn("publish_time", F.to_timestamp("publish_time"))
    .withColumn("comments_disabled", F.col("comments_disabled").cast("boolean"))
    .withColumn("ratings_disabled", F.col("ratings_disabled").cast("boolean"))
    .withColumn("video_error_or_removed", F.col("video_error_or_removed").cast("boolean"))
    .withColumn("region", F.lower(F.col("region")))
    # drop broken rows and duplicates
    .filter(F.col("video_id").isNotNull() & F.col("trending_date").isNotNull())
    .dropDuplicates(["video_id", "trending_date", "region"])
    .drop("thumbnail_link")
)
print(f"Clean rows: {df.count()}")

# ---------- Load ----------
# Clear target first so reruns don't duplicate data
glue.purge_s3_path(args["TARGET_PATH"], {"retentionPeriod": 0})

sink = glue.getSink(
    connection_type="s3",
    path=args["TARGET_PATH"],
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=args["TARGET_DB"], catalogTableName=args["TARGET_TABLE"])
sink.writeFrame(DynamicFrame.fromDF(df, glue, "cleansed_stats"))

job.commit()
