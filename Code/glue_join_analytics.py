"""
Glue Job 2: join cleansed statistics + category reference -> analytics zone.
Join key: category_id + region (category ids differ by region).
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_DB", "STATS_TABLE", "REF_TABLE", "TARGET_PATH", "TARGET_DB", "TARGET_TABLE"],
)

sc = SparkContext()
glue = GlueContext(sc)
job = Job(glue)
job.init(args["JOB_NAME"], args)

stats = glue.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DB"], table_name=args["STATS_TABLE"]
).toDF()

ref = glue.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DB"], table_name=args["REF_TABLE"]
).toDF().select(
    F.col("category_id").cast("int").alias("ref_category_id"),
    "category_title",
    F.col("region").alias("ref_region"),
)

final = (
    stats.join(
        ref,
        (stats.category_id == ref.ref_category_id) & (stats.region == ref.ref_region),
        "left",
    )
    .drop("ref_category_id", "ref_region")
    .withColumn("category_title", F.coalesce("category_title", F.lit("Unknown")))
    .withColumn(
        "engagement_rate",
        F.round((F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.greatest(F.col("views"), F.lit(1)), 4),
    )
)
print(f"Final rows: {final.count()}")

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
sink.writeFrame(DynamicFrame.fromDF(final, glue, "final_analytics"))

job.commit()
