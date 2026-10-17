# ETL Pipeline on YouTube Data — AWS S3, Glue, Lambda, Athena

Serverless data lake on AWS for the Kaggle **Trending YouTube Video Statistics** dataset.
Raw CSV + JSON land in S3, get cleansed into Parquet with Glue and Lambda, are joined into an
analytics table, and queried with Athena (QuickSight on top). Infra is provisioned with Terraform.

## Architecture

```
Kaggle files ──aws s3 cp──► S3 RAW (Landing)
                              ├─ youtube/raw_statistics/region=xx/*.csv ──► Glue Job 1 (Spark) ─┐
                              │      └─ Glue Crawler ─► yt_raw catalog                           │
                              └─ youtube/raw_statistics_reference_data/*.json                    ▼
                                     └─ S3 event ─► Lambda (pandas) ──────────────► S3 CLEANSED (Parquet)
                                                                                   yt_cleansed catalog
                                                                                          │
                                                                          Glue Job 2 (join on category_id + region)
                                                                                          ▼
                                                                       S3 ANALYTICS (Parquet) / yt_analytics
                                                                                          ▼
                                                                              Athena ─► QuickSight
```

| Zone | Bucket | Written by | Glue DB |
|---|---|---|---|
| Landing | `yt-etl-raw-*` | `upload_to_s3.sh` | `yt_raw` (crawler) |
| Cleansed | `yt-etl-cleansed-*` | Lambda (CSV + JSON) | `yt_cleansed` |
| Analytics | `yt-etl-analytics-*` | Athena CTAS | `yt_analytics` |

## Repository structure

```
.
├── Terraform/            S3 (3 zones + athena + scripts), IAM roles, Glue DBs/crawler/jobs, Lambda + S3 trigger, Athena workgroup
├── Code/
│   ├── lambda_json_to_parquet.py   JSON categories -> Parquet + Glue table
│   ├── glue_csv_to_parquet.py      Glue Job 1: raw CSV -> cleansed Parquet
│   ├── glue_join_analytics.py      Glue Job 2: join -> analytics Parquet
│   └── athena_queries.sql          checks + KPI queries (+ CTAS plan B)
├── scripts/
│   ├── check_glue.sh               5-min check that Glue/Lambda/Athena work on the account
│   └── upload_to_s3.sh             Kaggle files -> raw bucket
└── Snowflake_Fallback/
    └── snowflake_pipeline.sql      same RAW -> CLEANSED -> ANALYTICS in Snowflake if Glue is blocked
```

## Dataset

Kaggle: https://www.kaggle.com/datasets/datasnaek/youtube-new — one `XXvideos.csv` + one `XX_category_id.json` per region.
This project uses **CA, DE, FR, GB, IN, US** (JP/KR/MX/RU have encoding issues). `category_id` meaning differs by region,
so the join is on `category_id + region`.

## Run it

**Prereqs:** AWS CLI (`aws configure`), Terraform ≥ 1.5, Kaggle files downloaded and unzipped.

```bash
# 0. Check the account can run Glue/Lambda/Athena
bash scripts/check_glue.sh us-east-1

# 1. Infra
cd Terraform
cp terraform.tfvars.example terraform.tfvars   # set pandas_layer_arn
terraform init && terraform apply
RAW=$(terraform output -raw raw_bucket); cd ..

# 2. Load data (JSON uploads trigger the Lambda automatically)
bash scripts/upload_to_s3.sh $RAW ~/Downloads/youtube-new

# 3. Lambda runs automatically on each upload (CSV + JSON -> cleansed Parquet)
# 4. Athena (workgroup yt-etl-wg): run Code/athena_queries.sql
#    - CTAS builds yt_analytics.final_analytics, then the KPI queries

# Cleanup
cd Terraform && terraform destroy
```

Check Lambda: CloudWatch Logs → `/aws/lambda/yt-etl-json-to-parquet`.

## Alternative: Snowflake

`Snowflake_Fallback/snowflake_pipeline.sql` builds the same RAW → CLEANSED → ANALYTICS layers in Snowflake
from the raw S3 bucket, for environments without Lambda/Athena.

## Key transformations
- CSV read with `multiLine` (descriptions contain line breaks), types cast, `trending_date` parsed from `yy.dd.MM`
- Nested JSON `items[].snippet` flattened to `category_id, category_title, region`
- Duplicates removed on `video_id + trending_date + region`
- Parquet, partitioned by `region`; `engagement_rate` added in the analytics table

## Note: Glue Jobs blocked on this account
AWS returned `AccessDeniedException: Account is denied access` for Glue `CreateJob` and `CreateCrawler`
(account-level restriction, not IAM). The Glue Data Catalog still works, so the pipeline was adapted:
- **Lambda** (pandas / AWS SDK for pandas) does the CSV + JSON → Parquet ETL on S3 upload events and registers tables in the Glue Catalog
- **Athena CTAS** does the join into the analytics zone
- The original Glue PySpark scripts are kept in `Code/` (`glue_*.py`) for accounts where Glue jobs are enabled

## Results

**Cleansed layer row counts (after dedup on `video_id + trending_date`):**

| Region | Rows |
|---|---|
| CA | 40,881 |
| DE | 40,840 |
| FR | 40,724 |
| GB | 38,742 |
| IN | 32,458 |
| US | 40,899 |

The analytics CTAS (join of ~234K rows) ran in **4.3 s**, scanning **13.3 MB** of Parquet.

**Insights**
- **Music and Entertainment** are the top two categories in every region; GB is the most Music-heavy (~11.5B views).
- **Late-night shows (Ellen, Fallon, Kimmel, Colbert, Seth Meyers), ESPN and WWE** stayed trending the longest (~205 days).
- **Engagement rate** ((likes + dislikes + comments) / views) is highest for Gaming, Education and Comedy, lowest for Movies, Shows and Trailers.

**Data caveat:** the dataset is a daily snapshot, so a video trending for N days appears N times. Summing `views` directly
inflates totals 2–15x (e.g. GB Music 170B vs 11.5B), so view totals use `MAX(views)` per video:

```sql
SELECT region, category_title, SUM(max_views) AS total_views
FROM (SELECT region, category_title, video_id, MAX(views) AS max_views
      FROM yt_analytics.final_analytics GROUP BY 1,2,3)
GROUP BY 1,2 ORDER BY 1,3 DESC;
```

`Unknown` category = `category_id` values missing from that region's JSON reference file (kept via LEFT JOIN, not dropped).

## Screenshots

| | |
|---|---|
| Lambda with S3 triggers | ![Lambda](screenshots/02_lambda_s3_trigger.png) |
| Glue Data Catalog databases | ![Glue](screenshots/03_glue_data_catalog.png) |
| S3 analytics zone (partitioned by region) | ![S3](screenshots/04_s3_analytics_zone.png) |
| Athena workgroup | ![Workgroup](screenshots/01_athena_workgroup.png) |
| Athena queries | ![Athena](screenshots/05_athena_queries.png) |
