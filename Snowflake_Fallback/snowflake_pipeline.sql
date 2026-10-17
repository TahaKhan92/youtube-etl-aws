-- =====================================================================
-- PLAN B: Glue doesn't run -> keep S3 as the data lake, do ETL in Snowflake.
-- Same 3 zones: RAW (landing) -> CLEANSED -> ANALYTICS
-- Data still sits in the raw S3 bucket (uploaded by scripts/upload_to_s3.sh).
-- If S3 is also unavailable: create an INTERNAL stage and upload files via
-- Snowsight (Data > Add Data > Load files) or `PUT file://... @yt_stage`.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS YT_WH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE DATABASE IF NOT EXISTS YOUTUBE_DB;
CREATE SCHEMA IF NOT EXISTS YOUTUBE_DB.RAW;
CREATE SCHEMA IF NOT EXISTS YOUTUBE_DB.CLEANSED;
CREATE SCHEMA IF NOT EXISTS YOUTUBE_DB.ANALYTICS;
USE WAREHOUSE YT_WH;
USE SCHEMA YOUTUBE_DB.RAW;

-- ---------- Stage (keys = quickest for a 2-hour lab; use a storage integration in real projects)
CREATE OR REPLACE STAGE yt_stage
  URL = 's3://<raw-bucket>/youtube/'
  CREDENTIALS = (AWS_KEY_ID = '<key>' AWS_SECRET_KEY = '<secret>');

LIST @yt_stage;

-- ---------- File formats
CREATE OR REPLACE FILE FORMAT ff_csv
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ENCODING = 'UTF8'
  REPLACE_INVALID_CHARACTERS = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT ff_json TYPE = JSON STRIP_OUTER_ARRAY = FALSE;

-- ---------- RAW: statistics (all strings, as landed)
CREATE OR REPLACE TABLE raw_statistics (
  video_id STRING, trending_date STRING, title STRING, channel_title STRING,
  category_id STRING, publish_time STRING, tags STRING, views STRING, likes STRING,
  dislikes STRING, comment_count STRING, thumbnail_link STRING, comments_disabled STRING,
  ratings_disabled STRING, video_error_or_removed STRING, description STRING,
  region STRING, src_file STRING, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO raw_statistics (
  video_id, trending_date, title, channel_title, category_id, publish_time, tags,
  views, likes, dislikes, comment_count, thumbnail_link, comments_disabled,
  ratings_disabled, video_error_or_removed, description, region, src_file)
FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
         REGEXP_SUBSTR(METADATA$FILENAME, 'region=([a-z]{2})', 1, 1, 'e'),
         METADATA$FILENAME
  FROM @yt_stage/raw_statistics/
)
FILE_FORMAT = (FORMAT_NAME = ff_csv)
PATTERN = '.*videos[.]csv'
ON_ERROR = CONTINUE;

-- ---------- RAW: category JSON (one VARIANT row per file)
CREATE OR REPLACE TABLE raw_category_json (
  src_file STRING, data VARIANT, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO raw_category_json (src_file, data)
FROM (SELECT METADATA$FILENAME, $1 FROM @yt_stage/raw_statistics_reference_data/)
FILE_FORMAT = (FORMAT_NAME = ff_json)
PATTERN = '.*[.]json';

-- ---------- CLEANSED (= Glue Job 1 + Lambda)
CREATE OR REPLACE TABLE YOUTUBE_DB.CLEANSED.statistics AS
SELECT
  video_id,
  TRY_TO_DATE(trending_date, 'YY.DD.MM')          AS trending_date,
  title,
  channel_title,
  TRY_TO_NUMBER(category_id)                      AS category_id,
  TRY_TO_TIMESTAMP_NTZ(publish_time)              AS publish_time,
  tags,
  TRY_TO_NUMBER(views)                            AS views,
  TRY_TO_NUMBER(likes)                            AS likes,
  TRY_TO_NUMBER(dislikes)                         AS dislikes,
  TRY_TO_NUMBER(comment_count)                    AS comment_count,
  TRY_TO_BOOLEAN(comments_disabled)               AS comments_disabled,
  TRY_TO_BOOLEAN(ratings_disabled)                AS ratings_disabled,
  TRY_TO_BOOLEAN(video_error_or_removed)          AS video_error_or_removed,
  region
FROM raw_statistics
WHERE video_id IS NOT NULL
  AND TRY_TO_DATE(trending_date, 'YY.DD.MM') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY video_id, trending_date, region ORDER BY loaded_at DESC) = 1;

CREATE OR REPLACE TABLE YOUTUBE_DB.CLEANSED.category_reference AS
SELECT
  i.value:id::INT                 AS category_id,
  i.value:snippet:title::STRING   AS category_title,
  i.value:snippet:assignable::BOOLEAN AS assignable,
  LOWER(SPLIT_PART(SPLIT_PART(src_file, '/', -1), '_', 1)) AS region
FROM raw_category_json,
     LATERAL FLATTEN(input => data:items) i;

-- ---------- ANALYTICS (= Glue Job 2)
CREATE OR REPLACE TABLE YOUTUBE_DB.ANALYTICS.final_analytics AS
SELECT
  s.*,
  COALESCE(r.category_title, 'Unknown') AS category_title,
  ROUND((s.likes + s.dislikes + s.comment_count) / GREATEST(s.views, 1), 4) AS engagement_rate
FROM YOUTUBE_DB.CLEANSED.statistics s
LEFT JOIN YOUTUBE_DB.CLEANSED.category_reference r
  ON s.category_id = r.category_id AND s.region = r.region;

-- ---------- Checks + KPIs (connect QuickSight / Power BI / Snowsight dashboard here)
SELECT region, COUNT(*) FROM YOUTUBE_DB.ANALYTICS.final_analytics GROUP BY region;

SELECT region, category_title, SUM(views) AS total_views
FROM YOUTUBE_DB.ANALYTICS.final_analytics
GROUP BY 1, 2 ORDER BY 1, 3 DESC;

SELECT channel_title, COUNT(DISTINCT trending_date) AS trending_days
FROM YOUTUBE_DB.ANALYTICS.final_analytics
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
