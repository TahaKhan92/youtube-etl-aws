-- Run in Athena with workgroup yt-etl-wg

-- 1. Sanity checks
SELECT region, COUNT(*) AS rows_cnt FROM yt_cleansed.raw_statistics GROUP BY region;
SELECT * FROM yt_cleansed.cleaned_statistics_reference_data LIMIT 10;

-- 2. Join check (preview of the CTAS below)
SELECT s.title, s.region, r.category_title, s.views
FROM yt_cleansed.raw_statistics s
JOIN yt_cleansed.cleaned_statistics_reference_data r
  ON s.category_id = r.category_id AND s.region = r.region
LIMIT 20;

-- 3. ANALYTICS layer (join) - Athena CTAS (Glue Jobs are blocked on this account)
-- Replace <analytics-bucket> with terraform output analytics_bucket
CREATE TABLE yt_analytics.final_analytics
WITH (format = 'PARQUET',
      external_location = 's3://<analytics-bucket>/youtube/final_analytics/',
      partitioned_by = ARRAY['region'])
AS
SELECT s.video_id, s.trending_date, s.title, s.channel_title, s.category_id,
       COALESCE(r.category_title, 'Unknown') AS category_title,
       s.publish_time, s.views, s.likes, s.dislikes, s.comment_count,
       ROUND(CAST(s.likes + s.dislikes + s.comment_count AS DOUBLE) / GREATEST(s.views, 1), 4) AS engagement_rate,
       s.region
FROM yt_cleansed.raw_statistics s
LEFT JOIN yt_cleansed.cleaned_statistics_reference_data r
  ON s.category_id = r.category_id AND s.region = r.region;

-- 4. KPIs on the analytics table (QuickSight datasets)
-- Views by category per region
SELECT region, category_title, SUM(views) AS total_views
FROM yt_analytics.final_analytics
GROUP BY region, category_title
ORDER BY region, total_views DESC;

-- Top 10 channels by trending days
SELECT channel_title, COUNT(DISTINCT trending_date) AS trending_days, MAX(views) AS peak_views
FROM yt_analytics.final_analytics
GROUP BY channel_title
ORDER BY trending_days DESC
LIMIT 10;

-- Daily trend
SELECT trending_date, SUM(views) AS views, SUM(likes) AS likes
FROM yt_analytics.final_analytics
GROUP BY trending_date
ORDER BY trending_date;

-- Engagement by category
SELECT category_title, ROUND(AVG(engagement_rate), 4) AS avg_engagement
FROM yt_analytics.final_analytics
GROUP BY category_title
ORDER BY avg_engagement DESC;

