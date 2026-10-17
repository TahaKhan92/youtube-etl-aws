#!/usr/bin/env bash
# Usage: ./upload_to_s3.sh <raw-bucket-name> <folder-with-kaggle-files>
# Example: ./upload_to_s3.sh yt-etl-raw-a1b2c3 ~/Downloads/youtube-new
#
# Uses 6 regions (CA DE FR GB IN US). JP/KR/MX/RU files have encoding
# problems in this dataset - skip them to save time.
set -euo pipefail

BUCKET="$1"
DIR="$2"
REGIONS="CA DE FR GB IN US"

# 1) Statistics CSVs -> Hive-style region partitions
for r in $REGIONS; do
  lr=$(echo "$r" | tr '[:upper:]' '[:lower:]')
  aws s3 cp "$DIR/${r}videos.csv" "s3://$BUCKET/youtube/raw_statistics/region=$lr/${r}videos.csv"
done

# 2) Category JSONs -> reference prefix (each upload triggers the Lambda)
for r in $REGIONS; do
  aws s3 cp "$DIR/${r}_category_id.json" "s3://$BUCKET/youtube/raw_statistics_reference_data/${r}_category_id.json"
done

echo "Done. Check: aws s3 ls s3://$BUCKET/youtube/ --recursive"
