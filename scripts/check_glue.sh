#!/usr/bin/env bash
# 5-minute check: does Glue work on this AWS account?
# Run BEFORE terraform apply. If any step fails -> go to Snowflake_Fallback/.
set -u
REGION="${1:-us-east-1}"

echo "1) Who am I"
aws sts get-caller-identity || { echo "FAIL: aws configure not set"; exit 1; }

echo "2) Glue catalog access"
aws glue get-databases --region "$REGION" --max-results 1 \
  && echo "OK: Glue catalog reachable" \
  || echo "FAIL: no Glue access -> use Snowflake fallback"

echo "3) Glue jobs API"
aws glue list-jobs --region "$REGION" --max-results 1 >/dev/null \
  && echo "OK: Glue jobs API reachable (real test = Job 1 run after apply)" \
  || echo "FAIL: no Glue jobs access -> use Snowflake fallback"

echo "4) Lambda access"
aws lambda list-functions --region "$REGION" --max-items 1 >/dev/null \
  && echo "OK: Lambda reachable" || echo "FAIL: no Lambda access"

echo "5) Athena access"
aws athena list-work-groups --region "$REGION" --max-results 1 >/dev/null \
  && echo "OK: Athena reachable" || echo "FAIL: no Athena access"
