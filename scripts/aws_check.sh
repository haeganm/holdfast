#!/usr/bin/env bash
# After a destroy: show that nothing the stack made is left, then save the
# run's CloudWatch metrics (CloudWatch keeps them after their resources are
# gone). Read-only calls, within CloudWatch's free API allowance. The account
# id is looked up for the budgets call and never printed.
#
#   scripts/aws_check.sh <out dir> <run start ms> <run end ms> <function> <table> <api id>
#
# Take the names from `terraform output` before the destroy, and the run's
# start and end (epoch ms) from around the harness run. Writes
# <out dir>/metrics.json, which scripts/aws_report.py reads from
# results/aws/inputs. Save what this prints as inputs/teardown-check.txt.
set -uo pipefail
[[ $# -eq 6 ]] || { sed -n '7p' "$0" >&2; exit 2; }
out=$1 start_ms=$2 end_ms=$3 fn=$4 tbl=$5 api=$6
export AWS_PROFILE="${AWS_PROFILE:-holdfast}"
mask() { sed -E 's/[0-9]{12}/************/g'; }
iso() { python -I -c "import datetime, sys; print(datetime.datetime.fromtimestamp(int(sys.argv[1]) / 1000, datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))" "$1"; }
start=$(iso $((start_ms - 60000))) end=$(iso $((end_ms + 120000)))

echo "== left behind, by service (each should print nothing)"
echo "lambda:   $(aws lambda list-functions --query "Functions[?starts_with(FunctionName, 'holdfast')].FunctionName" --output text)"
echo "dynamodb: $(aws dynamodb list-tables --query "TableNames[?starts_with(@, 'holdfast')]" --output text)"
echo "api:      $(aws apigatewayv2 get-apis --query "Items[?starts_with(Name, 'holdfast')].Name" --output text)"
echo "iam:      $(aws iam list-roles --query "Roles[?starts_with(RoleName, 'holdfast')].RoleName" --output text)"
# MSYS_NO_PATHCONV: Git Bash would rewrite /aws/... into a Windows path and the call would find nothing.
echo "logs:     $(MSYS_NO_PATHCONV=1 aws logs describe-log-groups --log-group-name-prefix /aws/lambda/holdfast --query 'logGroups[].logGroupName' --output text) $(MSYS_NO_PATHCONV=1 aws logs describe-log-groups --log-group-name-prefix /aws/apigateway/holdfast --query 'logGroups[].logGroupName' --output text)"
echo "budgets:  $(aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)" --query 'Budgets[].BudgetName' --output text)"
echo "tagged project=holdfast (may lag deletes by minutes): $(aws resourcegroupstaggingapi get-resources --tag-filters Key=project,Values=holdfast --query 'ResourceTagMappingList[].ResourceARN' --output text | mask)"

echo "== metrics for $start to $end"
metric() {  # namespace metric statistic dimension...
  local ns=$1 m=$2 stat=$3; shift 3
  aws cloudwatch get-metric-statistics --namespace "$ns" --metric-name "$m" --dimensions "$@" \
    --start-time "$start" --end-time "$end" --period 60 --statistics "$stat" --output json
}
{
  echo "{"
  for m in Invocations Errors Throttles; do echo "\"lambda_$m\": $(metric AWS/Lambda "$m" Sum Name=FunctionName,Value="$fn"),"; done
  echo "\"lambda_ConcurrentExecutions\": $(metric AWS/Lambda ConcurrentExecutions Maximum Name=FunctionName,Value="$fn"),"
  for m in Count 4xx 5xx; do echo "\"api_$m\": $(metric AWS/ApiGateway "$m" Sum Name=ApiId,Value="$api"),"; done
  for m in ConsumedReadCapacityUnits ConsumedWriteCapacityUnits ReadThrottleEvents WriteThrottleEvents; do
    echo "\"ddb_$m\": $(metric AWS/DynamoDB "$m" Sum Name=TableName,Value="$tbl"),"
  done
  echo "\"window\": [\"$start\", \"$end\"]"
  echo "}"
} > "$out/metrics.json"
echo "wrote $out/metrics.json"
