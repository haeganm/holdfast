# holdfast cloud peer

The same node code as the edge nodes, running as one Go Lambda behind an API Gateway HTTP API, with tracks stored in DynamoDB. Edge nodes use it as a relay.

## What it creates

- DynamoDB table, on demand, hash key `icao`, TTL on `expires`
- Lambda (`provided.al2023`, arm64, 128 MB, 10 s), no reserved or provisioned concurrency
- IAM role limited to `dynamodb:Scan` and `dynamodb:PutItem` on the table, plus log writes to its own log group
- Two CloudWatch log groups (Lambda and API access logs), 7 day retention
- HTTP API with a `$default` route and stage, throttled by `burst` and `rate`
- A monthly cost budget that emails at 80% of the limit (it alerts, it does not cap spend)

Everything is tagged `project = holdfast`. State is local.

## Build the zip

The Lambda needs a `bootstrap` binary at `../build/bootstrap` (or set `bootstrap_path`). Terraform zips it and sets mode 0755, so building on Windows is fine:

    cd cloud && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -o build/bootstrap .

## Apply and destroy

    export TF_VAR_peer_token=$(python -c "import secrets; print(secrets.token_hex(32))") TF_VAR_alert_email=you@example.com
    terraform init
    terraform apply
    terraform destroy && rm -f terraform.tfstate terraform.tfstate.backup

Needs AWS credentials. `api_url` is the relay address for the edge nodes. The token goes in through the environment, never `-var`, so it stays out of the process list and the shell history. It still ends up in the Lambda's environment and in the state, and the backup Terraform keeps after a destroy still holds it, so both files go once the stack is gone.

## Deploy, measure, destroy

What keeps the bill small, since an AWS budget only emails, hours after the spend:

- The stack lives for one sitting: apply, measure for 10 minutes, destroy, then check that nothing tagged `project = holdfast` is left (`aws resourcegroupstaggingapi get-resources --tag-filters Key=project,Values=holdfast` should list nothing).
- Rate limits: the stage throttle (`rate`, `burst`) slows requests, but AWS applies it on a best-effort basis, as a target rather than a ceiling. `max_read_units` / `max_write_units` cap the table. A request without the token gets a 401 before anything touches the table, though API Gateway and Lambda still bill for it. So the teardown is what bounds the bill, not the throttle.
- A separate account budget, made once and kept after the stack is gone, emails at $1, $2.50 and $4 of actual spend and at $5 forecast (a forecast alert needs a few weeks of billing history before it can fire). The stack's own budget goes away with the stack.
- Credentials are short-lived Identity Center sessions (1 hour), never access keys; `aws sso logout` at the end clears the cached ones.

Prices as AWS lists them for us-east-1 (checked 2026-11-05): HTTP API requests $1.00 per million; Lambda requests $0.20 per million, and 400,000 GB-seconds a month in the free tier; DynamoDB on demand $0.125 per million reads and $0.625 per million writes, where a strongly consistent Scan costs 1 read unit per 4 KB of items evaluated and a failed conditional write still costs its unit; CloudWatch Logs $0.50 per GB ingested, 5 GB a month free. The measured 10-minute run of three edges made 1,758 requests and came to about $0.004 at these prices with no free tier, logs included ([`results/aws/aws-run.json`](../../results/aws/aws-run.json)).

Credentials come from IAM Identity Center (`aws configure sso --profile holdfast`, then `aws sso login --profile holdfast`). Terraform runs from the image `check.sh` pins, so nothing else is installed. From the repo root, in Git Bash:

    export AWS_PROFILE=holdfast TF_IMAGE=$(sed -n 's/^TF_IMAGE="\(.*\)"/\1/p' check.sh)
    export TF_VAR_peer_token=$(python -c "import secrets; print(secrets.token_hex(32))") TF_VAR_alert_email=you@example.com
    tf() { MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W)/cloud:/cloud" -v "$(cd ~ && pwd -W)/.aws:/root/.aws" \
             -e AWS_PROFILE -e TF_VAR_peer_token -e TF_VAR_alert_email -w /cloud/terraform "$TF_IMAGE" "$@"; }
    (cd cloud && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -o build/bootstrap .)
    tf init
    tf plan -out=demo.tfplan   # read it
    tf apply demo.tfplan; rm -f cloud/terraform/demo.tfplan   # a saved plan carries the token, so it goes whatever apply did
    export HOLDFAST_CLOUD_URL=$(tf output -raw api_url) HOLDFAST_CLOUD_TOKEN=$TF_VAR_peer_token
    fn=$(tf output -raw function_name) tbl=$(tf output -raw table_name) lg=$(tf output -raw log_group)
    api=${HOLDFAST_CLOUD_URL#https://}; api=${api%%.*}; in=results/aws/inputs
    curl -s -o /dev/null -w '%{http_code}\n' -X POST "${HOLDFAST_CLOUD_URL%/}/sync"   # no token: must print 401
    start=$(date +%s000)
    python -I harness/run.py --family aws --repeats 1 --duration 600 --out results/aws
    end=$(date +%s000)
    MSYS_NO_PATHCONV=1 aws logs filter-log-events --log-group-name "$lg" --start-time "$start" --output json | gzip -n > $in/lambda-logs.json.gz   # the destroy deletes them
    tf destroy -auto-approve && rm -f cloud/terraform/terraform.tfstate cloud/terraform/terraform.tfstate.backup
    bash scripts/aws_check.sh $in "$start" "$end" "$fn" "$tbl" "$api" | tee $in/teardown-check.txt   # nothing left, then the run's metrics
    aws sso logout

`-e NAME` with no value hands the container the variable from this shell, so the token never appears on a command line. The container has no terminal, so Terraform can't ask for a yes: apply takes the saved plan, which is what was read, and destroy takes `-auto-approve`.

The harness run is three edges on live ADS-B that relay only through the deployed peer. It never polls the peer during the run, since each poll would be an invocation and a table scan; at the end it reads the peer's hash beside the edges' (up to five times, since each side expires an aircraft on its own tick), and `cloud_matches` in `summary.csv` says whether the table ended equal to the edges. Invocations and throttles come from CloudWatch for the run's window, durations and cold starts from the Lambda's own log lines, which is why they're saved before the destroy. [`scripts/aws_report.py`](../../scripts/aws_report.py) turned the 2026-11-06 sitting's exports into [`results/aws/aws-run.json`](../../results/aws/aws-run.json); it reads that sitting's files, two deploys included, so a new sitting would adapt it.

## Tests

No credentials needed:

    terraform init -backend=false
    terraform fmt -check -recursive
    terraform validate
    terraform test

## Idle cost

Effectively zero. DynamoDB on demand, Lambda and HTTP API bill per request, and there is nothing provisioned. Log storage is a few cents at most and expires after a week.
