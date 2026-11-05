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

    terraform init
    terraform apply -var peer_token=<32+ chars> -var alert_email=you@example.com
    terraform destroy -var peer_token=<32+ chars> -var alert_email=you@example.com

Needs AWS credentials. `api_url` is the relay address for the edge nodes. `peer_token` ends up in the Lambda environment and in state, so keep the state file private.

## Deploy, measure, destroy

Credentials come from IAM Identity Center (`aws configure sso --profile holdfast`, then `aws sso login --profile holdfast`). Terraform runs from the image `check.sh` pins, so nothing else is installed. From the repo root, in Git Bash:

    export AWS_PROFILE=holdfast TF_IMAGE=$(sed -n 's/^TF_IMAGE="\(.*\)"/\1/p' check.sh)
    tf() { MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W)/cloud:/cloud" -v "$(cd ~ && pwd -W)/.aws:/root/.aws" \
             -e AWS_PROFILE -w /cloud/terraform "$TF_IMAGE" "$@"; }
    (cd cloud && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -o build/bootstrap .)
    export HOLDFAST_CLOUD_TOKEN=$(python -c "import secrets; print(secrets.token_hex(32))")
    tf init
    tf apply -var peer_token="$HOLDFAST_CLOUD_TOKEN" -var alert_email=you@example.com
    export HOLDFAST_CLOUD_URL=$(tf output -raw api_url)
    python -I harness/run.py --family aws --duration 600 --out results/aws
    tf destroy -var peer_token="$HOLDFAST_CLOUD_TOKEN" -var alert_email=you@example.com

The harness run is three edges on live ADS-B that relay only through the deployed peer. It never polls the peer during the run, since each poll would be an invocation and a table scan; at the end it asks the peer for its hash once, and `cloud_matches` in `summary.csv` says whether the table ended equal to the edges. Invocations, durations, cold starts and throttles come from CloudWatch for the run's window.

## Tests

No credentials needed:

    terraform init -backend=false
    terraform fmt -check -recursive
    terraform validate
    terraform test

## Idle cost

Effectively zero. DynamoDB on demand, Lambda and HTTP API bill per request, and there is nothing provisioned. Log storage is a few cents at most and expires after a week.
