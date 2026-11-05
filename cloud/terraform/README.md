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

    GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o cloud/build/bootstrap ./path/to/lambda

## Apply and destroy

    terraform init
    terraform apply -var peer_token=<32+ chars> -var alert_email=you@example.com
    terraform destroy -var peer_token=<32+ chars> -var alert_email=you@example.com

Needs AWS credentials. `api_url` is the relay address for the edge nodes. `peer_token` ends up in the Lambda environment and in state, so keep the state file private.

## Tests

No credentials needed:

    terraform init -backend=false
    terraform fmt -check -recursive
    terraform validate
    terraform test

## Idle cost

Effectively zero. DynamoDB on demand, Lambda and HTTP API bill per request, and there is nothing provisioned. Log storage is a few cents at most and expires after a week.
