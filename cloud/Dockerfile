# The cloud peer for the experiment harness: the same code that runs in
# Lambda, serving plain HTTP against DynamoDB Local. Built from the repo root:
#   docker build -t holdfast-cloud:dev -f cloud/Dockerfile .
FROM golang:1.27.3-trixie@sha256:c65ecfe613b539ac74c62a4c3b2c27e8f8a6076384c8f27e8a3c0f3be17cc385 AS build
WORKDIR /src
COPY go.mod ./
COPY internal ./internal
COPY cloud/go.mod cloud/go.sum ./cloud/
WORKDIR /src/cloud
RUN go mod download
COPY cloud/*.go ./
RUN CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -ldflags="-s -w" -o /out/cloud .

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2
# iproute2 for tc netem, so the harness can impair the relay's mesh link
# like any node's; the peer itself runs unprivileged.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/cloud /usr/local/bin/cloud
COPY harness/netem.sh /usr/local/bin/netem
RUN chmod 0755 /usr/local/bin/netem
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/cloud"]
