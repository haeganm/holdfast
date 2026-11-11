# The cloud peer for the experiment harness: the same code that runs in
# Lambda, serving plain HTTP against DynamoDB Local. Built from the repo root:
#   docker build -t holdfast-cloud:dev -f cloud/Dockerfile .
FROM golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80 AS build
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
# upgrade takes the trixie-security fixes newer than the pinned base; the
# image scan in check.sh fails without them.
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get install -y --no-install-recommends iproute2 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/cloud /usr/local/bin/cloud
COPY harness/netem.sh /usr/local/bin/netem
RUN chmod 0755 /usr/local/bin/netem
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/cloud"]
