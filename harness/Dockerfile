# Node image for the experiment harness. Built from the repo root:
#   docker build -t holdfast:dev -f harness/Dockerfile .
FROM golang:1.27.3-trixie@sha256:c65ecfe613b539ac74c62a4c3b2c27e8f8a6076384c8f27e8a3c0f3be17cc385 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/holdfast ./cmd/holdfast

FROM debian:trixie-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4
# iproute2 for tc netem. The harness runs netem as root through docker exec;
# the node itself runs unprivileged.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/holdfast /usr/local/bin/holdfast
COPY harness/netem.sh /usr/local/bin/netem
RUN chmod 0755 /usr/local/bin/netem
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/holdfast"]
