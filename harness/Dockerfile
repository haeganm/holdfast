# Node image for the experiment harness. Built from the repo root:
#   docker build -t holdfast:dev -f harness/Dockerfile .
FROM golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/holdfast ./cmd/holdfast

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2
# iproute2 for tc netem, ca-certificates for the live ADS-B feed over HTTPS.
# The harness runs netem as root through docker exec; the node itself runs
# unprivileged.
# upgrade takes the trixie-security fixes newer than the pinned base; the
# image scan in check.sh fails without them.
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get install -y --no-install-recommends iproute2 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/holdfast /usr/local/bin/holdfast
COPY harness/netem.sh /usr/local/bin/netem
RUN chmod 0755 /usr/local/bin/netem
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/holdfast"]
