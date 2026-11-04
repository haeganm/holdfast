# holdfast

Edge nodes that keep one shared picture of the aircraft around them when the network between them drops packets or cuts out, and a harness that measures how well that works.

Work in progress. No measured results yet; this README will carry them, with the scripts that produced them, once the harness runs. The predictions they will be checked against are already committed: [docs/predictions.md](docs/predictions.md).

## What it does

Each node ingests aircraft position reports and keeps a replica of the fused picture: a map from aircraft to a last-writer-wins register stamped with a hybrid logical clock. Nodes sync by anti-entropy over HTTP (send a digest, receive what you are missing, send what the peer is missing). The map is a CRDT, so replicas converge whatever order messages arrive in and however often they are lost or repeated, provided that:

- every pair of nodes eventually completes a sync round, directly or through other nodes
- the spread between the fastest and slowest physical clock stays under the max drift (5 s by default), since a node rejects stamps further ahead of its own clock than that
- the picture stays under 10,000 aircraft, the size one digest may carry

## What it will measure

Three nodes in Docker, with `tc netem` dropping packets or cutting a node off:

- propagation latency: for each report, time until each other node holds it or something fresher (p50, p95, p99)
- time for replicas to agree again after ingest stops, and to catch back up after a partition heals
- sync bytes per observation
- how often a merge keeps the older report, under the hybrid clock and under a plain wall clock, with one node's clock skewed

## Run the checks

```
./check.sh
```

## Not in scope yet

No authentication or TLS between nodes, so any host that can reach a node can write to it. The digest checksum is 64-bit FNV, which is fine against accidents but not against a peer crafting collisions. Both belong with node identity, a later phase.

## License

MIT
