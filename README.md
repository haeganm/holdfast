# holdfast

Edge nodes that keep one shared picture of the aircraft around them when the network between them drops packets or cuts out, and a harness that measures how well that works.

Work in progress. No measured results yet; this README will carry them, with the scripts that produced them, once the harness runs.

## What it does

Each node ingests aircraft position reports and keeps a replica of the fused picture: a map from aircraft to a last-writer-wins register stamped with a hybrid logical clock. Nodes sync by anti-entropy over HTTP (send a digest, receive what you are missing, send what the peer is missing). The map is a CRDT, so replicas converge however messages are lost, delayed, reordered or repeated.

## What it will measure

Three nodes in Docker, with `tc netem` dropping packets or cutting a node off:

- time for replicas to agree again after a partition heals
- how far behind the freshest report each node's view runs (p50, p95, p99)
- sync bytes per observation
- how often a merge keeps the older report, under the hybrid clock and under a plain wall clock, with one node's clock skewed

## Run the checks

```
./check.sh
```

## License

MIT
