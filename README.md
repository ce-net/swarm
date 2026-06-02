# swarm

A distributed work scheduler on the [CE](https://github.com/ce-net/ce) compute mesh — the
first app built on CE, using the [ce-rs](https://github.com/ce-net/ce-rs) SDK.

swarm is a **client**: it discovers hosts from your CE node's atlas, fans a task out across
them over the mesh, and gathers the results. CE provides the substrate (placement, sandboxed
execution, billing, the immutable interaction history); swarm is the orchestration policy on
top — CE's "Ray" / "Kubernetes". See the design in
[`ce/docs/apps/scheduler.md`](https://github.com/ce-net/ce/blob/main/docs/apps/scheduler.md).

## Install

```bash
cargo install --git https://github.com/ce-net/swarm
```

Needs a running local CE node (`ce start`) — swarm talks to it on `http://127.0.0.1:8844`.

## Use

```bash
# Which hosts can run my work?
swarm hosts --select gpu

# Fan a command out across up to 8 GPU hosts and collect every result:
swarm run nvidia/cuda:latest -n 8 --select gpu -- nvidia-smi

# Embarrassingly parallel: run the same job on every Docker host:
swarm run alpine:latest -- sh -c 'echo hello from $(hostname)'
```

`--node <url>` points swarm at a different CE node.

## How it works

1. **Discover** — `GET /atlas` for hosts advertising `docker` (and `--select <tag>`).
2. **Scatter** — `mesh_exec` to each host concurrently, directed over `/ce/rpc/1`.
3. **Gather** — collect each host's stdout / exit code as it returns.

## v0 scope and what's next

v0 is scatter/gather for **one-shot** commands (perceptible/verifiable work — you see the
output). Documented next steps, as CE primitives land:

- **Trust-tiered placement** — rank hosts by per-relationship reputation (needs the CE
  reputation-read index); gate opaque work behind earned trust.
- **Verification dials** — redundancy + random independent placement; spot-checks.
- **Long-running deploy** — directed `mesh_deploy`/`mesh_kill` (already in the SDK) with
  remote status polling.
- **DAGs, retries, coordinator HA** (Raft for the coordinator's own state).

## License

MIT © Leif Rydenfalk
