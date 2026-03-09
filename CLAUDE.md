# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

This is the PocketBase source code with a custom **multi-node active-active cluster replication** layer added on top of the upstream repo. It is not a fork intended for upstreaming — it extends PocketBase with cluster support while keeping all upstream code intact.

## Commands

### Go backend

```sh
# Run (development) — from repo root
cd examples/base && go run main.go serve

# Build standalone binary
cd examples/base && CGO_ENABLED=0 go build

# Run all tests
go test ./...

# Run a single package's tests
go test ./apis/... -v -run TestSomething

# Lint (requires golangci-lint installed)
make lint
# or: golangci-lint run -c ./golangci.yml ./...

# Full test with coverage report
make test-report
```

### Admin UI (Svelte + Vite)

Node is not available in this environment — UI changes require a separate machine to build.

```sh
cd ui
npm install
npm run dev        # dev server at http://localhost:3000 (needs PB backend at :8090)
npm run build      # produces ui/dist/ which is embedded in the Go binary
```

After `npm run build`, the updated `ui/dist/` assets must be committed so the embedded UI is updated.

The dev server expects the PocketBase backend at `http://localhost:8090` by default. Override with `ui/.env.development.local` containing `PB_BACKEND_URL = YOUR_ADDRESS`.

## Architecture

### Upstream PocketBase structure

- `core/` — core types (`App`, `Record`, `Collection`, model hooks, etc.)
- `apis/` — HTTP API handlers; `base.go` is the entry point that registers all route groups
- `cmd/` — CLI commands (`serve`, `migrate`, etc.)
- `examples/base/` — the `main.go` used to build the standalone binary
- `ui/` — Svelte 4 + Vite admin SPA; built assets embedded into the Go binary via `go:embed`
- `tools/` — internal utility packages (hooks, router, routine, etc.)

### Cluster replication layer (added in this fork)

Three files implement the entire cluster feature:

**`tools/cluster/cluster.go`** — Pure Go, no external dependencies.
- `Manager` — owns all SSE connections (inbound from peers + outbound to peers), broadcasts events, drives gossip
- `ReplicationEvent` — wire format (`op`, `table`, `id`, `data`, `origin`, `seq`)
- `MarkReplicating` / `IsReplicating` / `UnmarkReplicating` — package-level `sync.Map` for loop prevention
- `Broadcast(event)` — fan-out to all peers, non-blocking (drops if buffer full)
- `BroadcastToOne(nodeID, event)` — non-blocking send to a single peer
- `SendToPeer(nodeID, event, ctx)` — blocking send to a single peer; used during sync so no rows are dropped

**`apis/cluster.go`** — HTTP layer, apply logic, sync, and replication log.
- Registers three routes under `/api/cluster/`: `GET /events` (SSE stream), `GET /nodes`, `POST /peers`
- `bindClusterReplicationHooks()` — hooks into `OnModelAfterCreate/Update/DeleteSuccess`; skips if `IsReplicating()` (loop prevention) and skips `_logs` / `_cluster_log`
- `bindClusterLogHooks()` — separate hooks that write every model change (local and applied-from-peer) to `_cluster_log` for delta sync
- `ApplyReplication()` — dispatches incoming events with last-write-wins conflict check: skips if local record's `updated` >= incoming `updated`
- `syncAllTablesToPeer()` — full sync to one peer using blocking `SendToPeer`; triggered on first connect
- `deltaSyncToPeer()` — reads `_cluster_log` since peer's last connection time, deduplicates per record, sends only what changed; triggered on reconnect
- `pruneClusterLog()` — background goroutine pruning log entries older than 7 days

**`_cluster_log` table** — node-local SQLite table, never replicated.
- Columns: `table_name TEXT, record_id TEXT, op TEXT, created INTEGER` (Unix nanoseconds)
- Written by `bindClusterLogHooks` on every model change
- Queried by `deltaSyncToPeer` when a reconnecting peer sends `X-Cluster-Since`
- Pruned hourly; 7-day retention window

**`cmd/serve.go`** (modified) — Adds four flags: `--cluster-id`, `--cluster-secret`, `--cluster-self-url`, `--cluster-peers`. Creates and stores the `Manager` in `app.Store()` under `apis.ClusterManagerKey`. Wires `Manager.ApplyFunc` to `apis.ApplyReplication`. Starts the manager inside `OnServe`, stops it in `OnTerminate`.

**`apis/base.go`** (modified) — Calls `bindClusterApi(app, apiGroup)`.

**`ui/src/components/cluster/PageCluster.svelte`** — Admin UI page; polls `GET /api/cluster/nodes` every 5 s.

**`ui/src/routes.js`** and **`ui/src/App.svelte`** (modified) — Add `/cluster` route and sidebar icon.

### Key design decisions

- **No new Go dependencies** — SSE over stdlib `net/http`, no WebSocket, no external broker.
- **Bidirectional SSE** — each node pair has two unidirectional SSE connections (one in each direction). The server-side auto reverse-connects if the peer sends `X-Cluster-Self-URL`.
- **Self-connection detection** — the server always sends `X-Cluster-Node-ID` in every response (including errors); the client compares it against its own node ID and returns `errSelfConnect` to permanently stop retrying.
- **Loop prevention** — `sync.Map` keyed on `"table:id"` is set while `ApplyReplication` runs; hook checks this before broadcasting.
- **Gossip** — on each new inbound connection, the server sends a `peers` event with all currently known peer URLs; the connecting node calls `AddPeer()` on each, forming a full mesh automatically.
- **Conflict resolution** — last-write-wins enforced at apply time: incoming `updated` timestamp must be strictly after the local record's `updated` timestamp, otherwise the event is discarded. Applies to both records and collections.
- **Auto-sync on connect** — when a peer establishes its SSE connection, the server immediately starts a background sync goroutine. First connection → full sync (`syncAllTablesToPeer`). Reconnect with `X-Cluster-Since` header → delta sync (`deltaSyncToPeer` reads `_cluster_log`).
- **Replication log** — `_cluster_log` (node-local, never replicated) records every model change as `(table, record_id, op, created_ns)`. Used only for delta sync on reconnect. Pruned to 7 days.
- **`_logs` and `_cluster_log` excluded** — `_logs` is high-volume and node-local; `_cluster_log` is an internal sync artifact.
- **File storage not replicated** — use S3-compatible object storage for multi-node deployments.

### Running a two-node cluster locally

```sh
# Node A
./pocketbase serve --http=:8090 \
  --cluster-secret=mysecret \
  --cluster-self-url=http://localhost:8090 \
  --cluster-peers=http://localhost:8091

# Node B
./pocketbase serve --http=:8091 \
  --cluster-secret=mysecret \
  --cluster-self-url=http://localhost:8091 \
  --cluster-peers=http://localhost:8090
```

See `CLUSTER.md` for the full reference (topology guide, REST API, nginx setup, failure handling, known limitations).
