# PocketBase Cluster Replication

> Multi-node active-active replication for PocketBase via Server-Sent Events (SSE).

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Architecture](#architecture)
4. [How Replication Works](#how-replication-works)
5. [Gossip — Automatic Full-Mesh Discovery](#gossip--automatic-full-mesh-discovery)
6. [Late Joining — Adding Nodes After Start](#late-joining--adding-nodes-after-start)
7. [Realtime / Live Updates](#realtime--live-updates)
8. [What Gets Replicated](#what-gets-replicated)
9. [CLI Flags Reference](#cli-flags-reference)
10. [REST API Reference](#rest-api-reference)
11. [Admin Dashboard](#admin-dashboard)
12. [Topology Guide](#topology-guide)
13. [Proxy / Load-Balancer Setup (nginx)](#proxy--load-balancer-setup-nginx)
14. [Security](#security)
15. [Failure Handling & Reconnection](#failure-handling--reconnection)
16. [Known Limitations](#known-limitations)
17. [Internal Code Reference](#internal-code-reference)

---

## Overview

The cluster feature adds **active-active multi-master replication** between two or more PocketBase instances. Any node can accept writes; changes are propagated to all other nodes in near-real-time.

Key properties:

| Property | Value |
|---|---|
| Transport | Server-Sent Events (SSE) over HTTP |
| Replication model | Active-active (any node can write) |
| Conflict resolution | Last-write-wins (based on `updated` timestamp) |
| New dependencies | **None** – uses only the Go standard library |
| Peer discovery | Gossip (nodes share known peer lists on connect) |
| Late joining | Yes – new nodes connect to one existing node; mesh forms automatically |
| Initial sync | Manual (restore a backup on new nodes) or via `SyncAllTables` API |
| Live/realtime forwarding | Yes – SSE clients on any node get updates from all nodes |

---

## Quick Start

### Minimum two-node setup

Both nodes must share the same `--cluster-secret`. Each node must also set `--cluster-self-url` to its own reachable base URL so that peers can connect back and gossip can propagate.

**Node A:**

```bash
./pocketbase serve \
  --http=0.0.0.0:8090 \
  --cluster-secret=my-very-secret-key \
  --cluster-self-url=http://node-a-host:8090 \
  --cluster-peers=http://node-b-host:8091
```

**Node B:**

```bash
./pocketbase serve \
  --http=0.0.0.0:8091 \
  --cluster-secret=my-very-secret-key \
  --cluster-self-url=http://node-b-host:8091 \
  --cluster-peers=http://node-a-host:8090
```

That's it. Once both nodes are running you will see log lines like:

```
[cluster] connected to peer SSE stream  peer=node-b-host-8091  url=http://node-b-host:8091/api/cluster/events
[cluster] peer connected to our SSE stream  peer=node-a-host-8090  addr=<ip>
```

Create a record on Node A — it immediately appears on Node B, and vice versa.

### Three-node setup with gossip (recommended)

With gossip enabled, each new node only needs to know **one** existing node. It will automatically discover and connect to the rest.

```bash
# Node A — the initial bootstrap node, no peers needed at first
./pocketbase serve --http=:8090 \
  --cluster-secret=secret \
  --cluster-self-url=http://a:8090

# Node B — only knows about A
./pocketbase serve --http=:8091 \
  --cluster-secret=secret \
  --cluster-self-url=http://b:8091 \
  --cluster-peers=http://a:8090

# Node C — only knows about A, will discover B via gossip
./pocketbase serve --http=:8092 \
  --cluster-secret=secret \
  --cluster-self-url=http://c:8092 \
  --cluster-peers=http://a:8090
```

After all three start, each node automatically forms a full mesh (A↔B, A↔C, B↔C). No node needs to list all peers upfront.

### Traditional three-node setup (explicit, no gossip)

If you prefer explicit configuration without relying on gossip:

```bash
# Node A
./pocketbase serve --http=:8090 \
  --cluster-secret=secret \
  --cluster-peers=http://b:8091,http://c:8092

# Node B
./pocketbase serve --http=:8091 \
  --cluster-secret=secret \
  --cluster-peers=http://a:8090,http://c:8092

# Node C
./pocketbase serve --http=:8092 \
  --cluster-secret=secret \
  --cluster-peers=http://a:8090,http://b:8091
```

---

## Architecture

### Component map

```
┌───────────────────────────────────────┐
│  tools/cluster/cluster.go             │
│  ─────────────────────────────────    │
│  Manager          – peer lifecycle    │
│  ReplicationEvent – wire format       │
│  IsReplicating()  – loop prevention   │
│  KnownPeerURLs()  – gossip support    │
│  BroadcastToOne() – targeted send     │
│  SendToPeer()     – blocking send     │
└──────────────────┬────────────────────┘
                   │ used by
┌──────────────────▼────────────────────┐
│  apis/cluster.go                      │
│  ─────────────────────────────────    │
│  GET  /api/cluster/events  – SSE srv  │
│  GET  /api/cluster/nodes   – status   │
│  POST /api/cluster/peers   – dyn add  │
│  bindClusterReplicationHooks()        │
│  bindClusterLogHooks()                │
│  ApplyReplication()  (LWW check)      │
│  syncAllTablesToPeer()                │
│  deltaSyncToPeer()                    │
└──────────────────┬────────────────────┘
                   │ started by
┌──────────────────▼────────────────────┐
│  cmd/serve.go                         │
│  ─────────────────────────────────    │
│  --cluster-secret                     │
│  --cluster-peers                      │
│  --cluster-id                         │
│  --cluster-self-url                   │
└───────────────────────────────────────┘

┌───────────────────────────────────────┐
│  _cluster_log  (SQLite, node-local)   │
│  ─────────────────────────────────    │
│  table_name  TEXT                     │
│  record_id   TEXT                     │
│  op          TEXT  (create/update/…)  │
│  created     INTEGER  (Unix ns)       │
│                                       │
│  Written by bindClusterLogHooks       │
│  Read by    deltaSyncToPeer           │
│  Pruned to  7-day retention           │
└───────────────────────────────────────┘
```

### Node-to-node transport

Each node runs **two roles simultaneously**:

- **Server role** – exposes `GET /api/cluster/events`, an infinite SSE stream.
  Authenticated peers connect and receive all future replication events.

- **Client role** – for each URL in `--cluster-peers`, opens an outgoing HTTP connection
  to that peer's `/api/cluster/events` endpoint and reads events.

This means connections are bidirectional without needing WebSocket:

```
Node A                          Node B
  │                               │
  │  ←── GET /api/cluster/events ──│   (B reads A's stream)
  │                               │
  │  ──── GET /api/cluster/events →│   (A reads B's stream)
```

---

## How Replication Works

### Sending a change (originating node)

1. A record is created/updated/deleted through any normal PocketBase path (REST API, JS hooks, migrations, etc.)
2. PocketBase fires the `OnModelAfterCreateSuccess` / `OnModelAfterUpdateSuccess` / `OnModelAfterDeleteSuccess` hook.
3. The cluster hook handler checks:
   - Is cluster mode enabled? (manager stored in `app.Store`)
   - Is this change already a replication echo? (loop prevention)
   - Is the table excluded from replication? (`_logs` and `_cluster_log` are skipped)
4. The model is serialized:
   - `_collections` → JSON marshal of the full `Collection` struct
   - Everything else → raw `SELECT *` row from the DB (captures all columns including auth hashes)
5. A `ReplicationEvent` is written to every connected peer's SSE channel asynchronously (`routine.FireAndForget`).
6. A separate log hook (`bindClusterLogHooks`) also appends a row to `_cluster_log` recording `(table, record_id, op, now_ns)`. This happens for **all** model changes — both locally-originated and applied-from-peer — so the log always reflects the full change history of this node.

### Receiving a change (peer node)

1. The outgoing SSE client goroutine receives a line with `event: replicate`.
2. The JSON payload is decoded into a `ReplicationEvent`.
3. The origin node ID is checked – events originating from this node are discarded (safety net).
4. `ApplyReplication()` is called:

```
ApplyReplication(app, event)
  │
  ├── table == "_collections"?
  │     ├── DELETE → app.Delete(collection)
  │     └── CREATE/UPDATE → json.Unmarshal → compare col.Updated vs existing.Updated
  │                         → skip if existing is same age or newer  (last-write-wins)
  │                         → app.SaveNoValidate(col)
  │                           (PocketBase creates/alters the underlying table)
  │
  └── other table
        ├── collection found in cache?
        │     ├── DELETE → app.FindRecordById → app.Delete(record)
        │     └── CREATE/UPDATE → core.NewRecord → record.Load(data)
        │                         → compare incoming "updated" vs existing "updated"
        │                         → skip if existing is same age or newer  (last-write-wins)
        │                         → app.SaveNoValidate(record)
        │
        └── no collection (e.g. _params)
              └── raw SQL: INSERT OR REPLACE / DELETE
                  + app.ReloadSettings() if table == "_params"
```

5. `app.SaveNoValidate()` and `app.Delete()` fire the normal PocketBase hook chain, which triggers SSE delivery to local realtime clients (see [Realtime / Live Updates](#realtime--live-updates)).

### Loop prevention

A `sync.Map` in `tools/cluster` tracks `"table:id"` keys that are **currently being applied from a peer**. Before `SaveNoValidate`/`Delete` are called the key is registered; it is removed when the function returns (`defer`).

The cluster broadcast hook checks `IsReplicating(key)` before sending. Since the key is set, the hook skips the broadcast — preventing the change from being echoed back to peers.

---

## Gossip — Automatic Full-Mesh Discovery

When a peer node connects to `GET /api/cluster/events`, the server immediately sends it a **gossip event** containing the URLs of all other currently-connected nodes:

```
event: peers
data: {"urls":["http://c:8092","http://d:8093"]}
```

The connecting node receives this event and calls `AddPeer()` for each URL it does not already know about, which triggers outgoing connections to those nodes. Those nodes in turn send their own peer lists, and so on — the full mesh forms automatically.

### How it works step by step

Given nodes A, B, C where only A and B are initially running and C joins later:

```
1. A starts (no peers)

2. B starts with --cluster-peers=http://a:8090
   B → connects to A's /api/cluster/events
   A → sees X-Cluster-Self-URL: http://b:8091
   A → calls AddPeer("http://b:8091") [auto reverse-connect]
   A → sends B gossip: {"urls":[]} (no other peers yet)
   Result: A↔B connected

3. C starts with --cluster-peers=http://a:8090
   C → connects to A's /api/cluster/events
   A → sees X-Cluster-Self-URL: http://c:8092
   A → calls AddPeer("http://c:8092") [auto reverse-connect]
   A → sends C gossip: {"urls":["http://b:8091"]}
   C → receives gossip, calls AddPeer("http://b:8091")
   C → connects to B's /api/cluster/events
   B → sees X-Cluster-Self-URL: http://c:8092
   B → calls AddPeer("http://c:8092") [auto reverse-connect]
   Result: A↔B, A↔C, B↔C — full mesh ✓
```

### Requirement

For gossip to work, each node must set `--cluster-self-url` to its own reachable HTTP base URL. This URL is sent in the `X-Cluster-Self-URL` header when connecting to peers, and included in gossip events sent to others.

```bash
--cluster-self-url=http://my-node-hostname:8090
```

Nodes that do not set `--cluster-self-url` can still **receive** gossip (and connect to discovered peers) but cannot be **included** in gossip sent to others and cannot trigger auto reverse-connect.

---

## Late Joining — Adding Nodes After Start

### Automatic (via `--cluster-self-url` + gossip)

A node can join an existing cluster at any time. It only needs to know the address of **one** existing node:

```bash
# Existing cluster: A and B already running and connected

# New node D joins — only knows about A
./pocketbase serve --http=:8093 \
  --cluster-secret=secret \
  --cluster-self-url=http://d:8093 \
  --cluster-peers=http://a:8090
```

What happens:
1. D connects to A → A auto-connects back to D
2. A sends D a gossip event listing B (and any others)
3. D connects to B → B auto-connects back to D
4. Full mesh established: A↔B, A↔D, B↔D

### Via REST API (superuser auth required)

You can also dynamically add a peer to a running node via the admin API without restarting:

```bash
curl -X POST http://node-a:8090/api/cluster/peers \
  -H "Authorization: Bearer <superuser-token>" \
  -H "Content-Type: application/json" \
  -d '{"url": "http://node-d:8093"}'
```

This is equivalent to having configured `--cluster-peers=http://node-d:8093` at startup and is persistent for the lifetime of the process (not saved to disk — nodes still need their `--cluster-peers` flags on restart, or rely on gossip to rediscover).

---

## Realtime / Live Updates

PocketBase's realtime system (SSE to browser clients) uses the `OnModelAfterCreateSuccess` / `OnModelAfterUpdateSuccess` / `OnModelAfterDeleteSuccess` hooks internally.

When a peer's change is applied via `app.SaveNoValidate()` or `app.Delete()`, **those same hooks fire**, which causes the realtime system to broadcast the change to any browser clients connected to that node.

```
Browser on Node A                  Browser on Node B
      │                                   │
      │  subscribes realtime              │  subscribes realtime
      ▼                                   ▼
   Node A ──── SSE replication ────► Node B
      ▲                                   │
      │  user creates record              │  record arrives via replication
      │                                   │  → hooks fire
      │                                   │  → realtime SSE sent to browser
      │                                   ▼
      │                          [browser on B gets update] ✓
      │
 [browser on A gets update via normal realtime] ✓
```

Clients connected to **any** node receive updates from **all** nodes.

---

## What Gets Replicated

| Table / Content | Replicated | Notes |
|---|---|---|
| User-defined collection records | ✅ | All fields including file paths |
| `_collections` (schema) | ✅ | Table CREATE / ALTER / DROP triggered automatically |
| `_superusers` | ✅ | Admin accounts synced across nodes |
| `_authOrigins` | ✅ | Session origin tracking |
| `_externalAuths` | ✅ | OAuth2 provider links |
| `_mfas` | ✅ | Multi-factor auth records |
| `_otps` | ✅ | One-time passwords |
| `_params` (app settings) | ✅ | Raw SQL + settings reload |
| `_logs` | ❌ | Excluded: high volume, node-local |
| `_cluster_log` | ❌ | Excluded: internal delta-sync artifact, node-local |
| Uploaded files (storage) | ❌ | Use shared S3 / object storage |

> **File uploads** are stored on the local filesystem by default. For a cluster you must configure S3-compatible object storage in PocketBase settings so all nodes share the same storage backend.

---

## CLI Flags Reference

All flags belong to the `serve` sub-command.

| Flag | Default | Description |
|---|---|---|
| `--cluster-secret` | *(empty)* | **Required to enable cluster mode.** Shared secret that all nodes must have in common. Must be identical across every node. |
| `--cluster-self-url` | *(empty)* | Public base URL of **this** node (e.g. `http://node1:8090`). Sent to peers so they can auto-connect back and included in gossip sent to new peers. Required for automatic full-mesh formation. |
| `--cluster-peers` | *(empty)* | Comma-separated list of peer base URLs. Each URL is the HTTP root of another node (e.g. `http://node2:8091`). Can be specified multiple times. With gossip, you only need to list **one** bootstrap node. |
| `--cluster-id` | `<hostname>-<httpAddr>` | Unique identifier for this node. Shown in admin UI and log messages. If omitted, auto-generated from `os.Hostname()` + the `--http` address. |

### Examples

```bash
# Minimum required (two nodes, no gossip)
./pocketbase serve \
  --cluster-secret=abc123 \
  --cluster-peers=http://peer:8091

# Full setup with self-URL for gossip / late-join
./pocketbase serve \
  --cluster-id=eu-west-1 \
  --cluster-secret=abc123 \
  --cluster-self-url=http://eu-west-1.internal:8090 \
  --cluster-peers=http://us-east-1.internal:8090

# Multiple peers via repeated flag
./pocketbase serve \
  --cluster-secret=abc123 \
  --cluster-self-url=http://a:8090 \
  --cluster-peers=http://b:8091 \
  --cluster-peers=http://c:8092
```

---

## REST API Reference

### `GET /api/cluster/events`

**Purpose:** SSE stream consumed by peer nodes.

**Authentication:** Custom headers (not PocketBase JWT).

| Request Header | Required | Description |
|---|---|---|
| `X-Cluster-Secret` | ✅ | Must match the receiving node's `--cluster-secret` |
| `X-Cluster-Node-ID` | ✅ | Sending node's unique ID |
| `X-Cluster-Self-URL` | *(optional)* | Connecting node's own public base URL. Enables auto reverse-connect and inclusion in gossip. |
| `X-Cluster-Since` | *(optional)* | RFC3339Nano timestamp of the sender's previous successful connection to this node. When present, the server sends only records that changed since that time (delta sync). When absent, the server performs a full sync. |

| Response Header | Description |
|---|---|
| `X-Cluster-Node-ID` | This node's ID. Present on **all** responses including errors, so clients can detect self-connections. |
| `Content-Type` | `text/event-stream` |

**SSE event types sent by the server:**

```
event: hello
data: {"nodeId":"<this-node-id>"}

event: peers
data: {"urls":["http://c:8092","http://d:8093"]}

event: replicate
data: {"op":"create","table":"posts","id":"abc123","data":{...},"origin":"node-a","seq":42}
```

The `peers` event is sent once, immediately after the `hello`, and contains the URLs of all currently-known peers (excluding the connecting node). The connecting node uses this list to establish additional connections (gossip).

**Error responses:**

| HTTP status | Cause |
|---|---|
| `401 Unauthorized` | Wrong `X-Cluster-Secret` |
| `400 Bad Request` | Missing `X-Cluster-Node-ID`, or connecting to self |
| `503 Service Unavailable` | Cluster mode not enabled on this node |

---

### `GET /api/cluster/nodes`

**Purpose:** Returns cluster status for the admin dashboard.

**Authentication:** PocketBase superuser JWT (`Authorization: Bearer <token>`).

**Response (cluster disabled):**

```json
{
  "nodeId": "",
  "enabled": false,
  "nodes": []
}
```

**Response (cluster enabled):**

```json
{
  "nodeId": "my-node-hostname-8090",
  "enabled": true,
  "nodes": [
    {
      "id": "peer-node-hostname-8091",
      "addr": "192.168.1.42:54321",
      "connectedAt": "2025-03-05T10:00:00Z",
      "status": "connected"
    }
  ]
}
```

**Node status values:**

| Value | Meaning |
|---|---|
| `connected` | Peer has an active inbound SSE connection to this node |
| `reconnecting` | Outgoing connection attempt is in progress or retrying |

---

### `POST /api/cluster/peers`

**Purpose:** Dynamically add a new peer to a running node without restart.

**Authentication:** PocketBase superuser JWT (`Authorization: Bearer <token>`).

**Request body:**

```json
{
  "url": "http://node-d:8093"
}
```

**Response:**

```json
{
  "message": "Peer connection initiated.",
  "url": "http://node-d:8093"
}
```

**Error responses:**

| HTTP status | Cause |
|---|---|
| `400 Bad Request` | Missing or empty `url` field |
| `503 Service Unavailable` | Cluster mode not enabled on this node |

**Notes:**
- If the URL is already tracked, the call is a no-op (no duplicate connections).
- The connection is maintained for the lifetime of the process. It is **not** persisted to disk — on restart the node must rediscover the peer via `--cluster-peers` or gossip.

---

## Admin Dashboard

When the UI is built (`cd ui && npm install && npm run build`), a **Cluster** page is available at:

```
http://your-node/_/#/cluster
```

It is also accessible via the **node-tree icon** in the left sidebar (between Logs and Settings). The page:

- Shows whether cluster mode is enabled
- Displays this node's ID
- Lists all connected peers with their address, status, and connection timestamp
- Auto-refreshes every 5 seconds

If cluster mode is disabled, the page shows the command needed to enable it.

---

## Topology Guide

### Two nodes (most common)

Both nodes list each other and set `--cluster-self-url`. Gossip is automatic.

```
A ⇄ B
```

```bash
# A
--cluster-self-url=http://a:8090 --cluster-peers=http://b:8091

# B
--cluster-self-url=http://b:8091 --cluster-peers=http://a:8090
```

### N nodes with gossip bootstrap

Each node only needs to know one existing node. Set `--cluster-self-url` on every node. Full mesh forms automatically via gossip.

```
       A
      / \
     B   C
      \ /
       D  (joined later, only knew A)
```

```bash
# A — bootstrap node
--cluster-secret=X --cluster-self-url=http://a:8090

# B, C, D — each only needs to know A
--cluster-secret=X --cluster-self-url=http://b:8091 --cluster-peers=http://a:8090
--cluster-secret=X --cluster-self-url=http://c:8092 --cluster-peers=http://a:8090
--cluster-secret=X --cluster-self-url=http://d:8093 --cluster-peers=http://a:8090
```

After all four nodes are running, every node has a direct connection to every other node.

### N nodes without gossip (fully explicit)

If you prefer to not rely on gossip (e.g. in a controlled environment where all node addresses are known in advance), set `--cluster-peers` to list all other nodes on every node. Do not set `--cluster-self-url`. No automatic discovery occurs.

### Hub-and-spoke (not recommended)

If only the hub lists all spokes and spokes only list the hub, replication works as long as the hub is alive. If the hub goes down, spokes lose connectivity to each other. Use full-mesh topology for production.

### Starting order

Nodes can start in any order. If Node B starts before Node A, B will log connection failures and retry every 2 → 4 → 8 … seconds (up to 60 s) until A is reachable. When A comes back, B automatically connects and gossip re-establishes the mesh.

---

## Proxy / Load-Balancer Setup (nginx)

When placing PocketBase nodes behind a load balancer:

1. Use **sticky sessions** or route by path for client realtime connections (SSE connections from browsers must stay on one node for their lifetime).
2. The SSE cluster connections between nodes bypass the load balancer — configure `--cluster-peers` and `--cluster-self-url` with the **direct** node addresses, not the load-balancer address.

Example nginx with two upstream nodes:

```nginx
upstream pb_nodes {
    # sticky via ip_hash keeps browser SSE on one node
    ip_hash;
    server 127.0.0.1:8090;
    server 127.0.0.1:8091;
}

server {
    listen 80;

    location / {
        proxy_pass http://pb_nodes;
        proxy_http_version 1.1;

        # Required for SSE (client realtime)
        proxy_set_header Connection '';
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding on;
    }
}
```

The cluster SSE connections between nodes (`/api/cluster/events`) are direct peer-to-peer and do not go through nginx.

---

## Security

### Shared secret

- All nodes must be configured with the **same** `--cluster-secret` value.
- The secret is transmitted in the `X-Cluster-Secret` HTTP header over the SSE connection.
- Use HTTPS between nodes in production, or run cluster connections over a private network / VPN.
- Choose a strong random value: `openssl rand -hex 32`

### No public exposure needed

The `/api/cluster/events` endpoint does **not** need to be exposed to the public internet. Configure firewall rules to allow it only between node IPs. The `--cluster-self-url` values should be internal network addresses.

### Admin UI protection

`GET /api/cluster/nodes` and `POST /api/cluster/peers` both require a valid PocketBase superuser token. They are protected by the standard `RequireSuperuserAuth` middleware.

---

## Failure Handling & Reconnection

| Scenario | Behaviour |
|---|---|
| Peer node goes offline | Outgoing connection goroutine detects EOF/error, waits 2 s, retries. Back-off doubles each attempt up to 60 s. |
| Peer node comes back online | Reconnection is automatic. The reconnecting client sends `X-Cluster-Since` with its previous connection timestamp. The server queries `_cluster_log` and replays only what changed, then resumes live replication. Gossip also re-runs, re-establishing the full mesh. |
| Network partition (within 7 days) | When the partition heals, both nodes reconnect and exchange delta syncs from `_cluster_log`. All changes made during the partition on each side are replayed to the other. Conflicts are resolved by last-write-wins on the `updated` timestamp. |
| Network partition (> 7 days) | Log entries older than the retention window are pruned. On reconnect the server falls back to a full sync, which re-establishes consistency. No data is lost — full sync sends current state. |
| SSE buffer full (slow peer) | Live replication events are **dropped** for that peer to avoid back-pressure. Missed events are recovered on the next reconnect via delta sync. |
| A node crashes mid-write | SQLite's WAL mode ensures the local database stays consistent. The written record either made it (and will replicate on restart) or did not. |
| New node joins with empty DB | On first connection the server performs a full sync automatically. No manual backup restore required. |
| Self-connection attempt | Detected via `X-Cluster-Node-ID` response header. The client returns `errSelfConnect` and permanently removes the entry from `peerConns` without retrying. |

---

## Known Limitations

### Last-write-wins conflict resolution

Conflicts are resolved by comparing the `updated` timestamp of the incoming record against the local copy. The incoming change is applied only if its `updated` is strictly newer. There is no application-level merge — for concurrent updates to the same record from two nodes, the one with the later `updated` timestamp wins and the other is silently discarded.

### Partition recovery limited to 7-day log retention

Delta sync on reconnect is powered by `_cluster_log`, which retains entries for 7 days. If two nodes are partitioned for longer, log entries from the early part of the partition are pruned and a full sync is used instead. Full sync sends the **current state** of all records — any record deleted more than 7 days ago and not yet reconciled will not be re-deleted on the recovering node.

### File storage not replicated

Uploaded files are stored on the local filesystem. You must configure **S3-compatible object storage** (Settings → Storage) so all nodes share a single storage backend.

### Gossip does not survive restarts

Dynamically discovered peers (via gossip or `POST /api/cluster/peers`) are held in-memory only. On restart, each node reconnects only to its `--cluster-peers` list, and gossip re-discovers the rest from there. As long as at least one `--cluster-peers` entry is reachable, the full mesh re-forms automatically.

### Delta sync state does not survive restarts

The `X-Cluster-Since` timestamp sent on reconnect comes from the in-memory `peerConn.connectedAt`. After a node restart this is zero, so the first connection to each peer is always a full sync. Subsequent reconnects within the same process lifetime use delta sync.

### Logs not replicated

`_logs` and `_cluster_log` are intentionally excluded. Each node maintains its own independent log history.

---

## Internal Code Reference

### `tools/cluster/cluster.go`

| Symbol | Kind | Description |
|---|---|---|
| `ReplicationEvent` | struct | Wire format for a single replicated DB operation. Fields: `Op`, `Table`, `ID`, `RawData`, `Origin`, `Seq`. |
| `NodeInfo` | struct | Peer metadata returned by the admin API. |
| `Manager` | struct | Central component. Manages inbound SSE clients and outgoing peer connections. |
| `NewManager(nodeID, secret, selfURL, peerURLs, logger)` | func | Create and configure a Manager. `selfURL` is this node's public base URL for gossip. |
| `Manager.Start()` | method | Launch goroutines for each peer URL. Non-blocking. |
| `Manager.Stop()` | method | Close all connections and stop goroutines. |
| `Manager.Broadcast(event)` | method | Serialize and fan-out an event to all inbound SSE clients. Non-blocking (drops if buffer full). |
| `Manager.BroadcastToOne(nodeID, event)` | method | Serialize and send to one peer by node ID. Non-blocking (drops if buffer full). |
| `Manager.SendToPeer(nodeID, event, ctx)` | method | Blocking send to one peer. Blocks until the peer's channel has space or `ctx` is cancelled. Used during sync to guarantee delivery. |
| `Manager.AddPeer(url)` | method | Dynamically add a peer URL and start maintaining a connection. Idempotent. |
| `Manager.RegisterSSEClient(nodeID, addr, selfURL)` | method | Called by the HTTP handler when a peer connects. Returns `(<-chan []byte, cleanupFunc)`. |
| `Manager.Nodes()` | method | Snapshot of all known peers. |
| `Manager.KnownPeerURLs(excludeURL)` | method | Returns public URLs of all connected peers for gossip. |
| `Manager.SelfURL()` | method | Returns this node's public base URL. |
| `Manager.ApplyFunc` | field | Callback wired by `cmd/serve.go` to `apis.ApplyReplication`. |
| `IsReplicating(key)` | func | Returns true if `key` is currently being applied from a peer. |
| `MarkReplicating(key)` | func | Registers a replication-in-progress key. |
| `UnmarkReplicating(key)` | func | Removes a replication-in-progress key. |
| `ReplicationKey(table, id)` | func | Returns `"table:id"` string used as the loop-prevention key. |

### `apis/cluster.go`

| Symbol | Kind | Description |
|---|---|---|
| `ClusterManagerKey` | const | App store key (`"clusterManager"`) used to retrieve the Manager instance. |
| `bindClusterApi(app, rg)` | func | Registers HTTP routes and hooks. Called from `apis/base.go`. |
| `clusterEventsHandler` | func | HTTP handler for `GET /api/cluster/events`. Validates secret, registers SSE client, triggers auto reverse-connect if `X-Cluster-Self-URL` present, sends `hello` + `peers` gossip events, then streams replication events. |
| `clusterNodesHandler` | func | HTTP handler for `GET /api/cluster/nodes`. Returns JSON status. |
| `clusterAddPeerHandler` | func | HTTP handler for `POST /api/cluster/peers`. Dynamically adds a peer. Requires superuser auth. |
| `bindClusterReplicationHooks(app)` | func | Binds to `OnModelAfterCreate/Update/DeleteSuccess` to capture and broadcast local changes. |
| `bindClusterLogHooks(app)` | func | Binds to the same hooks (priority −97) to write every change to `_cluster_log`. Fires for both local and applied-from-peer changes. |
| `broadcastModelChange(app, op, model)` | func | Serializes model and calls `manager.Broadcast`. Skips if `IsReplicating`. |
| `serializeModelForReplication(app, model, op)` | func | Returns `map[string]any` — JSON for collections, raw SQL row for everything else. |
| `ApplyReplication(app, event)` | func | **Public.** Entry point for applying a received event. Dispatches to collection or record handlers. |
| `applyCollectionReplication(app, event)` | func | Handles `_collections` changes. Skips if local `Updated` >= incoming `Updated` (last-write-wins). |
| `applyRecordOrRawReplication(app, event)` | func | Handles record changes. Skips if local `updated` >= incoming `updated` (last-write-wins). |
| `applyRawSQL(app, event)` | func | Raw `INSERT OR REPLACE` / `DELETE` for non-collection tables (`_params`). |
| `SyncAllTables(app, manager, ctx)` | func | **Public.** Broadcasts all existing rows as create events to all peers (full sync). |
| `syncAllTablesToPeer(app, manager, peerNodeID, ctx)` | func | Full sync to one peer using blocking `SendToPeer`. Triggered on first connection. |
| `deltaSyncToPeer(app, manager, peerNodeID, since, ctx)` | func | Delta sync to one peer. Queries `_cluster_log WHERE created >= since−5min`, deduplicates per record, fetches current data, sends via `SendToPeer`. Falls back to full sync on error. |
| `ensureClusterLogTable(app)` | func | Creates `_cluster_log` and its index if they don't exist. Called once at startup from `bindClusterApi`. |
| `writeClusterLog(app, table, id, op)` | func | Appends one row to `_cluster_log` with `created = time.Now().UnixNano()`. |
| `pruneClusterLog(app)` | func | Background goroutine. Prunes log entries older than 7 days, once per hour. |

### `cmd/serve.go`

Adds four new persistent flags to the `serve` command:

| Flag | Go variable | Passed to |
|---|---|---|
| `--cluster-id` | `clusterNodeID` | `cluster.NewManager` |
| `--cluster-secret` | `clusterSecret` | `cluster.NewManager` |
| `--cluster-self-url` | `clusterSelfURL` | `cluster.NewManager` |
| `--cluster-peers` | `clusterPeers` | `cluster.NewManager` |

The Manager is created and stored in `app.Store().Set(apis.ClusterManagerKey, manager)`, then started inside the `OnServe` hook (after the HTTP server is ready) and stopped in `OnTerminate`.

### `ui/src/components/cluster/PageCluster.svelte`

Svelte component for the admin dashboard cluster page. Polls `GET /api/cluster/nodes` every 5 seconds and renders a table of peer nodes. Accessible at `/_/#/cluster` and via the sidebar icon.
