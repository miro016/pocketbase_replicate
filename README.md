<p align="center">
    <a href="https://pocketbase.io" target="_blank" rel="noopener">
        <img src="https://i.imgur.com/5qimnm5.png" alt="PocketBase - open source backend in 1 file" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/pocketbase/pocketbase/actions/workflows/release.yaml" target="_blank" rel="noopener"><img src="https://github.com/pocketbase/pocketbase/actions/workflows/release.yaml/badge.svg" alt="build" /></a>
    <a href="https://github.com/pocketbase/pocketbase/releases" target="_blank" rel="noopener"><img src="https://img.shields.io/github/release/pocketbase/pocketbase.svg" alt="Latest releases" /></a>
    <a href="https://pkg.go.dev/github.com/pocketbase/pocketbase" target="_blank" rel="noopener"><img src="https://godoc.org/github.com/pocketbase/pocketbase?status.svg" alt="Go package documentation" /></a>
</p>

[PocketBase](https://pocketbase.io) is an open source Go backend that includes:

- embedded database (_SQLite_) with **realtime subscriptions**
- built-in **files and users management**
- convenient **Admin dashboard UI**
- and simple **REST-ish API**

**For documentation and examples, please visit https://pocketbase.io/docs.**

> [!WARNING]
> Please keep in mind that PocketBase is still under active development
> and therefore full backward compatibility is not guaranteed before reaching v1.0.0.

---

## Cluster Replication (Fork Addition)

This fork adds **active-active multi-node replication** to PocketBase. Any node can accept writes; changes propagate to all peers in near-real-time via Server-Sent Events (SSE). No external dependencies are introduced — the entire feature uses only the Go standard library.

| Property | Value |
|---|---|
| Transport | SSE over HTTP/HTTPS |
| Replication model | Active-active (multi-master) |
| Conflict resolution | Last-write-wins (`updated` timestamp) |
| Peer discovery | Gossip (automatic full-mesh formation) |
| New dependencies | None |

### Quick Start

```bash
# Node A
./pocketbase serve --http=:8090 \
  --cluster-secret=my-secret \
  --cluster-self-url=http://node-a:8090 \
  --cluster-peers=http://node-b:8091

# Node B
./pocketbase serve --http=:8091 \
  --cluster-secret=my-secret \
  --cluster-self-url=http://node-b:8091 \
  --cluster-peers=http://node-a:8090
```

Create a record on Node A — it immediately appears on Node B, and vice versa. With `--cluster-self-url` set, gossip automatically forms a full mesh when adding more nodes (each new node only needs to know one existing peer).

### What Gets Replicated

Collections, records, superusers, auth origins, external auths, MFAs, OTPs, and app settings (`_params`) are all replicated. `_logs` and uploaded files are **not** replicated — use S3-compatible object storage for multi-node file sharing.

### Code Review and Fixes

A code review of the replication layer identified and fixed the following issues:

**Error handling (fixed):**
- Gossip JSON marshal error now checked and logged instead of silently ignored
- `ParseDateTime` errors in LWW conflict checks now logged with context (table, id, raw value)
- Delta sync DB query for `record_updated` now checked and logged on failure
- `ALTER TABLE` migration errors now logged (except expected "duplicate column" on already-migrated DBs)
- `setTombstonePruneCutoff` now logs errors instead of silently discarding
- `getTombstonePruneCutoff` `Sscanf` error now handled (returns 0 on parse failure)

**Goroutine and resource management (fixed):**
- `forceClose()` now called directly instead of via `go` — it's non-blocking (just closes a channel via `sync.Once`), so spawning a goroutine was unnecessary overhead
- Sync goroutines (`syncAllTablesToPeer`, `deltaSyncToPeer`) now use a child context derived from the SSE handler; cancelled automatically when the handler returns
- SSE write errors now logged at debug level before returning, aiding connection drop diagnosis

**Concurrency (fixed):**
- `Nodes()` now records `nodeID` in the `seen` map for outgoing connections, preventing duplicate entries when the same peer appears in both `sseClients` and `peerConns`
- Reconnect backoff now includes ±25% random jitter to prevent thundering-herd storms after network partitions

**Resource limits (fixed):**
- SSE scanner buffer increased from 1 MB to 8 MB max (starts at 64 KB, grows on demand)
- `deltaSyncToPeer` now caps at 100k unique entries; if the log is larger, falls back to full sync instead of OOM
- `fetchAndStoreFile` now uses a dedicated `http.Client` with a 5-minute timeout instead of `http.DefaultClient`

**Remaining known limitations (by design or low priority):**
- Cluster secret is transmitted in HTTP headers; a warning is logged but plaintext HTTP is not rejected (use HTTPS or a private network in production)
- No rate limiting on the SSE replication endpoint
- Table names in `buildUpsertSQL` are interpolated into SQL; mitigated by `app.HasTable()` pre-validation

### Recent Enhancements

**Clock skew detection:**
- Each peer connection measures the clock difference using the `X-Cluster-Server-Time` response header
- The admin dashboard shows a per-peer "Clock Skew" column and displays a prominent warning banner when any peer's skew exceeds 1 second
- Skew > 5 seconds is shown in red; > 1 second in orange — since LWW depends on `updated` timestamps, synchronized clocks (via NTP) are important for correct conflict resolution

**File replication during resync:**
- During full sync and delta sync, files are now pulled even when the record itself is already up-to-date locally (LWW skip)
- Previously, if a record existed on both nodes but files were missing on one node (e.g. after a restore or storage failure), the files would never be synced — this is now fixed

**Paginated full sync:**
- `syncTable` now uses cursor-based pagination (500 rows per page, keyed on `id`) instead of streaming the entire table in a single query
- This bounds memory usage during full sync of large tables and prevents SQLite from holding a long-lived read transaction

**Delivery acknowledgments:**
- Each SSE connection now sends periodic `ack` events (every 5 seconds) containing the node's current processed sequence number
- The admin dashboard shows a "Last Ack Seq" column per peer, enabling operators to detect replication lag
- Acks flow bidirectionally: the SSE server sends acks to connected clients, and connecting clients process acks from peers

For the full cluster reference (topology, REST API, nginx setup, failure handling, known limitations), see [CLUSTER.md](CLUSTER.md).

---

## API SDK clients

The easiest way to interact with the PocketBase Web APIs is to use one of the official SDK clients:

- **JavaScript - [pocketbase/js-sdk](https://github.com/pocketbase/js-sdk)** (_Browser, Node.js, React Native_)
- **Dart - [pocketbase/dart-sdk](https://github.com/pocketbase/dart-sdk)** (_Web, Mobile, Desktop, CLI_)

You could also check the recommendations in https://pocketbase.io/docs/how-to-use/.


## Overview

### Use as standalone app

You could download the prebuilt executable for your platform from the [Releases page](https://github.com/pocketbase/pocketbase/releases).
Once downloaded, extract the archive and run `./pocketbase serve` in the extracted directory.

The prebuilt executables are based on the [`examples/base/main.go` file](https://github.com/pocketbase/pocketbase/blob/master/examples/base/main.go) and comes with the JS VM plugin enabled by default which allows to extend PocketBase with JavaScript (_for more details please refer to [Extend with JavaScript](https://pocketbase.io/docs/js-overview/)_).

### Use as a Go framework/toolkit

PocketBase is distributed as a regular Go library package which allows you to build
your own custom app specific business logic and still have a single portable executable at the end.

Here is a minimal example:

0. [Install Go 1.23+](https://go.dev/doc/install) (_if you haven't already_)

1. Create a new project directory with the following `main.go` file inside it:
    ```go
    package main

    import (
        "log"

        "github.com/pocketbase/pocketbase"
        "github.com/pocketbase/pocketbase/core"
    )

    func main() {
        app := pocketbase.New()

        app.OnServe().BindFunc(func(se *core.ServeEvent) error {
            // registers new "GET /hello" route
            se.Router.GET("/hello", func(re *core.RequestEvent) error {
                return re.String(200, "Hello world!")
            })

            return se.Next()
        })

        if err := app.Start(); err != nil {
            log.Fatal(err)
        }
    }
    ```

2. To init the dependencies, run `go mod init myapp && go mod tidy`.

3. To start the application, run `go run main.go serve`.

4. To build a statically linked executable, you can run `CGO_ENABLED=0 go build` and then start the created executable with `./myapp serve`.

_For more details please refer to [Extend with Go](https://pocketbase.io/docs/go-overview/)._

### Building and running the repo main.go example

To build the minimal standalone executable, like the prebuilt ones in the releases page, you can simply run `go build` inside the `examples/base` directory:

0. [Install Go 1.24+](https://go.dev/doc/install) (_if you haven't already_)
1. Clone/download the repo
2. Navigate to `examples/base`
3. Run `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build`
   (_https://go.dev/doc/install/source#environment_)
4. Start the created executable by running `./base serve`.

Note that the supported build targets by the pure Go SQLite driver at the moment are:

```
darwin  amd64
darwin  arm64
freebsd amd64
freebsd arm64
linux   386
linux   amd64
linux   arm
linux   arm64
linux   loong64
linux   ppc64le
linux   riscv64
linux   s390x
windows 386
windows amd64
windows arm64
```

### Testing

PocketBase comes with mixed bag of unit and integration tests.
To run them, use the standard `go test` command:

```sh
go test ./...
```

Check also the [Testing guide](http://pocketbase.io/docs/testing) to learn how to write your own custom application tests.

## Security

If you discover a security vulnerability within PocketBase, please send an e-mail to **support at pocketbase.io**.

All reports will be promptly addressed and you'll be credited in the fix release notes.

## Contributing

PocketBase is free and open source project licensed under the [MIT License](LICENSE.md).
You are free to do whatever you want with it, even offering it as a paid service.

You could help continuing its development by:

- [Contribute to the source code](CONTRIBUTING.md)
- [Suggest new features and report issues](https://github.com/pocketbase/pocketbase/issues)

PRs for new OAuth2 providers, bug fixes, code optimizations and documentation improvements are more than welcome.

But please refrain creating PRs for _new features_ without previously discussing the implementation details.
PocketBase has a [roadmap](https://github.com/orgs/pocketbase/projects/2) and I try to work on issues in specific order and such PRs often come in out of nowhere and skew all initial planning with tedious back-and-forth communication.

Don't get upset if I close your PR, even if it is well executed and tested. This doesn't mean that it will never be merged.
Later we can always refer to it and/or take pieces of your implementation when the time comes to work on the issue (don't worry you'll be credited in the release notes).
