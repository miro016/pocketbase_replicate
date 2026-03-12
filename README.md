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

### Code Review Summary

A code review of the replication layer identified the following areas for improvement:

**Error handling gaps** — Several places silently discard errors that could cause subtle failures:
- `apis/cluster.go:191` — JSON marshal error ignored when building gossip payload; a failure produces a malformed SSE event
- `apis/cluster.go:554,624,649` — `ParseDateTime` errors discarded; a zero time breaks last-write-wins conflict resolution
- `apis/cluster.go:1312` — DB query error unchecked in `deltaSyncToPeer`; stale data may be sent
- `apis/cluster.go:1136,1374,1416,1441` — `CREATE TABLE` / `ALTER TABLE` errors ignored via `// nolint`; if these fail, the cluster log is unavailable

**Goroutine and resource management** — Potential leaks under edge-case conditions:
- `tools/cluster/cluster.go:285,319` — `forceClose()` spawned as a goroutine on every SSE write error; repeated failures accumulate goroutines
- `tools/cluster/cluster.go:545-551` — Context cancellation watcher goroutine may not exit on normal connection close (leak per peer connection)
- `apis/cluster.go:173-178` — Sync goroutines (`syncAllTablesToPeer`, `deltaSyncToPeer`) are not cancelled when the SSE handler returns; they may attempt sends on a closed channel

**Concurrency issues:**
- `tools/cluster/cluster.go:417-432` — TOCTOU race in `Nodes()`: `peerConn.nodeID` read under lock, but the dedup decision is made after unlock
- No reconnect jitter — all peers use identical exponential backoff (2s, 4s, ... 60s) without random jitter, risking thundering-herd reconnection storms after a network partition

**Resource limits:**
- `tools/cluster/cluster.go:633` — Hardcoded 1 MB SSE scanner buffer; records larger than 1 MB silently break the scanner and halt replication for that peer
- `apis/cluster.go:1282-1358` — `deltaSyncToPeer` loads the entire `_cluster_log` result set into memory; millions of entries during a long partition can cause OOM
- `apis/cluster.go:1059-1110` — `syncTable` streams all rows without pagination; large tables cause memory spikes

**Security considerations:**
- `cmd/serve.go:50-59` — Cluster secret transmitted in plaintext HTTP headers; a warning is logged but HTTP is not rejected (use HTTPS or a private network)
- No rate limiting on the SSE replication endpoint; a compromised peer can flood events
- `apis/cluster.go:907-921` — Table names in `buildUpsertSQL` are interpolated into SQL; mitigated by `app.HasTable()` validation but worth noting

**Minor / code quality:**
- Fix references (`Fix #1` through `Fix #13`) in comments lack a corresponding issue tracker
- `apis/cluster.go:863` uses `context.Background()` instead of the request context for file downloads, bypassing graceful shutdown
- `apis/cluster.go:1430` — `fmt.Sscanf` error unchecked when parsing tombstone prune timestamp

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
