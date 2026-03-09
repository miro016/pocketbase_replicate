package apis

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/cluster"
	"github.com/pocketbase/pocketbase/tools/filesystem"
	"github.com/pocketbase/pocketbase/tools/hook"
	"github.com/pocketbase/pocketbase/tools/router"
	"github.com/pocketbase/pocketbase/tools/routine"
	"github.com/pocketbase/pocketbase/tools/types"
)

// ClusterManagerKey is the app store key used to store/retrieve the cluster Manager.
// It is exported so that cmd/serve.go can set it.
const ClusterManagerKey = "clusterManager"

// getClusterManager retrieves the cluster Manager from the app store, or nil if not configured.
func getClusterManager(app core.App) *cluster.Manager {
	m, _ := app.Store().Get(ClusterManagerKey).(*cluster.Manager)
	return m
}

// collectionsTableName is the internal PocketBase table name for collections.
const collectionsTableName = "_collections"

// clusterLogTable is the node-local replication log used for delta sync on reconnect.
const clusterLogTable = "_cluster_log"

// clusterPeerStateTable persists the last-seen sync time per peer URL so that
// node restarts continue with delta sync rather than a full sync.
const clusterPeerStateTable = "_cluster_peer_state"

// clusterMetaTable stores node-local metadata as key-value pairs.
const clusterMetaTable = "_cluster_meta"

// metaKeyTombstoneCutoff is the _cluster_meta key for the tombstone prune cutoff
// (Unix nanoseconds). Any peer whose X-Cluster-Since predates this value is forced
// into a full sync because the tombstones it missed may have already been pruned.
const metaKeyTombstoneCutoff = "tomb_prune_cutoff_ns"

// clusterLogRetention is how long non-tombstone log entries are kept.
const clusterLogRetention = 7 * 24 * time.Hour

// clusterTombstoneRetention is how long delete tombstones are kept.
// Longer than clusterLogRetention so peers offline up to this duration can still
// receive correct delete events via delta sync.
const clusterTombstoneRetention = 30 * 24 * time.Hour

// clusterSyncBuffer is subtracted from the peer's last-seen timestamp when doing a
// delta sync, to tolerate minor clock skew between nodes.
const clusterSyncBuffer = 5 * time.Minute

// bindClusterApi registers the cluster-specific HTTP endpoints.
func bindClusterApi(app core.App, rg *router.RouterGroup[*core.RequestEvent]) {
	if err := ensureClusterLogTable(app); err != nil {
		app.Logger().Warn("[cluster] failed to create cluster log table", "error", err)
	}
	if err := ensureClusterPeerStateTable(app); err != nil {
		app.Logger().Warn("[cluster] failed to create cluster peer state table", "error", err)
	}
	if err := ensureClusterMetaTable(app); err != nil {
		app.Logger().Warn("[cluster] failed to create cluster meta table", "error", err)
	}

	// Wire persistent peer sync time callbacks so reconnects after a restart
	// use delta sync rather than a full sync. (Fix #6)
	if m := getClusterManager(app); m != nil {
		m.LoadPeerSyncTime = func(peerURL string) time.Time {
			return loadPeerSyncTime(app, peerURL)
		}
		m.SavePeerSyncTime = func(peerURL string, syncedAt time.Time) {
			savePeerSyncTime(app, peerURL, syncedAt)
		}
		// Prune goroutine is tied to the manager lifecycle so it exits on shutdown. (Fix #3)
		go pruneClusterLog(app, m.Done())
	}

	sub := rg.Group("/cluster")
	sub.GET("/events", clusterEventsHandler)
	sub.GET("/files", clusterFilesHandler)
	sub.GET("/nodes", clusterNodesHandler).Bind(RequireSuperuserAuth())
	sub.POST("/peers", clusterAddPeerHandler).Bind(RequireSuperuserAuth())

	bindClusterReplicationHooks(app)
	bindClusterLogHooks(app)
}

// -----------------------------------------------------------------
// HTTP handlers
// -----------------------------------------------------------------

// clusterEventsHandler serves an SSE stream to incoming peer nodes.
// Peers authenticate by providing the shared cluster secret in the
// X-Cluster-Secret header, and their own node ID in X-Cluster-Node-ID.
func clusterEventsHandler(e *core.RequestEvent) error {
	m := getClusterManager(e.App)
	if m == nil {
		return e.JSON(http.StatusServiceUnavailable, map[string]string{
			"message": "Cluster mode is not enabled on this node.",
		})
	}

	// Always identify this node so clients can detect self-connections from any response.
	e.Response.Header().Set("X-Cluster-Node-ID", m.NodeID())

	peerNodeID := e.Request.Header.Get("X-Cluster-Node-ID")
	peerSecret := e.Request.Header.Get("X-Cluster-Secret")

	// Use constant-time comparison to prevent timing-based secret oracle. (Fix #9)
	if subtle.ConstantTimeCompare([]byte(peerSecret), []byte(m.Secret())) != 1 {
		return e.JSON(http.StatusUnauthorized, map[string]string{
			"message": "Invalid cluster secret.",
		})
	}
	if peerNodeID == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{
			"message": "Missing X-Cluster-Node-ID header.",
		})
	}
	// Prevent connecting to ourselves
	if peerNodeID == m.NodeID() {
		return e.JSON(http.StatusBadRequest, map[string]string{
			"message": "Cannot connect to self.",
		})
	}

	// Disable global write deadline for SSE
	rc := http.NewResponseController(e.Response)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil && err != http.ErrNotSupported {
		return e.InternalServerError("Failed to initialize SSE stream.", err)
	}
	e.Response.Header().Set("Content-Type", "text/event-stream")
	e.Response.Header().Set("Cache-Control", "no-store")
	e.Response.Header().Set("X-Accel-Buffering", "no")

	// Determine peer remote address
	peerAddr := e.Request.Header.Get("X-Forwarded-For")
	if peerAddr == "" {
		peerAddr = e.Request.RemoteAddr
	}

	peerSelfURL := e.Request.Header.Get("X-Cluster-Self-URL")

	eventCh, doneCh, cleanup := m.RegisterSSEClient(peerNodeID, peerAddr, peerSelfURL)
	defer cleanup()

	// If the peer advertises its own public URL, connect back to it automatically
	// so that changes on the peer are also replicated to us (bidirectional, late-join).
	if peerSelfURL != "" {
		m.AddPeer(peerSelfURL)
	}

	ctx := e.Request.Context()
	if since, err := time.Parse(time.RFC3339Nano, e.Request.Header.Get("X-Cluster-Since")); err == nil {
		// If the peer's last-seen time predates our tombstone prune cutoff, some
		// tombstones may have been pruned that the peer never received.
		// Fall back to a full sync so deleted records don't survive on the peer.
		pruneCutoff := getTombstonePruneCutoff(e.App)
		if pruneCutoff > 0 && since.UnixNano() < pruneCutoff {
			go syncAllTablesToPeer(e.App, m, peerNodeID, ctx)
		} else {
			go deltaSyncToPeer(e.App, m, peerNodeID, since, ctx)
		}
	} else {
		go syncAllTablesToPeer(e.App, m, peerNodeID, ctx)
	}

	// Send a hello ping so the peer knows we are alive
	helloMsg := []byte("event: hello\ndata: {\"nodeId\":\"" + m.NodeID() + "\"}\n\n")
	if _, err := e.Response.Write(helloMsg); err != nil {
		return nil
	}
	_ = rc.Flush()

	// Gossip: send the connecting peer a list of all other known peer URLs so it
	// can establish a full-mesh automatically (without pre-configuring every node).
	if knownURLs := m.KnownPeerURLs(peerSelfURL); len(knownURLs) > 0 {
		peersData, _ := json.Marshal(map[string][]string{"urls": knownURLs})
		peersMsg := append([]byte("event: peers\ndata: "), peersData...)
		peersMsg = append(peersMsg, '\n', '\n')
		if _, err := e.Response.Write(peersMsg); err != nil {
			return nil
		}
		_ = rc.Flush()
	}

	// Heartbeat ticker: send a comment every 30 s so proxies/load-balancers don't
	// drop the idle SSE connection. (Fix #7)
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	// Stream events until the peer disconnects, the manager force-closes this
	// connection (buffer overflow → delta sync on reconnect), or we shut down.
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-doneCh:
			return nil
		case data, ok := <-eventCh:
			if !ok {
				return nil
			}
			if _, err := e.Response.Write(data); err != nil {
				return nil
			}
			_ = rc.Flush()
		case <-pingTicker.C:
			if _, err := e.Response.Write([]byte(": ping\n\n")); err != nil {
				return nil
			}
			_ = rc.Flush()
		}
	}
}

// clusterNodesHandler returns the list of connected peer nodes for the admin UI.
// Access is restricted to superusers via the RequireSuperuserAuth middleware bound to the route.
func clusterNodesHandler(e *core.RequestEvent) error {
	m := getClusterManager(e.App)
	if m == nil {
		return e.JSON(http.StatusOK, map[string]any{
			"nodeId":  "",
			"enabled": false,
			"nodes":   []any{},
		})
	}

	return e.JSON(http.StatusOK, map[string]any{
		"nodeId":  m.NodeID(),
		"enabled": true,
		"nodes":   m.Nodes(),
	})
}

// clusterAddPeerHandler dynamically adds a new peer URL to the cluster manager.
// This allows nodes to join the cluster after the initial startup without restart.
//
// Request body: {"url": "http://node3:8092"}
func clusterAddPeerHandler(e *core.RequestEvent) error {
	m := getClusterManager(e.App)
	if m == nil {
		return e.JSON(http.StatusServiceUnavailable, map[string]string{
			"message": "Cluster mode is not enabled on this node.",
		})
	}

	var body struct {
		URL string `json:"url"`
	}
	if err := e.BindBody(&body); err != nil || body.URL == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{
			"message": "Request body must contain a non-empty \"url\" field.",
		})
	}

	m.AddPeer(body.URL)

	return e.JSON(http.StatusOK, map[string]string{
		"message": "Peer connection initiated.",
		"url":     body.URL,
	})
}

// clusterFilesHandler serves a raw file from this node's storage to authenticated peers.
// This is the pull endpoint used for file replication: after a peer applies a create/update
// event it calls this endpoint to fetch any files that don't exist locally yet.
//
// Query param: path — the blob storage key (e.g. "collectionId/recordId/filename.jpg").
// Auth:        X-Cluster-Secret header (same shared secret as the SSE endpoint).
func clusterFilesHandler(e *core.RequestEvent) error {
	m := getClusterManager(e.App)
	if m == nil {
		return e.JSON(http.StatusServiceUnavailable, map[string]string{
			"message": "Cluster mode is not enabled on this node.",
		})
	}

	peerSecret := e.Request.Header.Get("X-Cluster-Secret")
	if subtle.ConstantTimeCompare([]byte(peerSecret), []byte(m.Secret())) != 1 {
		return e.JSON(http.StatusUnauthorized, map[string]string{
			"message": "Invalid cluster secret.",
		})
	}

	fileKey := e.Request.URL.Query().Get("path")
	if fileKey == "" {
		return e.JSON(http.StatusBadRequest, map[string]string{
			"message": "Missing path query parameter.",
		})
	}

	// Reject path traversal attempts.
	cleaned := path.Clean(fileKey)
	if strings.Contains(cleaned, "..") || strings.HasPrefix(cleaned, "/") {
		return e.JSON(http.StatusBadRequest, map[string]string{
			"message": "Invalid file path.",
		})
	}

	fsys, err := e.App.NewFilesystem()
	if err != nil {
		return e.InternalServerError("Failed to open filesystem.", err)
	}
	defer fsys.Close()

	reader, err := fsys.GetReader(cleaned)
	if err != nil {
		return e.NotFoundError("File not found.", err)
	}
	defer reader.Close()

	e.Response.Header().Set("Content-Type", reader.ContentType())
	e.Response.WriteHeader(http.StatusOK)
	_, _ = io.Copy(e.Response, reader)
	return nil
}

// -----------------------------------------------------------------
// Replication hook registration (sending side)
// -----------------------------------------------------------------

// bindClusterReplicationHooks wires up model-level hooks that broadcast
// local database changes to all connected peer nodes.
func bindClusterReplicationHooks(app core.App) {
	// CREATE
	app.OnModelAfterCreateSuccess().Bind(&hook.Handler[*core.ModelEvent]{
		Id: "clusterReplicateCreate",
		Func: func(e *core.ModelEvent) error {
			broadcastModelChange(e.App, cluster.OpCreate, e.Model)
			return e.Next()
		},
		Priority: -98, // after realtime (-99)
	})

	// UPDATE
	app.OnModelAfterUpdateSuccess().Bind(&hook.Handler[*core.ModelEvent]{
		Id: "clusterReplicateUpdate",
		Func: func(e *core.ModelEvent) error {
			broadcastModelChange(e.App, cluster.OpUpdate, e.Model)
			return e.Next()
		},
		Priority: -98,
	})

	// DELETE
	app.OnModelAfterDeleteSuccess().Bind(&hook.Handler[*core.ModelEvent]{
		Id: "clusterReplicateDelete",
		Func: func(e *core.ModelEvent) error {
			broadcastModelChange(e.App, cluster.OpDelete, e.Model)
			return e.Next()
		},
		Priority: -98,
	})
}

// tablesToSkip contains table names that should NOT be replicated
// (either because they are node-local or because they are too high-volume).
var tablesToSkip = map[string]bool{
	core.LogsTableName:    true,
	clusterLogTable:       true,
	clusterPeerStateTable: true,
	clusterMetaTable:      true,
}

// broadcastModelChange serializes the changed model and sends a replication
// event to all connected peer nodes.
func broadcastModelChange(app core.App, op string, model core.Model) {
	m := getClusterManager(app)
	if m == nil {
		return
	}

	tableName := model.TableName()
	id := fmt.Sprint(model.PK())

	// Skip if this change is itself the result of applying a replication event
	if cluster.IsReplicating(cluster.ReplicationKey(tableName, id)) {
		return
	}

	// Skip tables that should not be replicated
	if tablesToSkip[tableName] {
		return
	}

	// Serialize the model
	rawData, err := serializeModelForReplication(app, model, op)
	if err != nil {
		app.Logger().Warn("[cluster] failed to serialize model for replication",
			"table", tableName, "id", id, "error", err)
		return
	}

	routine.FireAndForget(func() {
		m.Broadcast(&cluster.ReplicationEvent{
			Op:      op,
			Table:   tableName,
			ID:      id,
			RawData: rawData,
		})
	})
}

// extractModelUpdated returns the "updated" timestamp string from a model.
// Returns "" if the model type is not recognised or the timestamp is zero.
func extractModelUpdated(model core.Model) string {
	switch m := model.(type) {
	case *core.Record:
		dt := m.GetDateTime("updated")
		if !dt.IsZero() {
			return dt.String()
		}
	case *core.Collection:
		if !m.Updated.IsZero() {
			return m.Updated.String()
		}
	}
	return ""
}

// serializeModelForReplication returns the raw data map that should be sent
// to peer nodes. For collections we use JSON marshal; for records we use
// a raw SQL SELECT so we capture ALL columns including hidden auth fields.
func serializeModelForReplication(app core.App, model core.Model, op string) (map[string]any, error) {
	if op == cluster.OpDelete {
		// For deletes, include the record's updated timestamp so that receivers
		// can apply LWW: a local update newer than this delete should win. (Fix #1)
		updated := extractModelUpdated(model)
		if updated != "" {
			return map[string]any{"updated": updated}, nil
		}
		return nil, nil
	}

	tableName := model.TableName()

	// If it's a core.Collection, use JSON round-trip so we get the full schema.
	if _, ok := model.(*core.Collection); ok {
		data, err := json.Marshal(model)
		if err != nil {
			return nil, err
		}
		result := map[string]any{}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, err
		}
		return result, nil
	}

	// For everything else (records, settings, etc.) read raw columns from the DB.
	return getRawRow(app, tableName, fmt.Sprint(model.PK()))
}

// getRawRow executes SELECT * for a single row and returns column → value pairs.
func getRawRow(app core.App, tableName, id string) (map[string]any, error) {
	sqlRows, err := app.NonconcurrentDB().
		NewQuery("SELECT * FROM {{" + tableName + "}} WHERE id={:id}").
		Bind(dbx.Params{"id": id}).
		Rows()
	if err != nil {
		return nil, fmt.Errorf("getRawRow query: %w", err)
	}
	defer sqlRows.Close()

	if !sqlRows.Next() {
		return nil, fmt.Errorf("getRawRow: row not found (%s/%s)", tableName, id)
	}

	cols, err := sqlRows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getRawRow columns: %w", err)
	}

	values := make([]any, len(cols))
	valuePtrs := make([]any, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	if err := sqlRows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("getRawRow scan: %w", err)
	}

	result := make(map[string]any, len(cols))
	for i, col := range cols {
		// SQLite returns []byte for TEXT columns in some drivers; normalise.
		if b, ok := values[i].([]byte); ok {
			result[col] = string(b)
		} else {
			result[col] = values[i]
		}
	}
	return result, nil
}

// -----------------------------------------------------------------
// Replication apply (receiving side)
// -----------------------------------------------------------------

// ApplyReplication applies a replication event received from a peer to the
// local database. It is called by the cluster Manager via Manager.ApplyFunc.
func ApplyReplication(app core.App, event *cluster.ReplicationEvent) {
	if event.Table == "" || event.ID == "" {
		return
	}

	// Validate table name (basic sanity check against SQL injection)
	if !app.HasTable(event.Table) {
		// For deletes the table might already be gone; for creates/updates it shouldn't
		if event.Op != cluster.OpDelete {
			app.Logger().Warn("[cluster] replication received for unknown table",
				"table", event.Table, "id", event.ID)
			return
		}
	}

	key := cluster.ReplicationKey(event.Table, event.ID)
	cluster.MarkReplicating(key)
	defer cluster.UnmarkReplicating(key)

	switch event.Table {
	case collectionsTableName:
		applyCollectionReplication(app, event)
	default:
		applyRecordOrRawReplication(app, event)
	}
}

// applyCollectionReplication handles replication events for the _collections table.
// These require special treatment because they drive CREATE/ALTER/DROP TABLE operations.
func applyCollectionReplication(app core.App, event *cluster.ReplicationEvent) {
	if event.Op == cluster.OpDelete {
		col, err := app.FindCachedCollectionByNameOrId(event.ID)
		if err != nil {
			// Already gone on this node – nothing to do.
			return
		}
		// LWW: if the event carries the deleted collection's updated timestamp,
		// skip if our local collection was updated more recently. (Fix #1)
		if rawUpdated, ok := event.RawData["updated"]; ok {
			incomingUpdated, _ := types.ParseDateTime(rawUpdated)
			if !incomingUpdated.IsZero() && incomingUpdated.Before(col.Updated) {
				return // local collection is newer — skip delete
			}
		}
		if err := app.Delete(col); err != nil {
			app.Logger().Warn("[cluster] failed to delete replicated collection",
				"id", event.ID, "error", err)
		}
		return
	}

	// Create / update: unmarshal the full collection JSON.
	rawJSON, err := json.Marshal(event.RawData)
	if err != nil {
		app.Logger().Warn("[cluster] failed to marshal collection data", "error", err)
		return
	}

	col := &core.Collection{}
	if err := json.Unmarshal(rawJSON, col); err != nil {
		app.Logger().Warn("[cluster] failed to unmarshal collection", "error", err)
		return
	}

	// Determine whether this is a create or update on this node.
	existing, _ := app.FindCachedCollectionByNameOrId(col.Id)
	if existing != nil {
		// Skip if local collection is the same age or newer (last-write-wins).
		if !col.Updated.After(existing.Updated) {
			return
		}
		col.MarkAsNotNew()
	} else {
		// Collection doesn't exist locally. Skip if it was intentionally deleted here
		// after the incoming event's timestamp (last-write-wins for deletes).
		if localDeleteIsNewer(app, collectionsTableName, col.Id, col.Updated.Time()) {
			return
		}
	}

	if err := app.SaveNoValidate(col); err != nil {
		app.Logger().Warn("[cluster] failed to save replicated collection",
			"id", col.Id, "error", err)
	}
}

// applyRecordOrRawReplication handles replication events for regular records
// (user-defined collections and system auth collections such as _superusers).
// For tables that are not backed by a collection (e.g. _params), a raw SQL
// upsert / delete is performed.
func applyRecordOrRawReplication(app core.App, event *cluster.ReplicationEvent) {
	collection, colErr := app.FindCachedCollectionByNameOrId(event.Table)

	if colErr != nil {
		// Not a collection-backed table — raw SQL path.
		applyRawSQL(app, event)
		return
	}

	switch event.Op {
	case cluster.OpDelete:
		record, err := app.FindRecordById(collection, event.ID)
		if err != nil {
			// Record doesn't exist here – nothing to do.
			return
		}
		// LWW: if the event carries the deleted record's updated timestamp,
		// skip if our local record was updated more recently. (Fix #1)
		if rawUpdated, ok := event.RawData["updated"]; ok {
			incomingUpdated, _ := types.ParseDateTime(rawUpdated)
			if !incomingUpdated.IsZero() && incomingUpdated.Before(record.GetDateTime("updated")) {
				return // local record is newer — skip delete
			}
		}
		if err := app.Delete(record); err != nil {
			app.Logger().Warn("[cluster] failed to delete replicated record",
				"table", event.Table, "id", event.ID, "error", err)
		}

	case cluster.OpCreate, cluster.OpUpdate:
		record := core.NewRecord(collection)

		// Reconstruct from raw column data.
		if err := loadRecordFromRawData(record, event.RawData); err != nil {
			app.Logger().Warn("[cluster] failed to load record from raw data",
				"table", event.Table, "id", event.ID, "error", err)
			return
		}

		// Read the incoming updated timestamp directly from rawData.
		// AutodateField uses a noopSetter so record.Set("updated", ...) is silently
		// discarded and record.GetDateTime("updated") would always return zero.
		var incomingUpdated types.DateTime
		if rawUpdated, ok := event.RawData["updated"]; ok {
			incomingUpdated, _ = types.ParseDateTime(rawUpdated)
		}

		existing, findErr := app.FindRecordById(collection, event.ID)
		if findErr == nil {
			// Record exists locally. Skip if local version is the same age or newer (last-write-wins).
			if !incomingUpdated.After(existing.GetDateTime("updated")) {
				return
			}
			record.MarkAsNotNew()
		} else {
			// Record doesn't exist locally. Check if it was intentionally deleted after
			// the incoming event's timestamp — if so, skip re-insertion (last-write-wins for deletes).
			if localDeleteIsNewer(app, event.Table, event.ID, incomingUpdated.Time()) {
				return
			}
		}

		if err := app.SaveNoValidate(record); err != nil {
			app.Logger().Warn("[cluster] failed to save replicated record",
				"table", event.Table, "id", event.ID, "error", err)
			return
		}

		// Asynchronously replicate any files referenced by file fields.
		// This is best-effort: missing files will be logged but won't block record replication.
		if event.OriginURL != "" {
			go syncRecordFiles(app, collection, event)
		}
	}
}

// loadRecordFromRawData populates a Record from the raw DB column map received
// in a replication event.
//
// After record.Load() (which calls Set() for each key), two field types need
// special treatment via SetRaw():
//
//   - AutodateField: FindSetter returns noopSetter, so Set("updated", ...) is
//     silently discarded. Without SetRaw the interceptor would overwrite the
//     replicated timestamp with time.Now() on every save.
//
//   - PasswordField: setValue() treats any non-empty string as plaintext and
//     bcrypt-hashes it, corrupting an already-hashed value from the DB.
func loadRecordFromRawData(record *core.Record, data map[string]any) error {
	if data == nil {
		return fmt.Errorf("replication data is nil")
	}
	record.Load(data)

	for _, field := range record.Collection().Fields {
		switch field.Type() {
		case core.FieldTypeAutodate:
			name := field.GetName()
			if raw, ok := data[name]; ok {
				dt, _ := types.ParseDateTime(raw)
				if !dt.IsZero() {
					record.SetRaw(name, dt)
				}
			}
		case core.FieldTypePassword:
			name := field.GetName()
			if raw, ok := data[name]; ok {
				record.SetRaw(name, raw)
			}
		}
	}

	return nil
}

// applyRawSQL performs a raw INSERT OR REPLACE / DELETE for tables that are
// not managed through the PocketBase Collection API (e.g. _params).
func applyRawSQL(app core.App, event *cluster.ReplicationEvent) {
	if event.Op == cluster.OpDelete {
		_, err := app.NonconcurrentDB().
			NewQuery("DELETE FROM {{" + event.Table + "}} WHERE id={:id}").
			Bind(dbx.Params{"id": event.ID}).
			Execute()
		if err != nil {
			app.Logger().Warn("[cluster] raw SQL delete failed",
				"table", event.Table, "id", event.ID, "error", err)
		}

		// Reload settings if _params was modified
		if event.Table == "_params" {
			if err := app.ReloadSettings(); err != nil {
				app.Logger().Warn("[cluster] failed to reload settings after _params delete", "error", err)
			}
		}
		return
	}

	if len(event.RawData) == 0 {
		return
	}

	// LWW: skip upsert if the local row is the same age or newer. (Fix #2)
	if rawUpdated, ok := event.RawData["updated"]; ok {
		incomingUpdated, err := types.ParseDateTime(rawUpdated)
		if err == nil && !incomingUpdated.IsZero() {
			var localUpdated string
			if err := app.NonconcurrentDB().NewQuery(
				"SELECT updated FROM {{"+event.Table+"}} WHERE id={:id}",
			).Bind(dbx.Params{"id": event.ID}).Row(&localUpdated); err == nil {
				localDt, _ := types.ParseDateTime(localUpdated)
				if !incomingUpdated.After(localDt) {
					return // local is same age or newer
				}
			}
		}
	}

	sql, params := buildUpsertSQL(event.Table, event.RawData)
	if _, err := app.NonconcurrentDB().
		NewQuery(sql).
		Bind(dbx.Params(params)).
		Execute(); err != nil {
		app.Logger().Warn("[cluster] raw SQL upsert failed",
			"table", event.Table, "id", event.ID, "error", err)
		return
	}

	// Reload settings if _params was modified
	if event.Table == "_params" {
		if err := app.ReloadSettings(); err != nil {
			app.Logger().Warn("[cluster] failed to reload settings after _params upsert", "error", err)
		}
	}
}

// -----------------------------------------------------------------
// File replication
// -----------------------------------------------------------------

// syncRecordFiles checks all file fields of the replicated record and pulls any
// missing files from the originating node. Called asynchronously after SaveNoValidate.
//
// File keys follow the PocketBase convention: {collectionId}/{recordId}/{filename}
// Single-file fields store the filename as plain TEXT; multi-file fields store a
// JSON array string ("[\"a.jpg\",\"b.png\"]"). Both formats are handled here.
func syncRecordFiles(app core.App, collection *core.Collection, event *cluster.ReplicationEvent) {
	m := getClusterManager(app)
	if m == nil || event.OriginURL == "" {
		return
	}

	fsys, err := app.NewFilesystem()
	if err != nil {
		app.Logger().Warn("[cluster] file sync: failed to open filesystem", "error", err)
		return
	}
	defer fsys.Close()

	for _, field := range collection.Fields {
		if field.Type() != core.FieldTypeFile {
			continue
		}

		rawVal, ok := event.RawData[field.GetName()]
		if !ok || rawVal == nil {
			continue
		}

		// Parse the raw column value into a list of filenames.
		// Single-file: plain string. Multi-file: JSON array stored as string.
		var filenames []string
		switch v := rawVal.(type) {
		case string:
			s := strings.TrimSpace(v)
			if s == "" {
				continue
			}
			if strings.HasPrefix(s, "[") {
				// JSON array
				var arr []string
				if err := json.Unmarshal([]byte(s), &arr); err == nil {
					filenames = arr
				}
			} else {
				filenames = []string{s}
			}
		}

		baseDir := collection.BaseFilesPath() + "/" + event.ID
		for _, filename := range filenames {
			if filename == "" {
				continue
			}
			fileKey := baseDir + "/" + filename

			exists, err := fsys.Exists(fileKey)
			if err != nil {
				app.Logger().Warn("[cluster] file sync: exists check failed",
					"key", fileKey, "error", err)
				continue
			}
			if exists {
				continue
			}

			if err := fetchAndStoreFile(app, fsys, event.OriginURL, fileKey, m.Secret()); err != nil {
				app.Logger().Warn("[cluster] file sync: failed to fetch file",
					"key", fileKey, "origin", event.OriginURL, "error", err)
			} else {
				app.Logger().Info("[cluster] file sync: replicated file", "key", fileKey)
			}
		}
	}
}

// fetchAndStoreFile fetches a file from the originating node's /api/cluster/files endpoint
// and stores it in the local filesystem.
func fetchAndStoreFile(app core.App, fsys *filesystem.System, originURL, fileKey, secret string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fetchURL := strings.TrimRight(originURL, "/") + "/api/cluster/files?path=" + url.QueryEscape(fileKey)
	req, err := http.NewRequestWithContext(ctx, "GET", fetchURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-Cluster-Secret", secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("origin returned status %d for %s", resp.StatusCode, fileKey)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	return fsys.Upload(content, fileKey)
}

// buildUpsertSQL constructs an INSERT OR REPLACE statement from a map of column values.
// Returns the SQL string and a Params map whose keys match the {: placeholders.
func buildUpsertSQL(tableName string, data map[string]any) (string, map[string]any) {
	cols := make([]string, 0, len(data))
	for k := range data {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	quotedCols := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	params := make(map[string]any, len(cols))

	for i, col := range cols {
		// Use a prefix to avoid conflicts with dbx reserved words.
		// Double-quote the column identifier (SQL standard); double any embedded
		// quotes to prevent SQL injection via crafted column names. (Fix #5)
		paramKey := "p_" + col
		quotedCols[i] = `"` + strings.ReplaceAll(col, `"`, `""`) + `"`
		placeholders[i] = "{:" + paramKey + "}"
		params[paramKey] = data[col]
	}

	sql := fmt.Sprintf(
		"INSERT OR REPLACE INTO {{%s}} (%s) VALUES (%s)",
		tableName,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)

	return sql, params
}

// -----------------------------------------------------------------
// Initial sync support
// -----------------------------------------------------------------

// SyncAllTables streams the full contents of all replicable tables to all connected peers.
// Designed to be called in a goroutine.
func SyncAllTables(app core.App, manager *cluster.Manager, ctx context.Context) {
	syncCollectionSchemas(app, manager.Broadcast)
	for _, tableName := range getReplicableTables(app) {
		if ctx.Err() != nil {
			return
		}
		syncTable(app, manager.Broadcast, tableName)
	}
	syncDeletedRecords(app, manager.Broadcast, ctx)
}

// syncAllTablesToPeer streams the full contents of all replicable tables to a single peer.
// Called automatically when a new peer connects to keep it in sync with this node.
// Uses a blocking send so that no rows are dropped, even for large tables.
//
// After sending all current rows it also replays delete tombstones so that records
// deleted on this node while the peer was offline (or restarted) are removed on the peer.
func syncAllTablesToPeer(app core.App, manager *cluster.Manager, peerNodeID string, ctx context.Context) {
	send := func(e *cluster.ReplicationEvent) {
		manager.SendToPeer(peerNodeID, e, ctx)
	}
	syncCollectionSchemas(app, send)
	for _, tableName := range getReplicableTables(app) {
		if ctx.Err() != nil {
			return
		}
		syncTable(app, send, tableName)
	}
	syncDeletedRecords(app, send, ctx)
}

// getReplicableTables returns non-collection table names in sync order:
// _params first, then collection record tables.
// Collections are handled separately via syncCollectionSchemas.
func getReplicableTables(app core.App) []string {
	var tables []string

	// Settings first.
	if app.HasTable("_params") {
		tables = append(tables, "_params")
	}

	// All collection-backed tables (user-defined + system auth tables).
	collections, err := app.FindAllCollections()
	if err == nil {
		for _, col := range collections {
			tables = append(tables, col.Name)
		}
	}

	return tables
}

// syncDeletedRecords sends delete events for every (table, record_id) whose latest
// _cluster_log entry is a delete. Called at the end of a full sync so that records
// deleted on this node while a peer was offline or restarted are also removed on the peer.
// The receiving peer ignores the delete if it never had the record, so false-positives are harmless.
func syncDeletedRecords(app core.App, send func(*cluster.ReplicationEvent), ctx context.Context) {
	if !app.HasTable(clusterLogTable) {
		return
	}

	rows, err := app.NonconcurrentDB().NewQuery(`
		SELECT l.table_name, l.record_id, COALESCE(l.record_updated, '')
		FROM ` + clusterLogTable + ` l
		INNER JOIN (
			SELECT table_name, record_id, MAX(created) AS max_created
			FROM ` + clusterLogTable + `
			GROUP BY table_name, record_id
		) latest ON l.table_name = latest.table_name
			AND l.record_id = latest.record_id
			AND l.created = latest.max_created
		WHERE l.op = {:op}
	`).Bind(dbx.Params{"op": cluster.OpDelete}).Rows()
	if err != nil {
		app.Logger().Warn("[cluster] sync: failed to query deleted records", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		if ctx.Err() != nil {
			return
		}
		var tableName, recordID, recordUpdated string
		if err := rows.Scan(&tableName, &recordID, &recordUpdated); err != nil {
			continue
		}
		var rawData map[string]any
		if recordUpdated != "" {
			rawData = map[string]any{"updated": recordUpdated}
		}
		send(&cluster.ReplicationEvent{
			Op:      cluster.OpDelete,
			Table:   tableName,
			ID:      recordID,
			RawData: rawData,
		})
	}
}

// syncCollectionSchemas sends all collection schemas to a peer using the proper JSON API
// format (via json.Marshal on *core.Collection), so the peer can correctly reconstruct
// field definitions, indexes, etc. Must be called before syncTable for record tables.
func syncCollectionSchemas(app core.App, send func(*cluster.ReplicationEvent)) {
	collections, err := app.FindAllCollections()
	if err != nil {
		app.Logger().Warn("[cluster] sync: failed to list collections", "error", err)
		return
	}
	for _, col := range collections {
		b, err := json.Marshal(col)
		if err != nil {
			app.Logger().Warn("[cluster] sync: failed to marshal collection", "id", col.Id, "error", err)
			continue
		}
		var rawData map[string]any
		if err := json.Unmarshal(b, &rawData); err != nil {
			continue
		}
		send(&cluster.ReplicationEvent{
			Op:      cluster.OpCreate,
			Table:   collectionsTableName,
			ID:      col.Id,
			RawData: rawData,
		})
	}
}

// syncTable sends all rows from a single table as replication create events via send.
func syncTable(app core.App, send func(*cluster.ReplicationEvent), tableName string) {
	sqlRows, err := app.NonconcurrentDB().
		NewQuery("SELECT * FROM {{" + tableName + "}}").
		Rows()
	if err != nil {
		app.Logger().Warn("[cluster] sync: failed to query table",
			"table", tableName, "error", err)
		return
	}
	defer sqlRows.Close()

	cols, err := sqlRows.Columns()
	if err != nil {
		return
	}

	for sqlRows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := sqlRows.Scan(valuePtrs...); err != nil {
			continue
		}

		data := make(map[string]any, len(cols))
		id := ""
		for i, col := range cols {
			v := values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			data[col] = v
			if col == "id" {
				id, _ = v.(string)
			}
		}

		if id == "" {
			continue
		}

		send(&cluster.ReplicationEvent{
			Op:      cluster.OpCreate,
			Table:   tableName,
			ID:      id,
			RawData: data,
		})
	}
}

// -----------------------------------------------------------------
// Replication log — used for delta sync on reconnect
// -----------------------------------------------------------------

// ensureClusterLogTable creates _cluster_log if it doesn't exist, and runs any
// needed column migrations (record_updated was added in fix #1).
// created is stored as Unix nanoseconds (INTEGER) for reliable ordering and comparison.
func ensureClusterLogTable(app core.App) error {
	if _, err := app.NonconcurrentDB().NewQuery(`
		CREATE TABLE IF NOT EXISTS ` + clusterLogTable + ` (
			table_name     TEXT    NOT NULL,
			record_id      TEXT    NOT NULL,
			op             TEXT    NOT NULL,
			created        INTEGER NOT NULL,
			record_updated TEXT
		)
	`).Execute(); err != nil {
		return err
	}

	// Migration: add record_updated column if it doesn't exist.
	// "duplicate column name" is the expected error on already-migrated DBs — ignore it.
	app.NonconcurrentDB().NewQuery(
		`ALTER TABLE ` + clusterLogTable + ` ADD COLUMN record_updated TEXT`,
	).Execute() // nolint: intentionally ignore error

	if _, err := app.NonconcurrentDB().NewQuery(
		`CREATE INDEX IF NOT EXISTS idx_cluster_log_created ON ` + clusterLogTable + ` (created)`,
	).Execute(); err != nil {
		return err
	}

	// Compound index for efficient localDeleteIsNewer queries. (Fix #10)
	_, err := app.NonconcurrentDB().NewQuery(
		`CREATE INDEX IF NOT EXISTS idx_cluster_log_table_record ON ` + clusterLogTable + ` (table_name, record_id)`,
	).Execute()
	return err
}

// writeClusterLog appends one entry to the replication log.
// recordUpdated is the model's "updated" timestamp at the time of the operation
// (used by localDeleteIsNewer for LWW on delete events). Pass "" if not available.
func writeClusterLog(app core.App, tableName, recordID, op, recordUpdated string) {
	if _, err := app.NonconcurrentDB().NewQuery(
		`INSERT INTO `+clusterLogTable+` (table_name, record_id, op, created, record_updated) VALUES ({:table_name}, {:record_id}, {:op}, {:created}, {:record_updated})`,
	).Bind(dbx.Params{
		"table_name":     tableName,
		"record_id":      recordID,
		"op":             op,
		"created":        time.Now().UnixNano(),
		"record_updated": recordUpdated,
	}).Execute(); err != nil {
		app.Logger().Warn("[cluster] failed to write cluster log", "error", err)
	}
}

// bindClusterLogHooks registers model hooks that write every local change
// (both originating and applied-from-peer) into _cluster_log so that
// reconnecting peers can request a delta instead of a full sync.
func bindClusterLogHooks(app core.App) {
	write := func(e *core.ModelEvent, op string) error {
		if getClusterManager(e.App) != nil {
			if table := e.Model.TableName(); !tablesToSkip[table] {
				writeClusterLog(e.App, table, fmt.Sprint(e.Model.PK()), op, extractModelUpdated(e.Model))
			}
		}
		return e.Next()
	}
	app.OnModelAfterCreateSuccess().Bind(&hook.Handler[*core.ModelEvent]{
		Id: "clusterLogCreate", Priority: -97,
		Func: func(e *core.ModelEvent) error { return write(e, cluster.OpCreate) },
	})
	app.OnModelAfterUpdateSuccess().Bind(&hook.Handler[*core.ModelEvent]{
		Id: "clusterLogUpdate", Priority: -97,
		Func: func(e *core.ModelEvent) error { return write(e, cluster.OpUpdate) },
	})
	app.OnModelAfterDeleteSuccess().Bind(&hook.Handler[*core.ModelEvent]{
		Id: "clusterLogDelete", Priority: -97,
		Func: func(e *core.ModelEvent) error { return write(e, cluster.OpDelete) },
	})
}

// localDeleteIsNewer returns true if this node has a recorded delete for the given
// (tableName, recordID) that is based on a version at least as new as incomingUpdated.
// Used to avoid re-inserting records that were intentionally deleted on this node.
func localDeleteIsNewer(app core.App, tableName, recordID string, incomingUpdated time.Time) bool {
	var recordUpdated string
	var deleteCreated int64
	err := app.NonconcurrentDB().NewQuery(
		`SELECT COALESCE(record_updated, ''), created FROM `+clusterLogTable+
			` WHERE table_name={:table} AND record_id={:id} AND op={:op}`+
			` ORDER BY created DESC LIMIT 1`,
	).Bind(dbx.Params{
		"table": tableName,
		"id":    recordID,
		"op":    cluster.OpDelete,
	}).Row(&recordUpdated, &deleteCreated)
	if err != nil {
		// No delete entry found.
		return false
	}
	// Prefer record_updated (the record's own timestamp at deletion time) for
	// accurate LWW. Fall back to deleteCreated (log entry timestamp) for backward
	// compatibility with entries written before fix #1.
	if recordUpdated != "" {
		dt, err := types.ParseDateTime(recordUpdated)
		if err == nil && !dt.IsZero() {
			return !dt.Time().Before(incomingUpdated) // dt >= incomingUpdated
		}
	}
	return deleteCreated > incomingUpdated.UnixNano()
}

// pruneClusterLog runs on a 1-hour tick until done is closed (manager shutdown).
//
//   - Non-tombstone entries (create/update) are deleted after clusterLogRetention (7 days).
//   - Tombstone entries (delete) are deleted after clusterTombstoneRetention (30 days).
//
// After each tombstone prune the cutoff timestamp is written to _cluster_meta.
// clusterEventsHandler uses this watermark to force a full sync for peers whose
// X-Cluster-Since predates the cutoff, ensuring they don't miss pruned tombstones.
func pruneClusterLog(app core.App, done <-chan struct{}) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}

		// Prune non-tombstone entries older than clusterLogRetention (7 days).
		cutoff := time.Now().Add(-clusterLogRetention).UnixNano()
		if _, err := app.NonconcurrentDB().NewQuery(
			`DELETE FROM `+clusterLogTable+` WHERE created < {:cutoff} AND op != 'delete'`,
		).Bind(dbx.Params{"cutoff": cutoff}).Execute(); err != nil {
			app.Logger().Warn("[cluster] failed to prune cluster log", "error", err)
		}

		// Prune tombstones older than clusterTombstoneRetention (30 days).
		tombstoneCutoff := time.Now().Add(-clusterTombstoneRetention).UnixNano()
		if _, err := app.NonconcurrentDB().NewQuery(
			`DELETE FROM `+clusterLogTable+` WHERE created < {:cutoff} AND op = 'delete'`,
		).Bind(dbx.Params{"cutoff": tombstoneCutoff}).Execute(); err != nil {
			app.Logger().Warn("[cluster] failed to prune tombstones", "error", err)
			continue
		}

		// Persist the prune cutoff. Peers reconnecting with X-Cluster-Since before
		// this value are forced into a full sync by clusterEventsHandler.
		setTombstonePruneCutoff(app, tombstoneCutoff)
	}
}

// deltaSyncToPeer replays log entries written since `since` to a single peer.
// It deduplicates per (table, record_id) keeping only the latest op, then fetches
// current row data (for create/update) before sending. Falls back to a full sync
// if the log query fails (e.g. retention window exceeded).
func deltaSyncToPeer(app core.App, manager *cluster.Manager, peerNodeID string, since time.Time, ctx context.Context) {
	cutoff := since.Add(-clusterSyncBuffer).UnixNano()

	rows, err := app.NonconcurrentDB().NewQuery(
		`SELECT table_name, record_id, op FROM `+clusterLogTable+` WHERE created >= {:cutoff} ORDER BY created ASC`,
	).Bind(dbx.Params{"cutoff": cutoff}).Rows()
	if err != nil {
		app.Logger().Warn("[cluster] delta sync failed, falling back to full sync", "peer", peerNodeID, "error", err)
		syncAllTablesToPeer(app, manager, peerNodeID, ctx)
		return
	}

	// Scan into memory so we can close the cursor before doing per-row DB lookups.
	type entry struct{ table, id, op string }
	type key struct{ table, id string }
	var order []key
	seen := make(map[key]bool)
	latest := make(map[key]string) // key → latest op (ORDER BY created ASC so last write wins)

	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.table, &e.id, &e.op); err != nil {
			continue
		}
		k := key{e.table, e.id}
		if !seen[k] {
			order = append(order, k)
			seen[k] = true
		}
		latest[k] = e.op
	}
	rows.Close()

	for _, k := range order {
		if ctx.Err() != nil {
			return
		}
		op := latest[k]

		if op == cluster.OpDelete {
			// Include record_updated so the receiver can apply LWW on delete. (Fix #1)
			var recordUpdated string
			app.NonconcurrentDB().NewQuery(
				`SELECT COALESCE(record_updated, '') FROM `+clusterLogTable+
					` WHERE table_name={:table} AND record_id={:id} AND op='delete'`+
					` ORDER BY created DESC LIMIT 1`,
			).Bind(dbx.Params{"table": k.table, "id": k.id}).Row(&recordUpdated)

			var rawData map[string]any
			if recordUpdated != "" {
				rawData = map[string]any{"updated": recordUpdated}
			}
			manager.SendToPeer(peerNodeID, &cluster.ReplicationEvent{
				Op:      cluster.OpDelete,
				Table:   k.table,
				ID:      k.id,
				RawData: rawData,
			}, ctx)
			continue
		}

		// Fetch current row data for create/update.
		var rawData map[string]any
		if k.table == collectionsTableName {
			col, err := app.FindCachedCollectionByNameOrId(k.id)
			if err != nil {
				continue // collection gone
			}
			b, err := json.Marshal(col)
			if err != nil {
				continue
			}
			if err := json.Unmarshal(b, &rawData); err != nil {
				continue
			}
		} else {
			rawData, err = getRawRow(app, k.table, k.id)
			if err != nil {
				continue // row deleted after log entry was written
			}
		}

		manager.SendToPeer(peerNodeID, &cluster.ReplicationEvent{
			Op:      op,
			Table:   k.table,
			ID:      k.id,
			RawData: rawData,
		}, ctx)
	}
}

// -----------------------------------------------------------------
// Peer state persistence — used for delta sync across restarts (Fix #6)
// -----------------------------------------------------------------

// ensureClusterPeerStateTable creates _cluster_peer_state if it doesn't exist.
// This table stores the last successful sync time per peer URL so that
// node restarts continue with delta sync rather than a full sync.
func ensureClusterPeerStateTable(app core.App) error {
	_, err := app.NonconcurrentDB().NewQuery(`
		CREATE TABLE IF NOT EXISTS ` + clusterPeerStateTable + ` (
			peer_url     TEXT PRIMARY KEY,
			connected_at TEXT NOT NULL
		)
	`).Execute()
	return err
}

// loadPeerSyncTime returns the last recorded sync time for peerURL, or zero time if not found.
func loadPeerSyncTime(app core.App, peerURL string) time.Time {
	var connectedAt string
	if err := app.NonconcurrentDB().NewQuery(
		`SELECT connected_at FROM `+clusterPeerStateTable+` WHERE peer_url={:url}`,
	).Bind(dbx.Params{"url": peerURL}).Row(&connectedAt); err != nil {
		return time.Time{}
	}
	dt, err := types.ParseDateTime(connectedAt)
	if err != nil {
		return time.Time{}
	}
	return dt.Time()
}

// savePeerSyncTime persists the sync time for peerURL to the database.
func savePeerSyncTime(app core.App, peerURL string, syncedAt time.Time) {
	if _, err := app.NonconcurrentDB().NewQuery(
		`INSERT OR REPLACE INTO `+clusterPeerStateTable+` (peer_url, connected_at) VALUES ({:url}, {:at})`,
	).Bind(dbx.Params{
		"url": peerURL,
		"at":  syncedAt.UTC().Format(time.RFC3339Nano),
	}).Execute(); err != nil {
		app.Logger().Warn("[cluster] failed to save peer sync time", "error", err)
	}
}

// -----------------------------------------------------------------
// Cluster metadata — tombstone prune watermark (Fix #2)
// -----------------------------------------------------------------

// ensureClusterMetaTable creates _cluster_meta if it doesn't exist.
func ensureClusterMetaTable(app core.App) error {
	_, err := app.NonconcurrentDB().NewQuery(`
		CREATE TABLE IF NOT EXISTS ` + clusterMetaTable + ` (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`).Execute()
	return err
}

// getTombstonePruneCutoff returns the Unix nanosecond cutoff stored after the last
// tombstone prune, or 0 if tombstones have never been pruned on this node.
func getTombstonePruneCutoff(app core.App) int64 {
	var value string
	if err := app.NonconcurrentDB().NewQuery(
		`SELECT value FROM `+clusterMetaTable+` WHERE key = {:key}`,
	).Bind(dbx.Params{"key": metaKeyTombstoneCutoff}).Row(&value); err != nil {
		return 0
	}
	var ns int64
	fmt.Sscanf(value, "%d", &ns)
	return ns
}

// setTombstonePruneCutoff persists the tombstone prune cutoff to _cluster_meta.
func setTombstonePruneCutoff(app core.App, cutoffNS int64) {
	app.NonconcurrentDB().NewQuery(
		`INSERT OR REPLACE INTO `+clusterMetaTable+` (key, value) VALUES ({:key}, {:value})`,
	).Bind(dbx.Params{
		"key":   metaKeyTombstoneCutoff,
		"value": fmt.Sprint(cutoffNS),
	}).Execute() // nolint: intentionally ignore error
}
