package apis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/cluster"
	"golang.org/x/crypto/bcrypt"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newClusterApp creates a test app with the cluster log table initialised.
func newClusterApp(t *testing.T) *tests.TestApp {
	t.Helper()
	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatal("NewTestApp:", err)
	}
	t.Cleanup(app.Cleanup)
	if err := apis.TestEnsureClusterLogTable(app); err != nil {
		t.Fatal("ensureClusterLogTable:", err)
	}
	return app
}

// collectSendEvents returns a send function and a pointer to the collected events slice.
func collectSendEvents() (func(*cluster.ReplicationEvent), *[]*cluster.ReplicationEvent) {
	var events []*cluster.ReplicationEvent
	send := func(e *cluster.ReplicationEvent) {
		cp := *e
		events = append(events, &cp)
	}
	return send, &events
}

// mustFindRecord retrieves a record or fails the test.
func mustFindRecord(t *testing.T, app core.App, collectionOrTable, id string) *core.Record {
	t.Helper()
	col, err := app.FindCachedCollectionByNameOrId(collectionOrTable)
	if err != nil {
		t.Fatalf("FindCachedCollectionByNameOrId(%q): %v", collectionOrTable, err)
	}
	rec, err := app.FindRecordById(col, id)
	if err != nil {
		t.Fatalf("FindRecordById(%q, %q): %v", collectionOrTable, id, err)
	}
	return rec
}

// insertLogEntry writes a raw entry into the cluster log for test setup.
func insertLogEntry(t *testing.T, app core.App, table, id, op string, createdNano int64) {
	t.Helper()
	if _, err := app.NonconcurrentDB().NewQuery(
		`INSERT INTO ` + apis.TestClusterLogTable + ` (table_name, record_id, op, created) VALUES ({:t}, {:i}, {:o}, {:c})`,
	).Bind(map[string]any{"t": table, "i": id, "o": op, "c": createdNano}).Execute(); err != nil {
		t.Fatal("insertLogEntry:", err)
	}
}

// ---------------------------------------------------------------------------
// ApplyReplication — record create
// ---------------------------------------------------------------------------

func TestApplyReplication_CreateRecord(t *testing.T) {
	appA := newClusterApp(t)
	appB := newClusterApp(t)

	// "demo1" record "imy661ixudk5izi" exists in both apps via test data clone.
	// Delete it from appB so we can test the create path.
	const table = "demo1"
	const id = "imy661ixudk5izi"

	recB := mustFindRecord(t, appB, table, id)
	if err := appB.Delete(recB); err != nil {
		t.Fatal("Delete on appB:", err)
	}

	rawData, err := apis.TestGetRawRow(appA, table, id)
	if err != nil {
		t.Fatal("TestGetRawRow:", err)
	}

	event := &cluster.ReplicationEvent{Op: cluster.OpCreate, Table: table, ID: id, RawData: rawData}
	apis.ApplyReplication(appB, event)

	if _, err := appB.FindRecordById(mustFindRecord(t, appB, table, id).Collection(), id); err != nil {
		t.Fatalf("record %q not found on appB after create replication: %v", id, err)
	}
}

// ---------------------------------------------------------------------------
// ApplyReplication — last-write-wins for updates
// ---------------------------------------------------------------------------

func TestApplyReplication_UpdateRecord_NewerWins(t *testing.T) {
	app := newClusterApp(t)

	const table = "demo1"
	const id = "imy661ixudk5izi"

	rawData, err := apis.TestGetRawRow(app, table, id)
	if err != nil {
		t.Fatal("TestGetRawRow:", err)
	}

	// Bump updated to be newer than the existing record.
	rawData["updated"] = time.Now().Add(time.Hour).UTC().Format("2006-01-02 15:04:05.000Z")
	rawData["text"] = "replicated-newer"

	apis.ApplyReplication(app, &cluster.ReplicationEvent{Op: cluster.OpUpdate, Table: table, ID: id, RawData: rawData})

	updated := mustFindRecord(t, app, table, id)
	if updated.GetString("text") != "replicated-newer" {
		t.Fatalf("newer incoming should have won: expected text=%q, got %q",
			"replicated-newer", updated.GetString("text"))
	}
}

func TestApplyReplication_UpdateRecord_OlderDiscarded(t *testing.T) {
	app := newClusterApp(t)

	const table = "demo1"
	const id = "imy661ixudk5izi"

	original := mustFindRecord(t, app, table, id)
	originalText := original.GetString("text")

	rawData, _ := apis.TestGetRawRow(app, table, id)
	// Use a date well before any test data timestamp to ensure the incoming event is older.
	rawData["updated"] = "2000-01-01 00:00:00.000Z"
	rawData["text"] = "should-not-apply"

	apis.ApplyReplication(app, &cluster.ReplicationEvent{Op: cluster.OpUpdate, Table: table, ID: id, RawData: rawData})

	current := mustFindRecord(t, app, table, id)
	if current.GetString("text") != originalText {
		t.Fatalf("older incoming should have been discarded: expected text=%q, got %q",
			originalText, current.GetString("text"))
	}
}

// ---------------------------------------------------------------------------
// ApplyReplication — delete
// ---------------------------------------------------------------------------

func TestApplyReplication_DeleteRecord(t *testing.T) {
	app := newClusterApp(t)

	const table = "demo1"
	const id = "imy661ixudk5izi"

	mustFindRecord(t, app, table, id) // verify exists

	apis.ApplyReplication(app, &cluster.ReplicationEvent{Op: cluster.OpDelete, Table: table, ID: id})

	col, _ := app.FindCachedCollectionByNameOrId(table)
	if _, err := app.FindRecordById(col, id); err == nil {
		t.Fatalf("record %q should have been deleted but still exists", id)
	}
}

// ---------------------------------------------------------------------------
// ApplyReplication — local-delete-is-newer guard
// ---------------------------------------------------------------------------

func TestApplyReplication_CreateIgnored_WhenLocalDeleteIsNewer(t *testing.T) {
	app := newClusterApp(t)

	const table = "demo1"
	const id = "imy661ixudk5izi"

	rawData, _ := apis.TestGetRawRow(app, table, id)

	// Write a delete log entry with a timestamp AFTER the incoming event's updated timestamp.
	incomingUpdated, _ := time.Parse("2006-01-02 15:04:05.000Z", fmt.Sprintf("%v", rawData["updated"]))
	insertLogEntry(t, app, table, id, cluster.OpDelete, incomingUpdated.Add(time.Minute).UnixNano())

	// Delete the record locally.
	rec := mustFindRecord(t, app, table, id)
	if err := app.Delete(rec); err != nil {
		t.Fatal("Delete:", err)
	}

	// Apply create event — should be ignored because local delete is newer.
	apis.ApplyReplication(app, &cluster.ReplicationEvent{Op: cluster.OpCreate, Table: table, ID: id, RawData: rawData})

	col, _ := app.FindCachedCollectionByNameOrId(table)
	if _, err := app.FindRecordById(col, id); err == nil {
		t.Fatal("record was re-created but local delete should have won")
	}
}

func TestApplyReplication_CreateApplied_WhenLocalDeleteIsOlder(t *testing.T) {
	app := newClusterApp(t)

	const table = "demo1"
	const id = "imy661ixudk5izi"

	rawData, _ := apis.TestGetRawRow(app, table, id)

	// Write a delete log entry that is OLDER than the incoming event's updated timestamp.
	insertLogEntry(t, app, table, id, cluster.OpDelete, time.Now().Add(-time.Hour).UnixNano())

	// Delete locally.
	rec := mustFindRecord(t, app, table, id)
	if err := app.Delete(rec); err != nil {
		t.Fatal("Delete:", err)
	}

	// Give the incoming event a newer updated timestamp so it wins.
	rawData["updated"] = time.Now().Add(time.Hour).UTC().Format("2006-01-02 15:04:05.000Z")
	apis.ApplyReplication(app, &cluster.ReplicationEvent{Op: cluster.OpCreate, Table: table, ID: id, RawData: rawData})

	col, _ := app.FindCachedCollectionByNameOrId(table)
	if _, err := app.FindRecordById(col, id); err != nil {
		t.Fatal("record should have been re-created (incoming create is newer than local delete):", err)
	}
}

// ---------------------------------------------------------------------------
// Password hash preservation
// ---------------------------------------------------------------------------

func TestLoadRecordFromRawData_PasswordHashPreserved(t *testing.T) {
	app := newClusterApp(t)

	// Create a known bcrypt hash.
	plainPassword := "test-password-1234"
	hash, err := bcrypt.GenerateFromPassword([]byte(plainPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal("bcrypt.GenerateFromPassword:", err)
	}
	bcryptHash := string(hash)

	col, err := app.FindCachedCollectionByNameOrId(core.CollectionNameSuperusers)
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId:", err)
	}

	rawData := map[string]any{
		"id":       "testpasswdXXXXXX",
		"email":    "testpwd@example.com",
		"password": bcryptHash,
		"tokenKey": "someRandomTokenKey",
		"verified": true,
		"created":  time.Now().UTC().Format("2006-01-02 15:04:05.000Z"),
		"updated":  time.Now().UTC().Format("2006-01-02 15:04:05.000Z"),
	}

	rec := core.NewRecord(col)
	if err := apis.TestLoadRecordFromRawData(rec, rawData); err != nil {
		t.Fatal("TestLoadRecordFromRawData:", err)
	}

	storedHash := rec.GetString("password:hash")
	if storedHash == "" {
		t.Fatal("password hash is empty after loadRecordFromRawData")
	}
	if storedHash != bcryptHash {
		t.Fatalf("password hash was corrupted (re-hashed):\n  original: %s\n  stored:   %s", bcryptHash, storedHash)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(plainPassword)); err != nil {
		t.Fatalf("stored hash does not validate against original password: %v", err)
	}
}

func TestApplyReplication_AuthRecord_PasswordHashPreservedAfterSync(t *testing.T) {
	appA := newClusterApp(t)
	appB := newClusterApp(t)

	const superuserID = "sywbhecnh46rhm0" // exists in test data

	rawData, err := apis.TestGetRawRow(appA, "_superusers", superuserID)
	if err != nil {
		t.Fatal("TestGetRawRow:", err)
	}

	hashOnA, _ := rawData["password"].(string)
	if hashOnA == "" {
		t.Fatal("password hash empty in raw data from appA")
	}

	// Delete from appB first to ensure we test the create path.
	colB, _ := appB.FindCachedCollectionByNameOrId(core.CollectionNameSuperusers)
	if recB, err := appB.FindRecordById(colB, superuserID); err == nil {
		_ = appB.Delete(recB)
	}

	apis.ApplyReplication(appB, &cluster.ReplicationEvent{
		Op:      cluster.OpCreate,
		Table:   "_superusers",
		ID:      superuserID,
		RawData: rawData,
	})

	colB2, _ := appB.FindCachedCollectionByNameOrId(core.CollectionNameSuperusers)
	recB, err := appB.FindRecordById(colB2, superuserID)
	if err != nil {
		t.Fatal("superuser not found on appB after replication:", err)
	}

	hashOnB := recB.GetString("password:hash")
	if hashOnA != hashOnB {
		t.Fatalf("password hash mismatch after replication:\n  appA: %v\n  appB: %v", hashOnA, hashOnB)
	}
}

// ---------------------------------------------------------------------------
// Collection schema replication
// ---------------------------------------------------------------------------

func TestApplyCollectionReplication_Create(t *testing.T) {
	appA := newClusterApp(t)
	appB := newClusterApp(t)

	// Create a collection only on A.
	col := core.NewBaseCollection("cluster_new_col")
	if err := appA.Save(col); err != nil {
		t.Fatal("Save collection on A:", err)
	}

	b, _ := json.Marshal(col)
	rawData := map[string]any{}
	json.Unmarshal(b, &rawData)

	apis.ApplyReplication(appB, &cluster.ReplicationEvent{
		Op:      cluster.OpCreate,
		Table:   apis.TestCollectionsTableName,
		ID:      col.Id,
		RawData: rawData,
	})

	if _, err := appB.FindCachedCollectionByNameOrId(col.Name); err != nil {
		t.Fatalf("collection %q not found on appB after create replication: %v", col.Name, err)
	}
}

func TestApplyCollectionReplication_Update_NewerWins(t *testing.T) {
	app := newClusterApp(t)

	col, err := app.FindCachedCollectionByNameOrId("demo1")
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId:", err)
	}

	b, _ := json.Marshal(col)
	rawData := map[string]any{}
	json.Unmarshal(b, &rawData)
	rawData["updated"] = time.Now().Add(time.Hour).UTC().Format("2006-01-02 15:04:05.999Z")

	// Should apply without error.
	apis.ApplyReplication(app, &cluster.ReplicationEvent{
		Op:      cluster.OpUpdate,
		Table:   apis.TestCollectionsTableName,
		ID:      col.Id,
		RawData: rawData,
	})

	// Collection should still exist.
	if _, err := app.FindCachedCollectionByNameOrId(col.Id); err != nil {
		t.Fatal("collection disappeared after update replication:", err)
	}
}

func TestApplyCollectionReplication_Update_OlderDiscarded(t *testing.T) {
	app := newClusterApp(t)

	col, err := app.FindCachedCollectionByNameOrId("demo1")
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId:", err)
	}

	b, _ := json.Marshal(col)
	rawData := map[string]any{}
	json.Unmarshal(b, &rawData)
	// Use a date well before any test data timestamp to ensure the incoming event is older.
	rawData["updated"] = "2000-01-01 00:00:00.000Z"
	rawData["name"] = "should_not_apply"

	apis.ApplyReplication(app, &cluster.ReplicationEvent{
		Op:      cluster.OpUpdate,
		Table:   apis.TestCollectionsTableName,
		ID:      col.Id,
		RawData: rawData,
	})

	reloaded, _ := app.FindCachedCollectionByNameOrId(col.Id)
	if reloaded != nil && reloaded.Name == "should_not_apply" {
		t.Fatal("older collection update was applied but should have been discarded")
	}
}

func TestApplyCollectionReplication_Delete(t *testing.T) {
	app := newClusterApp(t)

	col := core.NewBaseCollection("delete_me_col")
	if err := app.Save(col); err != nil {
		t.Fatal("Save collection:", err)
	}

	apis.ApplyReplication(app, &cluster.ReplicationEvent{
		Op:    cluster.OpDelete,
		Table: apis.TestCollectionsTableName,
		ID:    col.Id,
	})

	if _, err := app.FindCachedCollectionByNameOrId(col.Id); err == nil {
		t.Fatal("collection should have been deleted but still exists")
	}
}

func TestApplyCollectionReplication_LocalDeleteNewer_IncomingCreateIgnored(t *testing.T) {
	app := newClusterApp(t)

	col := core.NewBaseCollection("local_del_test_col")
	if err := app.Save(col); err != nil {
		t.Fatal("Save collection:", err)
	}

	// Write a delete log entry newer than the incoming event's updated timestamp.
	insertLogEntry(t, app, apis.TestCollectionsTableName, col.Id, cluster.OpDelete,
		time.Now().Add(time.Hour).UnixNano())

	if err := app.Delete(col); err != nil {
		t.Fatal("Delete collection:", err)
	}

	b, _ := json.Marshal(col)
	rawData := map[string]any{}
	json.Unmarshal(b, &rawData)
	rawData["updated"] = time.Now().Add(-time.Minute).UTC().Format("2006-01-02 15:04:05.999Z")

	apis.ApplyReplication(app, &cluster.ReplicationEvent{
		Op:      cluster.OpCreate,
		Table:   apis.TestCollectionsTableName,
		ID:      col.Id,
		RawData: rawData,
	})

	if _, err := app.FindCachedCollectionByNameOrId(col.Id); err == nil {
		t.Fatal("collection was re-created but local delete should have won")
	}
}

// ---------------------------------------------------------------------------
// localDeleteIsNewer
// ---------------------------------------------------------------------------

func TestLocalDeleteIsNewer_NoEntry(t *testing.T) {
	app := newClusterApp(t)
	if apis.TestLocalDeleteIsNewer(app, "demo1", "nonexistent_id", time.Now()) {
		t.Fatal("expected false when no log entry exists")
	}
}

func TestLocalDeleteIsNewer_OlderDelete(t *testing.T) {
	app := newClusterApp(t)
	ref := time.Now()

	insertLogEntry(t, app, "demo1", "rec_lww_1", cluster.OpDelete, ref.Add(-time.Minute).UnixNano())

	if apis.TestLocalDeleteIsNewer(app, "demo1", "rec_lww_1", ref) {
		t.Fatal("expected false: log delete is older than incomingUpdated")
	}
}

func TestLocalDeleteIsNewer_NewerDelete(t *testing.T) {
	app := newClusterApp(t)
	ref := time.Now()

	insertLogEntry(t, app, "demo1", "rec_lww_2", cluster.OpDelete, ref.Add(time.Minute).UnixNano())

	if !apis.TestLocalDeleteIsNewer(app, "demo1", "rec_lww_2", ref) {
		t.Fatal("expected true: log delete is newer than incomingUpdated")
	}
}

func TestLocalDeleteIsNewer_CreateAfterDelete_StillReturnsTrue(t *testing.T) {
	// The function checks the NEWEST delete entry, not whether the last op is delete.
	// If we have delete(t+1m) and create(t+2m), the newest delete is at t+1m which is > t.
	// So localDeleteIsNewer still returns true, which is the right defensive behaviour:
	// the caller's LWW check will sort it out via the existing-record path.
	app := newClusterApp(t)
	ref := time.Now()

	insertLogEntry(t, app, "demo1", "rec_lww_3", cluster.OpDelete, ref.Add(time.Minute).UnixNano())
	insertLogEntry(t, app, "demo1", "rec_lww_3", cluster.OpCreate, ref.Add(2*time.Minute).UnixNano())

	if !apis.TestLocalDeleteIsNewer(app, "demo1", "rec_lww_3", ref) {
		t.Fatal("expected true: the most recent delete entry is newer than incomingUpdated")
	}
}

// ---------------------------------------------------------------------------
// syncDeletedRecords — full-sync tombstone replay
// ---------------------------------------------------------------------------

func TestSyncDeletedRecords_SendsDeletesForLatestDeleteOp(t *testing.T) {
	app := newClusterApp(t)

	now := time.Now().UnixNano()

	// rec_del_x: create then delete → latest is delete → should be sent
	// rec_del_y: delete only → latest is delete → should be sent
	// rec_del_z: create only → latest is create → should NOT be sent
	// rec_del_w: delete then create → latest is create → should NOT be sent
	entries := []struct {
		table, id, op string
		ts            int64
	}{
		{"demo1", "rec_del_x", cluster.OpCreate, now - 4000},
		{"demo1", "rec_del_x", cluster.OpDelete, now - 3000},
		{"demo1", "rec_del_y", cluster.OpDelete, now - 2000},
		{"demo1", "rec_del_z", cluster.OpCreate, now - 1000},
		{"demo1", "rec_del_w", cluster.OpDelete, now - 500},
		{"demo1", "rec_del_w", cluster.OpCreate, now - 100},
	}
	for _, e := range entries {
		insertLogEntry(t, app, e.table, e.id, e.op, e.ts)
	}

	send, events := collectSendEvents()
	apis.TestSyncDeletedRecords(app, send, context.Background())

	deleted := map[string]bool{}
	for _, ev := range *events {
		if ev.Op == cluster.OpDelete {
			deleted[ev.Table+":"+ev.ID] = true
		}
	}

	if !deleted["demo1:rec_del_x"] {
		t.Error("expected delete event for rec_del_x (create→delete)")
	}
	if !deleted["demo1:rec_del_y"] {
		t.Error("expected delete event for rec_del_y (delete only)")
	}
	if deleted["demo1:rec_del_z"] {
		t.Error("should NOT have delete event for rec_del_z (create only)")
	}
	if deleted["demo1:rec_del_w"] {
		t.Error("should NOT have delete event for rec_del_w (delete→create, latest is create)")
	}
}

func TestSyncDeletedRecords_EmptyLog_NoEvents(t *testing.T) {
	app := newClusterApp(t)

	send, events := collectSendEvents()
	apis.TestSyncDeletedRecords(app, send, context.Background())

	for _, ev := range *events {
		if ev.Op == cluster.OpDelete {
			t.Errorf("unexpected delete event with empty log: table=%q id=%q", ev.Table, ev.ID)
		}
	}
}

// ---------------------------------------------------------------------------
// Full-sync integration: deleted records must not reappear after node restart
// ---------------------------------------------------------------------------

func TestFullSync_DeletedRecordsRemovedFromPeer(t *testing.T) {
	appA := newClusterApp(t)
	appB := newClusterApp(t)

	// Both apps have the same test data. "imy661ixudk5izi" exists on both.
	const table = "demo1"
	const deletedID = "imy661ixudk5izi"

	// Step 1: delete the record from appA and write to its cluster log.
	recA := mustFindRecord(t, appA, table, deletedID)
	if err := appA.Delete(recA); err != nil {
		t.Fatal("Delete on appA:", err)
	}
	apis.TestWriteClusterLog(appA, table, deletedID, cluster.OpDelete, "")

	// Verify appB still has it (simulates peer that was offline).
	mustFindRecord(t, appB, table, deletedID)

	// Step 2: simulate full sync from appA to appB (what happens on reconnect after restart).
	send, events := collectSendEvents()
	ctx := context.Background()
	apis.TestSyncCollectionSchemas(appA, send)
	for _, tbl := range apis.TestGetReplicableTables(appA) {
		if ctx.Err() != nil {
			break
		}
		apis.TestSyncTable(appA, send, tbl)
	}
	apis.TestSyncDeletedRecords(appA, send, ctx)

	// Apply all events to appB.
	for _, ev := range *events {
		apis.ApplyReplication(appB, ev)
	}

	// Deleted record must NOT exist on appB.
	colB, _ := appB.FindCachedCollectionByNameOrId(table)
	if _, err := appB.FindRecordById(colB, deletedID); err == nil {
		t.Fatalf("record %q was not deleted on appB: deleted-while-offline records must not reappear", deletedID)
	}
}

func TestFullSync_ExistingRecordsSurvivedOnPeer(t *testing.T) {
	appA := newClusterApp(t)
	appB := newClusterApp(t)

	// "al1h9ijdeojtsjy" exists in test data on both nodes and should still be on B after sync.
	const table = "demo1"
	const survivingID = "al1h9ijdeojtsjy"

	send, events := collectSendEvents()
	ctx := context.Background()
	apis.TestSyncCollectionSchemas(appA, send)
	for _, tbl := range apis.TestGetReplicableTables(appA) {
		apis.TestSyncTable(appA, send, tbl)
	}
	apis.TestSyncDeletedRecords(appA, send, ctx)

	for _, ev := range *events {
		apis.ApplyReplication(appB, ev)
	}

	mustFindRecord(t, appB, table, survivingID)
}

// ---------------------------------------------------------------------------
// syncCollectionSchemas — proper JSON format
// ---------------------------------------------------------------------------

func TestSyncCollectionSchemas_IncludesAllCollections(t *testing.T) {
	app := newClusterApp(t)

	send, events := collectSendEvents()
	apis.TestSyncCollectionSchemas(app, send)

	allCols, err := app.FindAllCollections()
	if err != nil {
		t.Fatal("FindAllCollections:", err)
	}

	sentIDs := map[string]bool{}
	for _, ev := range *events {
		if ev.Table == apis.TestCollectionsTableName {
			sentIDs[ev.ID] = true
		}
	}
	for _, col := range allCols {
		if !sentIDs[col.Id] {
			t.Errorf("collection %q (%s) was not included in syncCollectionSchemas output", col.Name, col.Id)
		}
	}
}

func TestSyncCollectionSchemas_EventUnmarshalsCorrectly(t *testing.T) {
	appA := newClusterApp(t)
	appB := newClusterApp(t)

	// Create a collection with a text field only on A.
	col := core.NewBaseCollection("schema_sync_col")
	if err := appA.Save(col); err != nil {
		t.Fatal("Save collection on A:", err)
	}

	send, events := collectSendEvents()
	apis.TestSyncCollectionSchemas(appA, send)

	for _, ev := range *events {
		apis.ApplyReplication(appB, ev)
	}

	if _, err := appB.FindCachedCollectionByNameOrId(col.Name); err != nil {
		t.Fatalf("collection %q not found on appB after schema sync: %v", col.Name, err)
	}
}

// ---------------------------------------------------------------------------
// serializeModelForReplication — collection uses JSON API format, not raw SQL
// ---------------------------------------------------------------------------

func TestSerializeModelForReplication_CollectionUsesJSONFormat(t *testing.T) {
	app := newClusterApp(t)

	col, err := app.FindCachedCollectionByNameOrId("demo1")
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId:", err)
	}

	data, err := apis.TestSerializeModelForRepli(app, col, cluster.OpCreate)
	if err != nil {
		t.Fatal("TestSerializeModelForRepli:", err)
	}

	b, _ := json.Marshal(data)
	restored := &core.Collection{}
	if err := json.Unmarshal(b, restored); err != nil {
		t.Fatalf("serialized collection data does not unmarshal into core.Collection: %v", err)
	}
	if restored.Name != col.Name {
		t.Fatalf("collection name mismatch: got %q, want %q", restored.Name, col.Name)
	}
}

// ---------------------------------------------------------------------------
// Loop prevention — replicating flag skips broadcast
// ---------------------------------------------------------------------------

func TestApplyReplication_SkipsTablesMissingFromDB(t *testing.T) {
	app := newClusterApp(t)

	// Sending an event for a completely unknown table should not panic.
	event := &cluster.ReplicationEvent{
		Op:    cluster.OpCreate,
		Table: "nonexistent_table_xyz",
		ID:    "abc123",
		RawData: map[string]any{
			"id":      "abc123",
			"updated": time.Now().UTC().Format("2006-01-02 15:04:05.000Z"),
		},
	}
	// Should not panic, just log a warning.
	apis.ApplyReplication(app, event)
}

func TestApplyReplication_EmptyEventIgnored(t *testing.T) {
	app := newClusterApp(t)

	// Empty table/id should be silently ignored.
	apis.ApplyReplication(app, &cluster.ReplicationEvent{Op: cluster.OpCreate})
	apis.ApplyReplication(app, &cluster.ReplicationEvent{Table: "demo1"})
}

// ---------------------------------------------------------------------------
// Auth collection secret preservation during cluster replication
// ---------------------------------------------------------------------------

func TestMarshalCollectionForCluster_IncludesAuthSecrets(t *testing.T) {
	app := newClusterApp(t)

	// Find a system auth collection (_superusers).
	col, err := app.FindCachedCollectionByNameOrId("_superusers")
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId(_superusers):", err)
	}

	if !col.IsAuth() {
		t.Fatal("_superusers should be an auth collection")
	}

	// Verify the collection has non-empty secrets.
	if col.AuthToken.Secret == "" {
		t.Fatal("_superusers AuthToken.Secret is empty before test; expected a non-empty secret")
	}

	// Marshal for cluster — should include secrets.
	rawData, err := apis.TestMarshalCollectionForCluster(col)
	if err != nil {
		t.Fatal("marshalCollectionForCluster:", err)
	}

	// Check that authToken.secret is present and matches the original.
	authTokenMap, ok := rawData["authToken"].(map[string]any)
	if !ok {
		t.Fatal("rawData[\"authToken\"] is missing or not a map")
	}
	secret, ok := authTokenMap["secret"].(string)
	if !ok || secret == "" {
		t.Fatal("authToken.secret is missing or empty in cluster-serialized data")
	}
	if secret != col.AuthToken.Secret {
		t.Fatalf("authToken.secret mismatch: got %q, want %q", secret, col.AuthToken.Secret)
	}

	// Also check other token secrets.
	for _, key := range []string{"fileToken", "passwordResetToken", "emailChangeToken", "verificationToken"} {
		tkMap, ok := rawData[key].(map[string]any)
		if !ok {
			t.Errorf("rawData[%q] is missing or not a map", key)
			continue
		}
		s, ok := tkMap["secret"].(string)
		if !ok || s == "" {
			t.Errorf("%s.secret is missing or empty in cluster-serialized data", key)
		}
	}

	// Verify that standard json.Marshal blanks the secrets (baseline assertion).
	stdJSON, _ := json.Marshal(col)
	var stdData map[string]any
	json.Unmarshal(stdJSON, &stdData)
	if stdAuthToken, ok := stdData["authToken"].(map[string]any); ok {
		if s, _ := stdAuthToken["secret"].(string); s != "" {
			t.Fatal("standard json.Marshal should blank authToken.secret, but it was present")
		}
	}
}

func TestSyncCollectionSchemas_PreservesAuthSecretsOnPeer(t *testing.T) {
	appA := newClusterApp(t)
	appB := newClusterApp(t)

	// Get the _superusers auth collection from A.
	colA, err := appA.FindCachedCollectionByNameOrId("_superusers")
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId(_superusers) on A:", err)
	}
	originalSecret := colA.AuthToken.Secret
	if originalSecret == "" {
		t.Fatal("_superusers AuthToken.Secret is empty on A")
	}

	// Sync all collection schemas from A to B.
	send, events := collectSendEvents()
	apis.TestSyncCollectionSchemas(appA, send)

	for _, ev := range *events {
		apis.ApplyReplication(appB, ev)
	}

	// Verify that B's _superusers collection has A's auth token secret.
	colB, err := appB.FindCachedCollectionByNameOrId("_superusers")
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId(_superusers) on B:", err)
	}

	if colB.AuthToken.Secret != originalSecret {
		t.Fatalf("AuthToken.Secret mismatch after sync:\n  got  %q\n  want %q", colB.AuthToken.Secret, originalSecret)
	}
}

func TestApplyCollectionReplication_AuthSecret_PreservedOnUpdate(t *testing.T) {
	app := newClusterApp(t)

	// Get the _superusers collection.
	col, err := app.FindCachedCollectionByNameOrId("_superusers")
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId:", err)
	}
	originalSecret := col.AuthToken.Secret
	if originalSecret == "" {
		t.Fatal("_superusers AuthToken.Secret is empty")
	}

	// Build a replication event with the correct secrets (as marshalCollectionForCluster would produce).
	rawData, err := apis.TestMarshalCollectionForCluster(col)
	if err != nil {
		t.Fatal("marshalCollectionForCluster:", err)
	}

	// Set a future timestamp so LWW allows the update.
	rawData["updated"] = time.Now().Add(time.Hour).UTC().Format("2006-01-02 15:04:05.999Z")

	apis.ApplyReplication(app, &cluster.ReplicationEvent{
		Op:      cluster.OpUpdate,
		Table:   apis.TestCollectionsTableName,
		ID:      col.Id,
		RawData: rawData,
	})

	// Reload the collection and verify the secret is preserved.
	reloaded, err := app.FindCachedCollectionByNameOrId(col.Id)
	if err != nil {
		t.Fatal("FindCachedCollectionByNameOrId after replication:", err)
	}
	if reloaded.AuthToken.Secret != originalSecret {
		t.Fatalf("AuthToken.Secret changed after replication:\n  got  %q\n  want %q", reloaded.AuthToken.Secret, originalSecret)
	}
}
