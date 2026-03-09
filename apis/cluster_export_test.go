package apis

// Exported wrappers for internal cluster functions, available only during testing.

var (
	TestEnsureClusterLogTable    = ensureClusterLogTable
	TestWriteClusterLog          = writeClusterLog
	TestLocalDeleteIsNewer       = localDeleteIsNewer
	TestSyncDeletedRecords       = syncDeletedRecords
	TestSyncCollectionSchemas    = syncCollectionSchemas
	TestGetReplicableTables      = getReplicableTables
	TestSyncTable                = syncTable
	TestSerializeModelForRepli   = serializeModelForReplication
	TestGetRawRow                = getRawRow
	TestLoadRecordFromRawData    = loadRecordFromRawData
)

const TestCollectionsTableName = collectionsTableName
const TestClusterLogTable = clusterLogTable
