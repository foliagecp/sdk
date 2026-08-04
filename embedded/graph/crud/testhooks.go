package crud

import "sync"

// Test-only observability for the crud package globals. These caches live for
// the whole process (they are not bound to a Runtime), so leak tests need a
// way to measure them and to isolate themselves from earlier suites in the
// same test binary.

// ObjectTypeCacheSizeForTest returns the number of objectID->typeID entries in
// the package-global objectTypeCache.
func ObjectTypeCacheSizeForTest() int {
	n := 0
	objectTypeCache.Range(func(_, _ any) bool { n++; return true })
	return n
}

// GraphKeyMutexEntriesForTest returns the live entry count of the package-
// global graph per-key mutex. On a quiesced runtime this must be zero.
func GraphKeyMutexEntriesForTest() int {
	return graphIdKeyMutex.EntryCountForTest()
}

// TypeEdgeCacheSizeForTest returns the number of outer fromType entries in
// the package-global type2TypeObjectLinkTypeCache.
func TypeEdgeCacheSizeForTest() int {
	n := 0
	type2TypeObjectLinkTypeCache.Range(func(_, _ any) bool { n++; return true })
	return n
}

// ResetPackageCachesForTest clears every package-global cache sync.Map
// (objectTypeCache, type2TypeObjectLinkTypeCache, typeObjectTriggersCache,
// typesLinkTriggersCache, typeHRNFieldCache). The caches survive across
// runtimes within one process, so tests call this to start from a clean,
// comparable baseline.
func ResetPackageCachesForTest() {
	clearSyncMapForTest(&objectTypeCache)
	clearSyncMapForTest(&type2TypeObjectLinkTypeCache)
	clearSyncMapForTest(&typeObjectTriggersCache)
	clearSyncMapForTest(&typesLinkTriggersCache)
	clearSyncMapForTest(&typeHRNFieldCache)
}

func clearSyncMapForTest(m *sync.Map) {
	m.Range(func(k, _ any) bool { m.Delete(k); return true })
}
