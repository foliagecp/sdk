package main

import "github.com/foliagecp/sdk/statefun/cache"

func CacheTest(cacheStore *cache.Store) {
	cacheStore.SetValue("a.b.c.d0", []byte{1, 2, 3}, true, -1, "")
	cacheStore.SetValue("a.b.c.d1", []byte{1, 2, 3, 4}, true, -1, "")
	cacheStore.SetValue("k1.k2", []byte{1, 2, 3, 4, 5}, true, -1, "")
	cacheStore.SetValue("k1.k2", []byte{1, 2, 3, 4, 6}, true, -1, "")
	cacheStore.SetValue("k1.k2.k3", []byte{1, 2, 3, 4, 5, 6}, true, -1, "")
	cacheStore.SetValue("k1.k2.k3.k4", []byte{1, 2, 3, 4, 5, 6, 7}, true, -1, "")
	cacheStore.SetValue("k1.k2.k3.k4", []byte{1, 2, 3, 4, 5, 6, 8}, true, -1, "")
	cacheStore.SetValue("k1", []byte{1, 2, 3, 4, 5, 6, 7, 8}, true, -1, "")

	cacheStore.DeleteValue("a.b.c.d1", true, -1, "")
	cacheStore.DeleteValue("a.b.c.d0", true, -1, "")
	cacheStore.DeleteValue("k1.k2.k3.k4", true, -1, "")
	cacheStore.DeleteValue("k1.k2.k3", true, -1, "")
	cacheStore.DeleteValue("k1.k2", true, -1, "")
	cacheStore.DeleteValue("k1", true, -1, "")
}
