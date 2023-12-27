

package basic

import "github.com/foliagecp/sdk/statefun/cache"

func CacheTest(cacheStore *cache.Store) {
	cacheStore.Set("a.b.c.d0", []byte{1, 2, 3})
	cacheStore.Set("a.b.c.d1", []byte{1, 2, 3, 4})
	cacheStore.Set("k1.k2", []byte{1, 2, 3, 4, 5})
	cacheStore.Set("k1.k2", []byte{1, 2, 3, 4, 6})
	cacheStore.Set("k1.k2.k3", []byte{1, 2, 3, 4, 5, 6})
	cacheStore.Set("k1.k2.k3.k4", []byte{1, 2, 3, 4, 5, 6, 7})
	cacheStore.Set("k1.k2.k3.k4", []byte{1, 2, 3, 4, 5, 6, 8})
	cacheStore.Set("k1", []byte{1, 2, 3, 4, 5, 6, 7, 8})

	cacheStore.Delete("a.b.c.d1")
	cacheStore.Delete("a.b.c.d0")
	cacheStore.Delete("k1.k2.k3.k4")
	cacheStore.Delete("k1.k2.k3")
	cacheStore.Delete("k1.k2")
	cacheStore.Delete("k1")
}
