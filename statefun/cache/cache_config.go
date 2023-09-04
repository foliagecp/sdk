

package cache

const (
	KV_STORE_PREFIX                 = "store"
	LRU_SIZE                        = 1000000
	LEVEL_SUBSCRIPTION_CHANNEL_SIZE = 64
)

type CacheConfig struct {
	kvStorePrefix                string
	lruSize                      int
	levelSubscriptionChannelSize int
}

func NewCacheConfig() *CacheConfig {
	return &CacheConfig{
		kvStorePrefix:                KV_STORE_PREFIX,
		lruSize:                      LRU_SIZE,
		levelSubscriptionChannelSize: LEVEL_SUBSCRIPTION_CHANNEL_SIZE,
	}
}

func (ro *CacheConfig) SetKVStorePrefix(kvStorePrefix string) *CacheConfig {
	ro.kvStorePrefix = kvStorePrefix
	return ro
}

func (ro *CacheConfig) SetLRUSize(lruSize int) *CacheConfig {
	ro.lruSize = lruSize
	return ro
}

func (ro *CacheConfig) SetLevelSubscriptionChannelSize(levelSubscriptionChannelSize int) *CacheConfig {
	ro.levelSubscriptionChannelSize = levelSubscriptionChannelSize
	return ro
}
