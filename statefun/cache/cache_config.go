

package cache

const (
	KVStorePrefix                               = "store"
	LRUSize                                     = 1000000
	LevelSubscriptionNotificationsBufferMaxSize = 30000 // ~16Mb: elemenets := 16 * 1024 * 1024 / (64 + 512), where 512 - avg value size, 64 - avg key size
)

type Config struct {
	id                                          string
	kvStorePrefix                               string
	lruSize                                     int
	levelSubscriptionNotificationsBufferMaxSize int
}

func NewCacheConfig(id string) *Config {
	return &Config{
		id:            id,
		kvStorePrefix: KVStorePrefix,
		lruSize:       LRUSize,
		levelSubscriptionNotificationsBufferMaxSize: LevelSubscriptionNotificationsBufferMaxSize,
	}
}

func (cc *Config) GetId() string {
	return cc.id
}

func (cc *Config) SetKVStorePrefix(kvStorePrefix string) *Config {
	cc.kvStorePrefix = kvStorePrefix
	return cc
}

func (cc *Config) SetLRUSize(lruSize int) *Config {
	cc.lruSize = lruSize
	return cc
}

func (cc *Config) SetLevelSubscriptionNotificationsBufferMaxSize(levelSubscriptionNotificationsBufferMaxSize int) *Config {
	cc.levelSubscriptionNotificationsBufferMaxSize = levelSubscriptionNotificationsBufferMaxSize
	return cc
}
