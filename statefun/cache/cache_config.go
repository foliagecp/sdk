

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

type ConfigOpt func(c *Config)

func WithStorePrefix(prefix string) ConfigOpt {
	return func(c *Config) {
		c.kvStorePrefix = prefix
	}
}

func WithLRUSize(size int) ConfigOpt {
	return func(c *Config) {
		c.lruSize = size
	}
}

func WithLevelSubscriptionBufferSize(size int) ConfigOpt {
	return func(c *Config) {
		c.levelSubscriptionNotificationsBufferMaxSize = size
	}
}

func NewCacheConfig(id string, opts ...ConfigOpt) *Config {
	c := &Config{
		id:            id,
		kvStorePrefix: KVStorePrefix,
		lruSize:       LRUSize,
		levelSubscriptionNotificationsBufferMaxSize: LevelSubscriptionNotificationsBufferMaxSize,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}
