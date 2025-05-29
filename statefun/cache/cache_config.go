package cache

const (
	KVStorePrefix                               = "store"
	LRUSize                                     = 1000000
	LevelSubscriptionNotificationsBufferMaxSize = 30000 // ~16Mb: elemenets := 16 * 1024 * 1024 / (64 + 512), where 512 - avg value size, 64 - avg key size
	LazyWriterValueProcessDelayMkS              = 500
	LazyWriterRepeatDelayMkS                    = 100000
)

type Config struct {
	id                                          string
	kvStorePrefix                               string
	lruSize                                     int
	levelSubscriptionNotificationsBufferMaxSize int
	lazyWriterValueProcessDelayMkS              int
	lazyWriterRepeatDelayMkS                    int
}

func NewCacheConfig(id string) *Config {
	return &Config{
		id:            id,
		kvStorePrefix: KVStorePrefix,
		lruSize:       LRUSize,
		levelSubscriptionNotificationsBufferMaxSize: LevelSubscriptionNotificationsBufferMaxSize,
		lazyWriterValueProcessDelayMkS:              LazyWriterValueProcessDelayMkS,
		lazyWriterRepeatDelayMkS:                    LazyWriterRepeatDelayMkS,
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

func (cc *Config) SetLazyWriterValueProcessDelayMkS(lazyWriterValueProcessDelayMkS int) *Config {
	cc.lazyWriterValueProcessDelayMkS = lazyWriterValueProcessDelayMkS
	return cc
}

func (cc *Config) SetLazyWriterRepeatDelayMkS(lazyWriterRepeatDelayMkS int) *Config {
	cc.lazyWriterRepeatDelayMkS = lazyWriterRepeatDelayMkS
	return cc
}
