// Copyright 2023 NJWS Inc.

package cache

const (
	KVStorePrefix                = "store"
	LRUSize                      = 1000000
	LevelSubscriptionChannelSize = 64
)

type Config struct {
	kvStorePrefix                string
	lruSize                      int
	levelSubscriptionChannelSize int
}

func NewCacheConfig() *Config {
	return &Config{
		kvStorePrefix:                KVStorePrefix,
		lruSize:                      LRUSize,
		levelSubscriptionChannelSize: LevelSubscriptionChannelSize,
	}
}

func (ro *Config) SetKVStorePrefix(kvStorePrefix string) *Config {
	ro.kvStorePrefix = kvStorePrefix
	return ro
}

func (ro *Config) SetLRUSize(lruSize int) *Config {
	ro.lruSize = lruSize
	return ro
}

func (ro *Config) SetLevelSubscriptionChannelSize(levelSubscriptionChannelSize int) *Config {
	ro.levelSubscriptionChannelSize = levelSubscriptionChannelSize
	return ro
}
