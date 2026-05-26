package cache

const (
	KVStorePrefix                  = "store"
	LazyWriterValueProcessDelayMkS = 500
	LazyWriterRepeatDelayMkS       = 100000
	WalBatchMinOps                 = 100 // min ops to accumulate before publishing a WAL transaction
)

type Config struct {
	id                             string
	kvStorePrefix                  string
	lazyWriterValueProcessDelayMkS int
	lazyWriterRepeatDelayMkS       int
	walBatchMinOps                 int
}

func NewCacheConfig(id string) *Config {
	return &Config{
		id:                             id,
		kvStorePrefix:                  KVStorePrefix,
		lazyWriterValueProcessDelayMkS: LazyWriterValueProcessDelayMkS,
		lazyWriterRepeatDelayMkS:       LazyWriterRepeatDelayMkS,
		walBatchMinOps:                 WalBatchMinOps,
	}
}

func (cc *Config) GetId() string {
	return cc.id
}

func (cc *Config) SetKVStorePrefix(kvStorePrefix string) *Config {
	cc.kvStorePrefix = kvStorePrefix
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

func (cc *Config) SetWalBatchMinOps(walBatchMinOps int) *Config {
	cc.walBatchMinOps = walBatchMinOps
	return cc
}

func (cc *Config) GetKVStorePrefix() string {
	return cc.kvStorePrefix
}
