package system

type TokenBucket struct {
	Capacity int
	tokens   chan struct{}
}

func NewTokenBucket(cap int) *TokenBucket {
	tb := &TokenBucket{
		Capacity: cap,
		tokens:   make(chan struct{}, cap),
	}
	for i := 0; i < cap; i++ {
		tb.tokens <- struct{}{}
	}
	return tb
}

func (tb *TokenBucket) TryAcquire() bool {
	select {
	case <-tb.tokens:
		return true
	default:
		return false
	}
}

func (tb *TokenBucket) Release() {
	select {
	case tb.tokens <- struct{}{}:
	default:
	}
}

func (tb *TokenBucket) GetLoadPercentage() float64 {
	return 100.0 * (1.0 - float64(len(tb.tokens))/float64(cap(tb.tokens)))
}
