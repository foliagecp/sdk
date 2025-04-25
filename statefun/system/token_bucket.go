package system

type TokenBucket struct {
	capacity int
	tokens   chan struct{}
}

func NewTokenBucket(cap int) *TokenBucket {
	tb := &TokenBucket{
		capacity: cap,
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
