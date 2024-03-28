package traces

import (
	"fmt"
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	limiter *rate.Limiter
}

type Record struct {
	Size      int64
	Timestamp time.Time
}

func NewRateLimiterWithInterval(limitSize int64, interval time.Duration) *RateLimiter {
	limit := rate.Limit(1 / float64(interval.Seconds()) * float64(limitSize))
	if limitSize <= 0 {
		limit = rate.Inf
	}

	limiter := rate.NewLimiter(limit, int(limit))
	return &RateLimiter{
		limiter: limiter,
	}
}

func NewRateLimiter(limitSize int64) *RateLimiter {
	return NewRateLimiterWithInterval(limitSize, time.Second)
}

func (l *RateLimiter) Add(size int64, now time.Time) {
	if l.limiter.Limit() == rate.Inf {
		return
	}

	// 一次Add 超过 Burst 的情况, 拆成N个Burst来处理
	for ; (int)(size) > l.limiter.Burst(); size -= (int64)(l.limiter.Burst()) {
		l.reserveN(l.limiter.Burst(), now)
	}

	if size > 0 {
		l.reserveN((int)(size), now)
	}
}

func (l *RateLimiter) reserveN(size int, now time.Time) {
	r := l.limiter.ReserveN(now, int(size))
	if r.OK() {
		time.Sleep(r.Delay())
	} else {
		// size must less than burst
		panic(fmt.Sprintf("size[%d] out of burst[%d]", size, l.limiter.Burst()))
	}
}
