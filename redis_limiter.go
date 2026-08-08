package gatekeep

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// lua script to run the whole token-bucket decision inside Redis as one atomic unit.
// Redis executes a script as a single command, so no other client can interleave between the read and write.
const tokenBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])

if tokens == nil then
	tokens = capacity
	ts = now
end

-- clamp at 0: clock skew between servers must never drain tokens.
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
local wait = 0

if tokens >= 1 then
	tokens = tokens - 1
	allowed = 1
else
	wait = (1 - tokens) / rate
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, ttl)

-- return float values as strings, because redis truncates numeric values to integers
return {allowed, tostring(tokens), tostring(wait)}
`

type RedisLimiter struct {
	client   *redis.Client
	script   *redis.Script
	capacity float64
	rate     float64
	ttl      time.Duration
	now      func() time.Time
}

func NewRedisLimiter(client *redis.Client, capacity, ratePerSec float64) *RedisLimiter {
	// TTL: how long an idle key survives in Redis.
	// A full bucket tells us nothing - deleting it is the same as keeping it,
	// because a missing key starts full anyway. So we can delete after the
	// bucket has had time to refill: capacity/rate seconds.
	// Doubled for margin, and floored at 1 second because Redis EXPIRE only
	// accepts whole seconds : a sub-second TTL rounds to 0, which deletes the
	// key immediately and would disable limiting entirely.
	ttl := max(time.Second, 2*time.Duration(capacity/ratePerSec*float64(time.Second)))

	return &RedisLimiter{
		client:   client,
		script:   redis.NewScript(tokenBucketScript),
		capacity: capacity,
		rate:     ratePerSec,
		ttl:      ttl,
		now:      time.Now,
	}
}

func (l *RedisLimiter) Allow(ctx context.Context, id string) (bool, time.Duration, error) {
	now := l.now()
	keys := []string{"gatekeep:" + id}

	res, err := l.script.Run(ctx, l.client, keys,
		l.capacity,
		l.rate,
		float64(now.UnixNano())/1e9,
		int(l.ttl.Seconds()),
	).Slice() // returns ([]any, error)

	if err != nil {
		return false, 0, err
	}

	if len(res) != 3 {
		return false, 0, fmt.Errorf("expected 3 values from script, got %d", len(res))
	}

	allowed, ok := res[0].(int64)
	if !ok {
		return false, 0, fmt.Errorf("unexpected type for allowed: %T", res[0])
	}

	// skipping res[1]: tokens because token count is not used for anything as of now

	waitStr, ok := res[2].(string)
	if !ok {
		return false, 0, fmt.Errorf("unexpected type for wait: %T", res[2])
	}
	waitSec, err := strconv.ParseFloat(waitStr, 64)
	if err != nil {
		return false, 0, fmt.Errorf("parsing wait %q: %w", waitStr, err)
	}

	if allowed == 1 {
		return true, 0, nil
	}

	return false, time.Duration(waitSec * float64(time.Second)), nil
}
