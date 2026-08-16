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

	// failOpen decides what Allow returns when it has no token count it can trust.
	// true allows the request, false denies it.
	//
	// "No token count it can trust" is wider than "Redis is down". It covers three cases:
	//   1. the call to Redis failed (connection refused, socket died)
	//   2. Redis answered, but the response had the wrong shape or types
	//   3. Redis answered, but a value would not parse
	//
	// Case 1 is Redis failing. Cases 2 and 3 are a bug in this file -
	// the Lua script and this Go code disagreeing about the response. Redis did nothing wrong there.
	//
	// They share one setting because the outcome is the same in all three:
	// no token count, and Allow still has to return a decision.
	//
	// This does NOT affect a normal deny. When Redis answers correctly and the bucket is empty, Allow denies the request at the bottom of the function
	// and failOpen is never read.
	failOpen bool
}

func NewRedisLimiter(client *redis.Client, capacity, ratePerSec float64) *RedisLimiter {
	// TTL: how long an idle bucket survives in Redis.
	//
	// Why an idle bucket is safe to delete: a missing key starts full - see the
	// `tokens == nil` branch in the script. So once a bucket has had time to
	// refill to capacity, "stored state" and "no state" mean the same thing, and
	// deleting it loses nothing.
	//
	// Refilling from empty takes capacity/rate seconds. That is the earliest
	// point deletion is safe.
	//
	// Why doubled: Allow sends this as whole seconds via int(l.ttl.Seconds()),
	// which truncates - a 2.9s TTL goes out as 2. Truncation always shortens, so
	// the key can expire while the bucket is still partially drained. A drained
	// bucket that disappears comes back full, which hands the client free tokens.
	// Doubling absorbs the truncation loss.
	//
	// The two failure directions are not equal. Expiring too late wastes a little
	// memory. Expiring too early lets a client past the limit. So bias long.
	//
	// Why floored at one second: EXPIRE takes an integer, and int(0.4) is 0.
	// EXPIRE with 0 deletes the key immediately, so every request would build a
	// fresh full bucket and the limiter would allow everything. This is reachable
	// with ordinary settings - capacity 2 at rate 10/s gives 0.2s, doubled to
	// 0.4s, truncated to 0.
	ttl := max(time.Second, 2*time.Duration(capacity/ratePerSec*float64(time.Second)))

	return &RedisLimiter{
		client:   client,
		script:   redis.NewScript(tokenBucketScript),
		capacity: capacity,
		rate:     ratePerSec,
		ttl:      ttl,
		now:      time.Now,
		failOpen: true,
	}
}

func (l *RedisLimiter) onError(err error) (bool, time.Duration, error) {
	if l.failOpen {
		return true, 0, err
	}
	return false, 0, err
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
		return l.onError(err)
	}

	if len(res) != 3 {
		return l.onError(fmt.Errorf("expected 3 values from script, got %d", len(res)))
	}

	// if ok is false, allowed holds 0 - the zero value, not an actual denial of request from Redis
	allowed, ok := res[0].(int64)
	if !ok {
		return l.onError(fmt.Errorf("unexpected type for allowed: %T", res[0]))
	}

	// skipping res[1]: tokens because token count is not used for anything as of now

	waitStr, ok := res[2].(string)
	if !ok {
		return l.onError(fmt.Errorf("unexpected type for wait: %T", res[2]))
	}
	waitSec, err := strconv.ParseFloat(waitStr, 64)
	if err != nil {
		return l.onError(fmt.Errorf("parsing wait %q: %w", waitStr, err))
	}

	if allowed == 1 {
		return true, 0, nil
	}

	return false, time.Duration(waitSec * float64(time.Second)), nil
}
