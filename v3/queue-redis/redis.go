package queueredis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

// ErrNilMessage is returned when attempting to enqueue a nil message.
var ErrNilMessage = errors.New("queue-redis: cannot enqueue nil message")

// ErrDeliveryLost is returned when a delivery token no longer owns its in-flight lease,
// either because the visibility timeout expired and the message was reclaimed by another
// worker, or because it was already settled.
var ErrDeliveryLost = queue.ErrDeliveryLost

type deliveryClaim struct {
	id    string
	lease float64
	owner string
}

func parseDeliveryClaim(tok any) (string, string, error) {
	var claim deliveryClaim
	switch t := tok.(type) {
	case deliveryClaim:
		claim = t
	case *deliveryClaim:
		if t == nil {
			return "", "", fmt.Errorf("queue-redis: nil delivery claim")
		}
		claim = *t
	default:
		return "", "", fmt.Errorf("queue-redis: invalid delivery token type: %T", tok)
	}
	if claim.id == "" || claim.owner == "" || claim.lease <= 0 || math.IsNaN(claim.lease) || math.IsInf(claim.lease, 0) {
		return "", "", fmt.Errorf("queue-redis: invalid delivery claim")
	}
	return claim.id, claim.owner, nil
}

// DefaultPrefix is the default key prefix for Redis queue data structures.
const DefaultPrefix = "gosuite:queue:"

// DefaultVisibilityTimeout is the default lease held on in-flight messages.
// A delivered message stays in-flight until Ack/Nack; if the consumer crashes
// and the lease expires, the next Dequeue redelivers it (at-least-once).
const DefaultVisibilityTimeout = 30 * time.Second

// Wire format (split transport): the message hash stores the user payload as
// raw bytes under "<id>:p" and the adapter metadata msgpack-encoded under
// "<id>:m". The payload never passes through any text encoding, so no base64
// tax applies. This is the only wire format: a record that is missing or
// cannot be decoded surfaces as a poison delivery instead of being guessed.
const (
	payloadFieldSuffix = ":p"
	metaFieldSuffix    = ":m"
)

func payloadField(id string) string { return id + payloadFieldSuffix }

func metaField(id string) string { return id + metaFieldSuffix }

// wireMeta is the v2 adapter-side metadata envelope. It is msgpack-encoded so
// metadata values stay binary-safe: header values (e.g. a poison message's
// raw_payload) round-trip losslessly, unlike the v1 JSON envelope which
// replaces invalid UTF-8.
type wireMeta struct {
	ID          string
	Topic       string
	Priority    queue.Priority
	GroupID     string
	GroupOrder  int
	Headers     map[string]string
	CreatedAt   time.Time
	Attempt     int
	MaxAttempts int
	Driver      string
}

func metaFromMessage(msg *queue.Message) wireMeta {
	return wireMeta{
		ID:          msg.ID,
		Topic:       msg.Topic,
		Priority:    msg.Priority,
		GroupID:     msg.GroupID,
		GroupOrder:  msg.GroupOrder,
		Headers:     msg.Headers,
		CreatedAt:   msg.CreatedAt,
		Attempt:     msg.Attempt,
		MaxAttempts: msg.MaxAttempts,
		Driver:      msg.Driver,
	}
}

func (m wireMeta) toMessage(payload []byte) *queue.Message {
	return &queue.Message{
		ID:          m.ID,
		Topic:       m.Topic,
		Payload:     payload,
		Priority:    m.Priority,
		GroupID:     m.GroupID,
		GroupOrder:  m.GroupOrder,
		Headers:     m.Headers,
		CreatedAt:   m.CreatedAt,
		Attempt:     m.Attempt,
		MaxAttempts: m.MaxAttempts,
		Driver:      m.Driver,
	}
}

// encodeMeta serializes the v2 metadata; the payload itself is not touched
// (it is stored raw under its own hash field, zero-copy).
func encodeMeta(msg *queue.Message) ([]byte, error) {
	return msgpack.Marshal(metaFromMessage(msg))
}

func decodeMeta(b []byte) (wireMeta, error) {
	var m wireMeta
	if err := msgpack.Unmarshal(b, &m); err != nil {
		return wireMeta{}, err
	}
	return m, nil
}

// RedisDriver implements queue.Adapter (and queue.DelayedEnqueuer) using Redis Sorted Sets and Hashes.
type RedisDriver struct {
	client redis.Cmdable
	prefix string
	lease  time.Duration
}

// RedisAdapter is an alias for RedisDriver under the Adapter architecture.
type RedisAdapter = RedisDriver

// Option configures RedisDriver parameters.
type Option func(*RedisDriver)

// WithPrefix sets the Redis key prefix for the queue.
func WithPrefix(prefix string) Option {
	return func(r *RedisDriver) {
		if prefix != "" {
			r.prefix = prefix
		}
	}
}

// WithVisibilityTimeout sets how long a delivered-but-unacked message stays
// in-flight before it becomes eligible for redelivery after a consumer crash.
func WithVisibilityTimeout(d time.Duration) Option {
	return func(r *RedisDriver) {
		if d > 0 {
			r.lease = d
		}
	}
}

// New constructs a RedisDriver with options.
func New(client redis.Cmdable, opts ...Option) *RedisDriver {
	return NewRedisDriver(client, DefaultPrefix, opts...)
}

// NewRedisDriver constructs a new Redis queue driver with a specified key prefix and options.
func NewRedisDriver(client redis.Cmdable, prefix string, opts ...Option) *RedisDriver {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	r := &RedisDriver{
		client: client,
		prefix: prefix,
		lease:  DefaultVisibilityTimeout,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewRedisAdapter constructs the official Redis Adapter for the Queue Core.
func NewRedisAdapter(client redis.Cmdable, prefix string, opts ...Option) *RedisAdapter {
	return NewRedisDriver(client, prefix, opts...)
}

// Name returns the driver identifier.
func (r *RedisDriver) Name() string {
	return "redis"
}

// Capabilities returns the capability profile for the Redis adapter.
func (r *RedisDriver) Capabilities() queue.Capabilities {
	return queue.Capabilities{
		Durable: false,
		// Persistent = broker redelivers unacknowledged messages on consumer
		// crash. The inflight lease provides exactly that (reclaimExpired), so
		// this must be true to stay consistent with Guarantee: AtLeastOnce.
		Persistent:        true,
		NativeRedelivery:  false,
		NativePriority:    queue.PrioritySoft,
		DelayedEnqueue:    true,
		StrictGlobalOrder: false,
		Replay:            false,
		Dedup:             true, // Effectively idempotent per message ID via last-write-wins (hash overwrite, single ZSet member); not a time window.
		MaxBatchSize:      500,
		Guarantee:         queue.AtLeastOnce,
	}
}

func (r *RedisDriver) queueKey() string {
	return r.prefix + "priority_zset"
}

func (r *RedisDriver) delayedKey() string {
	return r.prefix + "delayed_zset"
}

func (r *RedisDriver) dataKey() string {
	return r.prefix + "messages_hash"
}

// inflightKey tracks delivered-but-unacked members. The score is the lease
// expiry (unix seconds). Unlike the ready/delayed ZSets, entries here are not
// popped; Dequeue reclaims members whose lease expired so a crashed consumer's
// messages are redelivered (the at-least-once guarantee Redis lacks natively).
func (r *RedisDriver) inflightKey() string {
	return r.prefix + "inflight_zset"
}

// inflightScoreKey remembers each in-flight member's original ready-queue score
// so a reclaimed (crashed-consumer) message returns to the ready ZSet at its
// original position instead of losing its priority.
func (r *RedisDriver) inflightScoreKey() string {
	return r.prefix + "inflight_scores"
}

// inflightClaimKey stores an opaque acquisition identity independently of expiry.
// Each new acquisition overwrites the identity; settlement must match it exactly.
func (r *RedisDriver) inflightClaimKey() string {
	return r.prefix + "inflight_claims"
}

// popToInflight atomically moves up to N highest-priority members from the ready
// ZSet into the in-flight ZSet stamped with the lease score. Doing the pop and
// lease-stamp in one script closes the window where a crash between the two
// would strand a message that is neither ready nor reclaimable. The member's
// original ready score is saved so reclaimExpired can restore its position.
var popToInflightScript = redis.NewScript(`
local items = redis.call('ZPOPMAX', KEYS[1], ARGV[1])
local out = {}
for i = 1, #items, 2 do
  local member = items[i]
  local score = items[i+1]
  redis.call('ZADD', KEYS[2], ARGV[2], member)
  redis.call('HSET', KEYS[3], member, score)
  redis.call('HSET', KEYS[4], member, ARGV[3] .. ':' .. member)
  out[#out+1] = member
end
return out
`)

// ackInflightScript atomically checks that the message is still in the in-flight ZSet
// and matches the acquisition identity exactly before deleting it from the hash
// and clearing the in-flight lease. If the lease expired and the message was reclaimed
// (or claimed by another worker), it aborts and returns 0 without touching the message hash.
var ackInflightScript = redis.NewScript(`
local currentLease = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not currentLease then
  return 0
end
local owner = redis.call('HGET', KEYS[4], ARGV[1])
if ARGV[2] == '' or not owner or owner ~= ARGV[2] then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1]..':p', ARGV[1]..':m')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
return 1
`)

// nackInflightScript atomically restores a message to the ready ZSet only if
// the delivery still owns its in-flight lease. If the lease expired and was reclaimed
// by another worker, it aborts and returns 0 without stealing the new worker's lease.
var nackInflightScript = redis.NewScript(`
local currentLease = redis.call('ZSCORE', KEYS[2], ARGV[2])
if not currentLease then
  return 0
end
local owner = redis.call('HGET', KEYS[4], ARGV[2])
if ARGV[3] == '' or not owner or owner ~= ARGV[3] then
  return 0
end
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HDEL', KEYS[3], ARGV[2])
redis.call('HDEL', KEYS[4], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// reenqueueInflightScript atomically reschedules a message to the delayed ZSet
// only if the delivery still owns its in-flight lease.
var reenqueueInflightScript = redis.NewScript(`
local currentLease = redis.call('ZSCORE', KEYS[2], ARGV[2])
if not currentLease then
  return 0
end
local owner = redis.call('HGET', KEYS[5], ARGV[2])
if ARGV[3] == '' or not owner or owner ~= ARGV[3] then
  return 0
end
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HDEL', KEYS[3], ARGV[2])
redis.call('HDEL', KEYS[5], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[4], ARGV[2]..':p', ARGV[4], ARGV[2]..':m', ARGV[5])
return 1
`)

// migrateDelayedScript moves only records still due with the metadata observed
// when computing their ready score. A concurrent same-ID replacement may have
// changed the due time or priority; leave it for a fresh poll in that case.
// The conditional removal prevents overlapping migrations from re-adding an
// already claimed or settled message.
var migrateDelayedScript = redis.NewScript(`
local out = {}
for i = 2, #ARGV, 3 do
  local member = ARGV[i]
  local score = ARGV[i+1]
  local due = redis.call('ZSCORE', KEYS[1], member)
  local meta = redis.call('HGET', KEYS[3], member..':m') or ''
  if due and tonumber(due) <= tonumber(ARGV[1]) and meta == ARGV[i+2] then
    redis.call('ZREM', KEYS[1], member)
    redis.call('ZADD', KEYS[2], score, member)
    out[#out+1] = member
  end
end
return out
`)

// reclaimExpiredScript atomically returns every expired in-flight member to the
// ready ZSet, restoring its original score when known. Because the ZRem and the
// ZAdd happen in one script, a consumer crash cannot lose a message between the
// two (the window the previous non-atomic implementation left open). The count
// of reclaimed members is returned.
var reclaimExpiredScript = redis.NewScript(`
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
local out = {}
for i = 1, #expired do
  local member = expired[i]
  local score = redis.call('HGET', KEYS[3], member)
  if not score then
    score = ARGV[2]
  end
  redis.call('ZADD', KEYS[1], score, member)
  redis.call('HDEL', KEYS[3], member)
  redis.call('HDEL', KEYS[4], member)
  redis.call('ZREM', KEYS[2], member)
  out[#out+1] = member
end
return out
`)

// reclaimExpired redelivers in-flight members whose lease elapsed. The whole
// reclaim is performed by a single Lua script, so the removal from the in-flight
// ZSet and the reinsertion into the ready ZSet are atomic: a crash can never
// drop a message that was removed from in-flight but not yet requeued.
func (r *RedisDriver) reclaimExpired(ctx context.Context, nowScore float64) error {
	defaultScore := float64(queue.PriorityDefault)*1e9 - nowScore
	err := reclaimExpiredScript.Run(ctx, r.client,
		[]string{r.queueKey(), r.inflightKey(), r.inflightScoreKey(), r.inflightClaimKey()},
		fmt.Sprintf("%f", nowScore), defaultScore,
	).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}

// replacePublicationScript implements last-write-wins for one logical ID.
// Invalidate its old claim and remove the opposite schedule atomically with
// storing the replacement, so old settlement cannot delete newer payloads.
var replacePublicationScript = redis.NewScript(`
redis.call('HSET', KEYS[3], ARGV[1]..':p', ARGV[3], ARGV[1]..':m', ARGV[4])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`)

// enqueue atomically replaces a publication and invalidates any old claim.
func (r *RedisDriver) enqueue(ctx context.Context, msg *queue.Message, key string, score float64) error {
	if msg == nil {
		return ErrNilMessage
	}
	meta, err := encodeMeta(msg)
	if err != nil {
		return fmt.Errorf("queue-redis: failed to encode metadata: %w", err)
	}
	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}

	opposite := r.queueKey()
	if key == r.queueKey() {
		opposite = r.delayedKey()
	}
	_, err = replacePublicationScript.Run(ctx, r.client,
		[]string{key, opposite, r.dataKey(), r.inflightKey(), r.inflightScoreKey(), r.inflightClaimKey()},
		msg.ID, score, payload, meta).Result()
	return err
}

// Enqueue serializes and pushes a message into the Redis priority ZSet and message hash.
func (r *RedisDriver) Enqueue(ctx context.Context, msg *queue.Message) error {
	if msg == nil {
		return ErrNilMessage
	}
	// Score is Priority (higher priority = higher score).
	// Sub-score tie-breaker: timestamp unix seconds deducted
	score := float64(msg.Priority)*1e9 - float64(msg.CreatedAt.Unix())
	return r.enqueue(ctx, msg, r.queueKey(), score)
}

// EnqueueAt schedules a message to become active at a specific future timestamp.
func (r *RedisDriver) EnqueueAt(ctx context.Context, msg *queue.Message, at time.Time) error {
	if msg == nil {
		return ErrNilMessage
	}
	// In delayed ZSet, score is unix timestamp with fractional seconds
	score := float64(at.UnixNano()) / 1e9
	return r.enqueue(ctx, msg, r.delayedKey(), score)
}

// Dequeue pops the highest-priority messages from Redis, migrating matured delayed messages first.
func (r *RedisDriver) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	now := time.Now()
	nowScore := float64(now.UnixNano()) / 1e9

	// 0. Reclaim in-flight messages whose lease expired (consumer crash recovery).
	if err := r.reclaimExpired(ctx, nowScore); err != nil {
		return nil, err
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	maxMigrate := int64(batchSize * 5)
	if maxMigrate > 500 {
		maxMigrate = 500
	}

	// 1. Migrate any matured delayed messages from delayedKey to queueKey.
	// Bounded with Count to prevent blocking Redis single-threaded event loop during spikes.
	delayedMembers, err := r.client.ZRangeByScore(ctx, r.delayedKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%f", nowScore),
		Count: maxMigrate,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if len(delayedMembers) > 0 {
		// Read each message's msgpack metadata to preserve its original
		// priority during migration.
		fields := make([]string, 0, len(delayedMembers))
		for _, mID := range delayedMembers {
			fields = append(fields, metaField(mID))
		}
		raws, err := r.client.HMGet(ctx, r.dataKey(), fields...).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		args := make([]interface{}, 1, 1+len(delayedMembers)*3)
		args[0] = fmt.Sprintf("%f", nowScore)
		for i, mID := range delayedMembers {
			score := float64(queue.PriorityDefault)*1e9 - nowScore
			var expectedMeta []byte
			if i < len(raws) {
				if metaRaw := rawValue(raws[i]); metaRaw != nil {
					expectedMeta = metaRaw
					if meta, err := decodeMeta(metaRaw); err == nil {
						score = float64(meta.Priority)*1e9 - float64(meta.CreatedAt.Unix())
					}
				}
			}
			if expectedMeta == nil {
				expectedMeta = []byte{}
			}
			args = append(args, mID, fmt.Sprintf("%f", score), expectedMeta)
		}
		// The migration is conditional on the ZREM (see migrateDelayedScript):
		// an overlapping migration that lost the race contributes nothing, so
		// a member can never be re-added to the ready set after it was
		// already delivered and Acked.
		if _, err := migrateDelayedScript.Run(ctx, r.client,
			[]string{r.delayedKey(), r.queueKey(), r.dataKey()}, args...).Result(); err != nil {
			return nil, err
		}
	}

	// 2. Atomically pop highest-priority members into the in-flight ZSet with a lease.
	leaseScore := float64(now.Add(r.lease).UnixNano()) / 1e9
	claimOwner := queue.NewUUID()
	res, err := popToInflightScript.Run(ctx, r.client,
		[]string{r.queueKey(), r.inflightKey(), r.inflightScoreKey(), r.inflightClaimKey()},
		batchSize, leaseScore, claimOwner).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	popped, _ := res.([]interface{})
	if len(popped) == 0 {
		return nil, nil
	}

	msgIDs := make([]string, 0, len(popped))
	for _, v := range popped {
		if id, ok := v.(string); ok {
			msgIDs = append(msgIDs, id)
		}
	}
	if len(msgIDs) == 0 {
		return nil, nil
	}

	// Fetch the split record for each popped ID: "<id>:p" (raw payload) and
	// "<id>:m" (msgpack metadata).
	fields := make([]string, 0, len(msgIDs)*2)
	for _, id := range msgIDs {
		fields = append(fields, payloadField(id), metaField(id))
	}
	rawItems, err := r.client.HMGet(ctx, r.dataKey(), fields...).Result()
	if err != nil {
		return nil, err
	}

	deliveries := make([]*queue.Delivery, 0, len(msgIDs))
	for i, msgID := range msgIDs {
		v2Payload := rawValue(rawItems[i*2])
		v2Meta := rawValue(rawItems[i*2+1])
		if v2Payload == nil || v2Meta == nil {
			// The split record is incomplete while the ZSet member was already
			// popped: the payload is unrecoverable. Surface a poison delivery
			// so the consumer routes the loss to the DLQ instead of silently
			// dropping it.
			deliveries = append(deliveries, &queue.Delivery{
				Message: queue.NewPoisonMessage(msgID, nil, fmt.Errorf("queue-redis: missing payload for message id %q", msgID), r.Name()),
				Token:   deliveryClaim{id: msgID, lease: leaseScore, owner: claimOwner + ":" + msgID},
			})
			continue
		}
		meta, err := decodeMeta(v2Meta)
		if err != nil {
			deliveries = append(deliveries, &queue.Delivery{
				Message: queue.NewPoisonMessage(msgID, v2Payload, fmt.Errorf("queue-redis: corrupt metadata: %w", err), r.Name()),
				Token:   deliveryClaim{id: msgID, lease: leaseScore, owner: claimOwner + ":" + msgID},
			})
			continue
		}
		msg := meta.toMessage(v2Payload)
		deliveries = append(deliveries, &queue.Delivery{
			Message: msg,
			Token:   deliveryClaim{id: msgID, lease: leaseScore, owner: claimOwner + ":" + msgID},
		})
	}

	return deliveries, nil
}

// Reenqueue reschedules a delivery by atomically upserting the message into the delayed set
// only if the delivery still owns its in-flight lease. If the lease expired and was claimed
// by another worker, it aborts and returns ErrDeliveryLost without modifying delayed or hash sets.
func (r *RedisDriver) Reenqueue(ctx context.Context, d *queue.Delivery, msg *queue.Message, at time.Time) error {
	if d == nil || d.Token == nil {
		return nil
	}
	if msg == nil {
		return ErrNilMessage
	}
	id, owner, err := parseDeliveryClaim(d.Token)
	if err != nil {
		return err
	}
	meta, err := encodeMeta(msg)
	if err != nil {
		return fmt.Errorf("queue-redis: failed to encode metadata: %w", err)
	}
	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}
	delayedScore := float64(at.UnixNano()) / 1e9
	res, err := reenqueueInflightScript.Run(ctx, r.client,
		[]string{r.delayedKey(), r.inflightKey(), r.inflightScoreKey(), r.dataKey(), r.inflightClaimKey()},
		delayedScore, id, owner, payload, meta).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrDeliveryLost
	}
	return nil
}

// Ack removes the processed message payload from the Redis hash and clears its
// in-flight lease only while the caller still owns the lease. If the lease expired
// or was claimed by another worker, ErrDeliveryLost is returned and the message hash is untouched.
func (r *RedisDriver) Ack(ctx context.Context, d *queue.Delivery) error {
	if d == nil || d.Token == nil {
		return nil
	}
	id, owner, err := parseDeliveryClaim(d.Token)
	if err != nil {
		return err
	}
	res, err := ackInflightScript.Run(ctx, r.client,
		[]string{r.dataKey(), r.inflightKey(), r.inflightScoreKey(), r.inflightClaimKey()},
		id, owner).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrDeliveryLost
	}
	return nil
}

// Nack restores the message back into the priority queue for retry and clears
// its in-flight lease only while the caller still owns the lease; otherwise ErrDeliveryLost is returned.
func (r *RedisDriver) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	if d == nil || d.Token == nil {
		return nil
	}
	id, owner, err := parseDeliveryClaim(d.Token)
	if err != nil {
		return err
	}
	prio := queue.PriorityDefault
	createdAt := time.Now()
	if d.Message != nil {
		prio = d.Message.Priority
		if !d.Message.CreatedAt.IsZero() {
			createdAt = d.Message.CreatedAt
		}
	}
	score := float64(prio)*1e9 - float64(createdAt.Unix())
	res, err := nackInflightScript.Run(ctx, r.client,
		[]string{r.queueKey(), r.inflightKey(), r.inflightScoreKey(), r.inflightClaimKey()},
		score, id, owner).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrDeliveryLost
	}
	return nil
}

// Close gracefully closes driver resources.
func (r *RedisDriver) Close() error {
	return nil
}

// rawValue normalizes a Redis bulk string (string or []byte) to []byte.
func rawValue(v interface{}) []byte {
	switch s := v.(type) {
	case string:
		return []byte(s)
	case []byte:
		return s
	default:
		return nil
	}
}
