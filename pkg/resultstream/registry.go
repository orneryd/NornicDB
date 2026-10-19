package resultstream

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const registryShardCount = 32

var (
	ErrInvalidQID   = errors.New("invalid result stream qid")
	ErrExpiredQID   = errors.New("result stream qid expired")
	ErrGoneQID      = errors.New("result stream qid no longer exists")
	ErrCapacity     = errors.New("result stream capacity exceeded")
	ErrDisabled     = errors.New("result stream continuation is disabled")
	ErrInvalidScope = errors.New("invalid result stream scope")
)

// Scope binds a stream to one authenticated owner and canonical database.
type Scope struct {
	Owner    string
	Database string
}

// Observer receives bounded cursor lifecycle and aggregate usage signals.
type Observer interface {
	CursorEvent(outcome string)
	CursorUsage(active, retainedBytes int64)
}

type observerHolder struct{ observer Observer }

// Config bounds process-local durable result streams.
type Config struct {
	Disabled                 bool
	TTL                      time.Duration
	MaxStreams               int64
	MaxStreamsPerOwner       int64
	MaxPageSize              int
	MaxRetainedBytes         int64
	MaxRetainedBytesPerOwner int64
}

type registryEntry struct {
	scopeHash     [32]byte
	ownerHash     [32]byte
	database      string
	expires       int64
	retainedBytes atomic.Int64
	stream        Stream
	released      atomic.Bool
}

type ownerUsage struct {
	streams int64
	bytes   int64
}

type registryShard struct {
	mu      sync.RWMutex
	entries map[[16]byte]*registryEntry
}

// Registry owns signed qids and sharded process-local stream entries.
type Registry struct {
	config      Config
	disabled    bool
	secret      [32]byte
	instance    [8]byte
	shards      [registryShardCount]registryShard
	count       atomic.Int64
	retained    atomic.Int64
	closed      atomic.Bool
	scopeMAC    sync.Pool
	scopeInput  sync.Pool
	admissionMu sync.Mutex
	owners      map[[32]byte]ownerUsage
	observer    atomic.Pointer[observerHolder]
	stop        chan struct{}
	workers     sync.WaitGroup
}

// SetObserver replaces the optional lifecycle observer.
func (r *Registry) SetObserver(observer Observer) {
	if observer == nil {
		r.observer.Store(nil)
		return
	}
	r.observer.Store(&observerHolder{observer: observer})
	observer.CursorUsage(r.count.Load(), r.retained.Load())
}

// NewRegistry constructs a registry with a random signing key and instance ID.
func NewRegistry(config Config) (*Registry, error) {
	if config.TTL < 0 || config.MaxStreams < 0 || config.MaxStreamsPerOwner < 0 || config.MaxPageSize < 0 ||
		config.MaxRetainedBytes < 0 || config.MaxRetainedBytesPerOwner < 0 {
		return nil, ErrCapacity
	}
	if config.TTL == 0 {
		config.TTL = 5 * time.Minute
	}
	if config.MaxStreams == 0 {
		config.MaxStreams = 1024
	}
	if config.MaxStreamsPerOwner == 0 {
		config.MaxStreamsPerOwner = 64
	}
	if config.MaxPageSize == 0 {
		config.MaxPageSize = DefaultMaxPageSize
	}
	if config.MaxRetainedBytes == 0 {
		config.MaxRetainedBytes = 1 << 30
	}
	if config.MaxRetainedBytesPerOwner == 0 {
		config.MaxRetainedBytesPerOwner = 256 << 20
	}
	registry := &Registry{
		config: config, disabled: config.Disabled, owners: make(map[[32]byte]ownerUsage),
		stop: make(chan struct{}),
	}
	if _, err := io.ReadFull(rand.Reader, registry.secret[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(rand.Reader, registry.instance[:]); err != nil {
		return nil, err
	}
	secret := registry.secret
	registry.scopeMAC.New = func() any { return hmac.New(sha256.New, secret[:]) }
	registry.scopeInput.New = func() any { return make([]byte, 0, 128) }
	for index := range registry.shards {
		registry.shards[index].entries = make(map[[16]byte]*registryEntry)
	}
	registry.workers.Add(1)
	go registry.reapExpired()
	return registry, nil
}

// Start returns the first page and publishes the stream only when more rows are
// available.
func (r *Registry) Start(ctx context.Context, scope Scope, stream Stream, n int) (*Page, error) {
	if err := r.validate(scope, n); err != nil {
		r.observeError(err)
		return nil, err
	}
	page, err := stream.Pull(ctx, 0, n)
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	if !page.HasMore {
		_ = stream.Close()
		return page, nil
	}
	retainedBytes := int64(0)
	if reporter, ok := stream.(RetainedBytesReporter); ok {
		retainedBytes = reporter.RetainedBytes()
		if retainedBytes < 0 {
			_ = stream.Close()
			return nil, ErrCapacity
		}
	}
	ownerHash := r.ownerDigest(scope.Owner)
	if !r.reserve(ownerHash, retainedBytes) {
		r.observe("capacity")
		_ = stream.Close()
		return nil, ErrCapacity
	}

	entry := &registryEntry{
		scopeHash: r.scopeDigest(scope),
		ownerHash: ownerHash,
		database:  scope.Database,
		expires:   time.Now().Add(r.config.TTL).Unix(),
		stream:    stream,
	}
	entry.retainedBytes.Store(retainedBytes)
	var streamID [16]byte
	inserted := false
	for attempt := 0; attempt < 4; attempt++ {
		if _, err := io.ReadFull(rand.Reader, streamID[:]); err != nil {
			break
		}
		shard := r.shard(streamID)
		shard.mu.Lock()
		if _, exists := shard.entries[streamID]; !exists && !r.closed.Load() {
			shard.entries[streamID] = entry
			inserted = true
		}
		shard.mu.Unlock()
		if inserted {
			break
		}
	}
	if !inserted {
		r.release(entry)
		_ = stream.Close()
		if r.closed.Load() {
			r.observe("release")
			return nil, ErrClosed
		}
		r.observe("capacity")
		return nil, ErrCapacity
	}
	if guarded, ok := stream.(RetainedBytesGrowthGuard); ok {
		guarded.SetRetainedBytesGrowthGuard(func() bool {
			reporter, reportsBytes := stream.(RetainedBytesReporter)
			if !reportsBytes {
				return true
			}
			retainedBytes := reporter.RetainedBytes()
			return retainedBytes == entry.retainedBytes.Load() || r.resize(entry, retainedBytes)
		})
	}
	r.observe("start")
	r.observeUsage()
	page.QID = encodeToken(r.secret, r.instance, streamID, page.Next, entry.expires)
	page.ExpiresAt = time.Unix(entry.expires, 0).UTC()
	return page, nil
}

// ResolveDatabase returns the database bound to a signed qid after validating
// its authenticated owner. A nonempty requested database must match exactly.
func (r *Registry) ResolveDatabase(owner, qid, requested string) (string, error) {
	if r.disabled {
		return "", ErrDisabled
	}
	if r.closed.Load() {
		return "", ErrClosed
	}
	if owner == "" {
		return "", ErrInvalidScope
	}
	token, err := decodeToken(r.secret, r.instance, qid)
	if err != nil {
		return "", err
	}
	if time.Now().Unix() >= token.expires {
		r.remove(token.streamID, nil)
		return "", ErrExpiredQID
	}
	entry := r.lookup(token.streamID)
	if entry == nil || entry.released.Load() {
		return "", ErrGoneQID
	}
	wantOwner := r.ownerDigest(owner)
	if entry.expires != token.expires || !hmac.Equal(entry.ownerHash[:], wantOwner[:]) {
		return "", ErrInvalidQID
	}
	if requested != "" && requested != entry.database {
		return "", ErrInvalidQID
	}
	return entry.database, nil
}

// Pull resolves a signed qid and invokes the stream without holding a registry
// shard lock.
func (r *Registry) Pull(ctx context.Context, scope Scope, qid string, n int) (*Page, error) {
	if r.disabled {
		r.observe("disabled")
		return nil, ErrDisabled
	}
	if err := r.validate(scope, n); err != nil {
		r.observeError(err)
		return nil, err
	}
	token, err := decodeToken(r.secret, r.instance, qid)
	if err != nil {
		r.observe("invalid")
		return nil, err
	}
	if time.Now().Unix() >= token.expires {
		r.remove(token.streamID, nil)
		r.observe("expiry")
		return nil, ErrExpiredQID
	}
	entry := r.lookup(token.streamID)
	wantScope := r.scopeDigest(scope)
	if entry == nil || entry.released.Load() {
		r.observe("gone")
		return nil, ErrGoneQID
	}
	if entry.expires != token.expires || !hmac.Equal(entry.scopeHash[:], wantScope[:]) {
		r.observe("invalid")
		return nil, ErrInvalidQID
	}
	page, err := entry.stream.Pull(ctx, token.position, n)
	if err != nil {
		if errors.Is(err, ErrCapacity) {
			r.remove(token.streamID, entry)
		}
		r.observeError(err)
		return nil, err
	}
	if reporter, ok := entry.stream.(RetainedBytesReporter); ok {
		retainedBytes := reporter.RetainedBytes()
		if retainedBytes < 0 || (retainedBytes != entry.retainedBytes.Load() && !r.resize(entry, retainedBytes)) {
			r.remove(token.streamID, entry)
			r.observe("capacity")
			return nil, ErrCapacity
		}
	}
	if page.HasMore {
		page.QID = encodeToken(r.secret, r.instance, token.streamID, page.Next, entry.expires)
	}
	page.ExpiresAt = time.Unix(entry.expires, 0).UTC()
	r.observe("pull")
	return page, nil
}

// Discard releases the complete stream addressed by any of its qids.
func (r *Registry) Discard(scope Scope, qid string) error {
	if r.disabled {
		r.observe("disabled")
		return ErrDisabled
	}
	if scope.Owner == "" || scope.Database == "" {
		return ErrInvalidScope
	}
	token, err := decodeToken(r.secret, r.instance, qid)
	if err != nil {
		r.observe("invalid")
		return err
	}
	if time.Now().Unix() >= token.expires {
		r.remove(token.streamID, nil)
		r.observe("expiry")
		return ErrExpiredQID
	}
	entry := r.lookup(token.streamID)
	wantScope := r.scopeDigest(scope)
	if entry == nil || entry.released.Load() {
		r.observe("gone")
		return ErrGoneQID
	}
	if entry.expires != token.expires || !hmac.Equal(entry.scopeHash[:], wantScope[:]) {
		r.observe("invalid")
		return ErrInvalidQID
	}
	r.remove(token.streamID, entry)
	r.observe("discard")
	return nil
}

func (r *Registry) observe(outcome string) {
	if holder := r.observer.Load(); holder != nil {
		holder.observer.CursorEvent(outcome)
	}
}

func (r *Registry) observeError(err error) {
	switch {
	case errors.Is(err, ErrCapacity):
		r.observe("capacity")
	case errors.Is(err, ErrDisabled):
		r.observe("disabled")
	case errors.Is(err, ErrGoneQID):
		r.observe("gone")
	case errors.Is(err, ErrExpiredQID):
		r.observe("expiry")
	case errors.Is(err, ErrInvalidQID), errors.Is(err, ErrInvalidScope), errors.Is(err, ErrInvalidPageSize), errors.Is(err, ErrInvalidPosition):
		r.observe("invalid")
	case errors.Is(err, ErrInvalidated):
		r.observe("release")
	}
}

func (r *Registry) observeUsage() {
	if holder := r.observer.Load(); holder != nil {
		holder.observer.CursorUsage(r.count.Load(), r.retained.Load())
	}
}

func (r *Registry) validate(scope Scope, n int) error {
	if r.disabled {
		return ErrDisabled
	}
	if r.closed.Load() {
		return ErrClosed
	}
	if scope.Owner == "" || scope.Database == "" {
		return ErrInvalidScope
	}
	if n > r.config.MaxPageSize {
		return r.pageSizeExceeded()
	}
	if n <= 0 {
		return ErrInvalidPageSize
	}
	return nil
}

func (r *Registry) lookup(id [16]byte) *registryEntry {
	shard := r.shard(id)
	shard.mu.RLock()
	entry := shard.entries[id]
	shard.mu.RUnlock()
	return entry
}

func (r *Registry) remove(id [16]byte, expected *registryEntry) {
	shard := r.shard(id)
	shard.mu.Lock()
	entry := shard.entries[id]
	if entry != nil && (expected == nil || expected == entry) {
		delete(shard.entries, id)
		entry.released.Store(true)
	}
	shard.mu.Unlock()
	if entry != nil && (expected == nil || expected == entry) {
		r.release(entry)
		_ = entry.stream.Close()
	}
}

func (r *Registry) reserve(owner [32]byte, bytes int64) bool {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()
	usage := r.owners[owner]
	if r.count.Load() >= r.config.MaxStreams || usage.streams >= r.config.MaxStreamsPerOwner ||
		r.retained.Load()+bytes > r.config.MaxRetainedBytes || usage.bytes+bytes > r.config.MaxRetainedBytesPerOwner {
		return false
	}
	usage.streams++
	usage.bytes += bytes
	r.owners[owner] = usage
	r.count.Add(1)
	r.retained.Add(bytes)
	return true
}

func (r *Registry) resize(entry *registryEntry, bytes int64) bool {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()
	if entry.released.Load() {
		return false
	}
	current := entry.retainedBytes.Load()
	delta := bytes - current
	if delta <= 0 {
		usage := r.owners[entry.ownerHash]
		usage.bytes += delta
		r.owners[entry.ownerHash] = usage
		entry.retainedBytes.Store(bytes)
		r.retained.Add(delta)
		return true
	}
	usage := r.owners[entry.ownerHash]
	if r.retained.Load()+delta > r.config.MaxRetainedBytes || usage.bytes+delta > r.config.MaxRetainedBytesPerOwner {
		return false
	}
	usage.bytes += delta
	r.owners[entry.ownerHash] = usage
	entry.retainedBytes.Store(bytes)
	r.retained.Add(delta)
	return true
}

func (r *Registry) release(entry *registryEntry) {
	r.admissionMu.Lock()
	retainedBytes := entry.retainedBytes.Load()
	usage := r.owners[entry.ownerHash]
	usage.streams--
	usage.bytes -= retainedBytes
	if usage.streams == 0 {
		delete(r.owners, entry.ownerHash)
	} else {
		r.owners[entry.ownerHash] = usage
	}
	r.count.Add(-1)
	r.retained.Add(-retainedBytes)
	r.admissionMu.Unlock()
	r.observeUsage()
}

func (r *Registry) shard(id [16]byte) *registryShard {
	return &r.shards[id[0]&byte(registryShardCount-1)]
}

func scopeDigest(secret [32]byte, scope Scope) [32]byte {
	digest := hmac.New(sha256.New, secret[:])
	_, _ = digest.Write([]byte(scope.Owner))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(scope.Database))
	var out [32]byte
	copy(out[:], digest.Sum(nil))
	return out
}

func (r *Registry) scopeDigest(scope Scope) [32]byte {
	digest := r.scopeMAC.Get().(hash.Hash)
	digest.Reset()
	input := r.scopeInput.Get().([]byte)[:0]
	input = append(input, scope.Owner...)
	input = append(input, 0)
	input = append(input, scope.Database...)
	_, _ = digest.Write(input)
	var out [32]byte
	digest.Sum(out[:0])
	clear(input)
	r.scopeInput.Put(input[:0])
	r.scopeMAC.Put(digest)
	return out
}

func (r *Registry) ownerDigest(owner string) [32]byte {
	return scopeDigest(r.secret, Scope{Owner: owner})
}

func (r *Registry) reapExpired() {
	defer r.workers.Done()
	interval := r.config.TTL / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.sweepExpired(time.Now().Unix())
		case <-r.stop:
			return
		}
	}
}

func (r *Registry) sweepExpired(now int64) {
	for index := range r.shards {
		shard := &r.shards[index]
		var expired []*registryEntry
		shard.mu.Lock()
		for id, entry := range shard.entries {
			if now >= entry.expires {
				delete(shard.entries, id)
				entry.released.Store(true)
				expired = append(expired, entry)
			}
		}
		shard.mu.Unlock()
		for _, entry := range expired {
			r.release(entry)
			_ = entry.stream.Close()
			r.observe("expiry")
		}
	}
}

// Close releases every retained stream and rejects future operations.
func (r *Registry) Close() {
	if !r.closed.CompareAndSwap(false, true) {
		return
	}
	close(r.stop)
	r.workers.Wait()
	for index := range r.shards {
		shard := &r.shards[index]
		shard.mu.Lock()
		entries := shard.entries
		shard.entries = make(map[[16]byte]*registryEntry)
		shard.mu.Unlock()
		for _, entry := range entries {
			entry.released.Store(true)
			r.release(entry)
			_ = entry.stream.Close()
		}
	}
	r.observe("release")
}
