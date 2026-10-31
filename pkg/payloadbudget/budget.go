// Package payloadbudget enforces an optional, process-wide byte budget for
// payloads in flight across the tunnel link. A single Budget backs every link
// stage: poll decoding reserves request bytes before decoded commands are
// queued, the reservation travels with the command through queuing and MCP
// execution, and response delivery reserves the outgoing payload bytes until
// the final POST attempt finishes. Reservations are released exactly once as
// soon as their stage completes, so stalled delivery or client shutdown cannot
// pin quota forever.
package payloadbudget

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

const (
	stageRequest  = "request"
	stageDelivery = "delivery"
)

var (
	// ErrPayloadTooLarge reports that a request payload can never fit the
	// configured budget, even with no other traffic in flight. Admission must
	// be rejected before any downstream (tool) operation runs.
	ErrPayloadTooLarge = errors.New("payload exceeds tunnel link byte budget")

	// ErrFrameTooLarge reports that a single poll response frame exceeds the
	// configured hard frame cap. The entire frame is discarded atomically; no
	// partial batch may enter the dispatcher queue.
	ErrFrameTooLarge = errors.New("poll frame exceeds configured byte limit")
)

// Reservation records bytes held against a Budget. The zero value and nil are
// safe to release. Release is idempotent.
type Reservation struct {
	budget *Budget
	bytes  int64
	once   sync.Once
}

// Release returns the reserved bytes to the budget exactly once. It is safe to
// call on a nil receiver and may be deferred from every termination path.
func (r *Reservation) Release() {
	if r == nil || r.budget == nil || r.bytes == 0 {
		return
	}
	r.once.Do(func() {
		r.budget.release(r.bytes)
	})
}

// Bytes reports the reservation size.
func (r *Reservation) Bytes() int64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

type waiter struct {
	n       int64
	held    int64
	stage   string
	ready   chan struct{}
	granted bool
}

// Budget is a FIFO byte semaphore shared by all link stages. A zero or negative
// limit disables enforcement and every Acquire returns a no-op reservation.
type Budget struct {
	limit int64

	mu       sync.Mutex
	inFlight int64
	waiters  []*waiter

	// onWait, when set, is signaled after a waiter is enqueued. Tests use it to
	// observe FIFO ordering deterministically; production callers leave it nil.
	onWait chan<- struct{}

	metrics *budgetMetrics
}

// New builds a Budget. A non-positive limit disables budgeting. When a meter is
// supplied it records in-flight/limit gauges, an admission-rejection counter,
// and an acquisition wait histogram.
func New(limit int64, meters ...metric.Meter) *Budget {
	b := &Budget{limit: limit}
	if limit > 0 && len(meters) > 0 && meters[0] != nil {
		b.metrics = newBudgetMetrics(meters[0], b)
	}
	return b
}

// Limit reports the configured byte limit; zero means unlimited.
func (b *Budget) Limit() int64 {
	if b == nil {
		return 0
	}
	return b.limit
}

// InFlight reports bytes currently reserved.
func (b *Budget) InFlight() int64 {
	if b == nil || b.limit <= 0 {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight
}

// Acquire reserves request payload bytes before the request is admitted to the
// link (decoded and queued). It blocks in FIFO order until enough bytes are
// free or ctx ends. A payload larger than the total limit fails immediately
// with ErrPayloadTooLarge instead of waiting, because it could never fit.
func (b *Budget) Acquire(ctx context.Context, n int64) (*Reservation, error) {
	if b == nil || b.limit <= 0 || n <= 0 {
		return &Reservation{budget: b, bytes: n}, nil
	}
	if n > b.limit {
		b.countRejection(stageRequest)
		return nil, ErrPayloadTooLarge
	}
	w := &waiter{n: n, stage: stageRequest, ready: make(chan struct{})}
	if err := b.wait(ctx, w); err != nil {
		return nil, err
	}
	return &Reservation{budget: b, bytes: n}, nil
}

// AcquireDelivery reserves outgoing response payload bytes immediately before
// the (potentially large) response body is allocated and sent. The held
// reservation is the same request's in-flight reservation; its bytes are not
// counted against the new reservation, otherwise a request could deadlock
// against its own quota. A response larger than the total limit is not
// rejected: the tool has already executed by the time a response exists, so it
// waits for every other reservation to drain and then goes out solo, pinning
// the budget until delivery finishes. The reservation spans every HTTP retry
// attempt and is released after the terminal outcome.
func (b *Budget) AcquireDelivery(ctx context.Context, n int64, held *Reservation) (*Reservation, error) {
	if b == nil || b.limit <= 0 || n <= 0 {
		return &Reservation{budget: b, bytes: n}, nil
	}
	var heldBytes int64
	if held != nil && held.budget == b {
		heldBytes = held.bytes
	}
	w := &waiter{n: n, held: heldBytes, stage: stageDelivery, ready: make(chan struct{})}
	if err := b.wait(ctx, w); err != nil {
		return nil, err
	}
	return &Reservation{budget: b, bytes: n}, nil
}

// wait enqueues w in FIFO order and blocks until it is granted, fails, or ctx
// ends. FIFO ordering prevents a recovered budget from being starved by
// earlier waiters.
func (b *Budget) wait(ctx context.Context, w *waiter) error {
	start := time.Now()
	b.mu.Lock()
	if len(b.waiters) == 0 && b.fitsLocked(w) {
		b.inFlight += w.n
		w.granted = true
		b.mu.Unlock()
		return nil
	}
	b.waiters = append(b.waiters, w)
	onWait := b.onWait
	b.mu.Unlock()

	// Test hook: signal (non-blocking) only after the waiter is visibly
	// enqueued, so tests can establish deterministic FIFO preconditions.
	if onWait != nil {
		select {
		case onWait <- struct{}{}:
		default:
		}
	}

	select {
	case <-w.ready:
		b.observeWait(w.stage, time.Since(start))
		return nil
	case <-ctx.Done():
		b.mu.Lock()
		// A concurrent release may have granted this waiter just as its context
		// ended; honor the grant so the bytes are not leaked or double-booked.
		if !w.granted {
			b.removeWaiterLocked(w)
			b.pumpLocked()
			b.mu.Unlock()
			return ctx.Err()
		}
		b.mu.Unlock()
		b.observeWait(w.stage, time.Since(start))
		return nil
	}
}

// fitsLocked reports whether w can be granted against current occupancy. A
// delivery larger than the limit requires exclusive use of the budget; all
// other requests must fit without exceeding it.
func (b *Budget) fitsLocked(w *waiter) bool {
	effective := b.inFlight - w.held
	if effective < 0 {
		effective = 0
	}
	if w.n > b.limit {
		return effective == 0
	}
	return effective+w.n <= b.limit
}

func (b *Budget) removeWaiterLocked(target *waiter) {
	for i, w := range b.waiters {
		if w == target {
			b.waiters = append(b.waiters[:i], b.waiters[i+1:]...)
			return
		}
	}
}

// pumpLocked grants FIFO waiters while they fit, stopping at the first one that
// does not so strict ordering is preserved.
func (b *Budget) pumpLocked() {
	for len(b.waiters) > 0 {
		head := b.waiters[0]
		if !b.fitsLocked(head) {
			return
		}
		b.waiters = b.waiters[1:]
		b.inFlight += head.n
		head.granted = true
		close(head.ready)
	}
}

func (b *Budget) release(n int64) {
	b.mu.Lock()
	b.inFlight -= n
	if b.inFlight < 0 {
		b.inFlight = 0
	}
	b.pumpLocked()
	b.mu.Unlock()
}

func (b *Budget) countRejection(stage string) {
	if b.metrics != nil {
		b.metrics.rejections.Add(context.Background(), 1, metric.WithAttributes(stageAttribute(stage)))
	}
}

func (b *Budget) observeWait(stage string, d time.Duration) {
	if b.metrics != nil && d > 0 {
		b.metrics.acquireWait.Record(context.Background(), d.Seconds(), metric.WithAttributes(stageAttribute(stage)))
	}
}

type reservationContextKey struct{}

// ContextWithReservation attaches the in-flight request reservation to a
// context so response delivery can exclude the request's own bytes when
// reserving delivery quota.
func ContextWithReservation(ctx context.Context, r *Reservation) context.Context {
	if r == nil {
		return ctx
	}
	return context.WithValue(ctx, reservationContextKey{}, r)
}

// ReservationFromContext retrieves a reservation attached with
// ContextWithReservation.
func ReservationFromContext(ctx context.Context) (*Reservation, bool) {
	r, ok := ctx.Value(reservationContextKey{}).(*Reservation)
	return r, ok
}
