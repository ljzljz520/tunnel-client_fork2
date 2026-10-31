package payloadbudget

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDisabledBudgetIsUnlimited(t *testing.T) {
	t.Parallel()

	b := New(0)
	for range 100 {
		r, err := b.Acquire(context.Background(), 1<<30)
		if err != nil {
			t.Fatalf("acquire on disabled budget: %v", err)
		}
		defer r.Release()
	}
	if got := b.InFlight(); got != 0 {
		t.Fatalf("disabled budget in-flight = %d, want 0", got)
	}
	if got := b.Limit(); got != 0 {
		t.Fatalf("disabled budget limit = %d, want 0", got)
	}
}

func TestNilBudgetAndReservationAreSafe(t *testing.T) {
	t.Parallel()

	var b *Budget
	r, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatalf("nil budget acquire: %v", err)
	}
	r.Release()
	r.Release()

	var nilRes *Reservation
	nilRes.Release()
	if got := nilRes.Bytes(); got != 0 {
		t.Fatalf("nil reservation bytes = %d, want 0", got)
	}
}

func TestAcquireWithinLimitAndRelease(t *testing.T) {
	t.Parallel()

	b := New(100)
	r1, err := b.Acquire(context.Background(), 60)
	if err != nil {
		t.Fatalf("acquire 60: %v", err)
	}
	if got := b.InFlight(); got != 60 {
		t.Fatalf("in-flight = %d, want 60", got)
	}
	r2, err := b.Acquire(context.Background(), 40)
	if err != nil {
		t.Fatalf("acquire 40: %v", err)
	}
	if got := b.InFlight(); got != 100 {
		t.Fatalf("in-flight = %d, want 100", got)
	}

	r1.Release()
	r1.Release() // idempotent
	if got := b.InFlight(); got != 40 {
		t.Fatalf("in-flight after release = %d, want 40", got)
	}
	r2.Release()
	if got := b.InFlight(); got != 0 {
		t.Fatalf("in-flight after release = %d, want 0", got)
	}
}

func TestOversizeRequestIsRejectedWithoutReserving(t *testing.T) {
	t.Parallel()

	b := New(100)
	if _, err := b.Acquire(context.Background(), 101); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversize acquire error = %v, want ErrPayloadTooLarge", err)
	}
	if got := b.InFlight(); got != 0 {
		t.Fatalf("in-flight after rejection = %d, want 0", got)
	}

	// A small request must still be admissible after the rejection.
	r, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatalf("acquire after rejection: %v", err)
	}
	r.Release()
}

func TestAcquireBlocksThenRecoversInFIFOOrder(t *testing.T) {
	t.Parallel()

	b := New(100)
	enqueued := make(chan struct{}, 2)
	b.onWait = enqueued
	held, err := b.Acquire(context.Background(), 80)
	if err != nil {
		t.Fatalf("acquire held: %v", err)
	}

	started := make(chan *Reservation, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	acquireAsync := func(n int64) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := b.Acquire(context.Background(), n)
			if err != nil {
				errs <- err
				return
			}
			started <- r
		}()
	}
	acquireAsync(30) // waiter A: queued first
	<-enqueued       // wait until A is visibly enqueued
	acquireAsync(80) // waiter B: queued behind A by FIFO
	<-enqueued       // wait until B is visibly enqueued

	// Neither fits while 80 bytes are held.
	select {
	case r := <-started:
		t.Fatalf("acquire unexpectedly succeeded before release: %v", r)
	case err := <-errs:
		t.Fatalf("acquire unexpectedly failed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	held.Release()

	// FIFO: A (30) is granted first. B (80) still cannot fit behind A
	// (30+80 > 100), which makes the head-of-line grant deterministic.
	var first *Reservation
	select {
	case first = <-started:
	case err := <-errs:
		t.Fatalf("first waiter failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first waiter")
	}
	if first.Bytes() != 30 {
		t.Fatalf("first granted reservation = %d bytes, want FIFO head 30", first.Bytes())
	}
	if got := b.InFlight(); got != 30 {
		t.Fatalf("in-flight = %d, want 30", got)
	}
	select {
	case r := <-started:
		t.Fatalf("FIFO violation: B (80) granted behind A: %v", r)
	case err := <-errs:
		t.Fatalf("second waiter failed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	first.Release()
	select {
	case second := <-started:
		if second.Bytes() != 80 {
			t.Fatalf("second granted reservation = %d bytes, want 80", second.Bytes())
		}
		if got := b.InFlight(); got != 80 {
			t.Fatalf("in-flight = %d, want 80", got)
		}
		second.Release()
	case err := <-errs:
		t.Fatalf("second waiter failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second waiter")
	}
	wg.Wait()
}

func TestAcquireContextCancelReleasesWaiter(t *testing.T) {
	t.Parallel()

	b := New(100)
	held, err := b.Acquire(context.Background(), 100)
	if err != nil {
		t.Fatalf("acquire held: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		_, err := b.Acquire(ctx, 10)
		errs <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled waiter")
	}

	// A later small request must be admitted after the canceled waiter, which
	// proves cancellation removed the waiter from the FIFO queue.
	held.Release()
	r, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatalf("acquire after canceled waiter: %v", err)
	}
	r.Release()
}

func TestDeliveryAcquireExcludesOwnRequestReservation(t *testing.T) {
	t.Parallel()

	b := New(100)
	request, err := b.Acquire(context.Background(), 60)
	if err != nil {
		t.Fatalf("acquire request: %v", err)
	}

	// 60 (held) + 40 (delivery) == limit; without excluding its own bytes this
	// would block forever and time out the test.
	done := make(chan *Reservation, 1)
	go func() {
		r, err := b.AcquireDelivery(context.Background(), 40, request)
		if err != nil {
			t.Errorf("delivery acquire: %v", err)
			done <- nil
			return
		}
		done <- r
	}()
	select {
	case r := <-done:
		if r == nil {
			t.Fatal("delivery acquire failed")
		}
		if got := b.InFlight(); got != 100 {
			t.Fatalf("in-flight = %d, want 100", got)
		}
		r.Release()
	case <-time.After(time.Second):
		t.Fatal("delivery acquire blocked against the request's own reservation")
	}
	request.Release()
}

func TestOversizeDeliveryGoesSoloAndThenBudgetRecovers(t *testing.T) {
	t.Parallel()

	b := New(100)
	request, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatalf("acquire request: %v", err)
	}

	// Response larger than the whole limit; the tool already ran, so delivery
	// must not be rejected. The request reservation is this response's own
	// in-flight bytes, so it is excluded: only that 10-byte reservation may be
	// present when the oversized response goes solo.
	delivery, err := b.AcquireDelivery(context.Background(), 250, request)
	if err != nil {
		t.Fatalf("oversize delivery acquire: %v", err)
	}
	if got := b.InFlight(); got != 260 {
		t.Fatalf("in-flight with own request + solo delivery = %d, want 260", got)
	}

	// Releasing the own request leaves the oversized response pinned alone.
	request.Release()
	if got := b.InFlight(); got != 250 {
		t.Fatalf("in-flight during solo delivery = %d, want 250", got)
	}

	// While the oversized response is in flight, an unrelated small request
	// waits for the budget to drain.
	smallDone := make(chan *Reservation, 1)
	go func() {
		r, err := b.Acquire(context.Background(), 10)
		if err != nil {
			t.Errorf("small acquire: %v", err)
			smallDone <- nil
			return
		}
		smallDone <- r
	}()
	select {
	case r := <-smallDone:
		t.Fatalf("small acquire succeeded during solo delivery: %v", r)
	case <-time.After(50 * time.Millisecond):
	}

	// An unrelated reservation also blocks a second oversized delivery: solo
	// mode requires every other reservation to drain.
	blockedCtx, cancelBlocked := context.WithCancel(context.Background())
	blockedDone := make(chan error, 1)
	go func() {
		_, err := b.AcquireDelivery(blockedCtx, 200, nil)
		blockedDone <- err
	}()
	select {
	case err := <-blockedDone:
		t.Fatalf("second solo delivery admitted while one is in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancelBlocked()
	select {
	case err := <-blockedDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked delivery error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked delivery did not react to cancellation")
	}

	delivery.Release()
	select {
	case r := <-smallDone:
		if r == nil {
			t.Fatal("small acquire failed after solo delivery release")
		}
		if got := b.InFlight(); got != 10 {
			t.Fatalf("in-flight after recovery = %d, want 10", got)
		}
		r.Release()
	case <-time.After(time.Second):
		t.Fatal("small request did not complete after budget recovered")
	}
}

func TestDeliveryAcquireRejectsOversizeOnlyForRequestMode(t *testing.T) {
	t.Parallel()

	b := New(100)
	if _, err := b.Acquire(context.Background(), 101); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("request oversize error = %v, want ErrPayloadTooLarge", err)
	}

	// An empty budget admits an oversized delivery solo (the tool already ran,
	// so the response must be delivered rather than rejected).
	solo, err := b.AcquireDelivery(context.Background(), 101, nil)
	if err != nil {
		t.Fatalf("delivery oversize on empty budget: %v", err)
	}
	if got := b.InFlight(); got != 101 {
		t.Fatalf("in-flight during solo delivery = %d, want 101", got)
	}
	solo.Release()

	// With unrelated traffic in flight, the same delivery waits for exclusive
	// use instead of being rejected; the expired context surfaces the wait.
	other, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatalf("acquire unrelated reservation: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := b.AcquireDelivery(ctx, 101, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delivery oversize wait error = %v, want context.DeadlineExceeded", err)
	}
	if got := b.InFlight(); got != 10 {
		t.Fatalf("in-flight after timed-out delivery = %d, want 10", got)
	}
	other.Release()
}
