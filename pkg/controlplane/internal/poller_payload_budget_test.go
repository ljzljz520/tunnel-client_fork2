package internal

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/openai/tunnel-client/pkg/controlplane"
	"github.com/openai/tunnel-client/pkg/payloadbudget"
)

// budgetStubCommand is a polled command that carries a measured payload and a
// budget reservation, mirroring the production jsonRpcCommand carrier.
type budgetStubCommand struct {
	stubCommand
	payloadBytes int64
	reservation  *payloadbudget.Reservation
	admissionErr error
}

func (c *budgetStubCommand) PayloadBytes() int64 { return c.payloadBytes }

func (c *budgetStubCommand) AttachPayloadReservation(r *payloadbudget.Reservation) {
	c.reservation = r
}

func (c *budgetStubCommand) PayloadReservation() *payloadbudget.Reservation { return c.reservation }

func (c *budgetStubCommand) ReleasePayloadReservation() {
	if c.reservation != nil {
		c.reservation.Release()
		c.reservation = nil
	}
}

func (c *budgetStubCommand) MarkPayloadAdmissionRejected(reason error) { c.admissionErr = reason }

func (c *budgetStubCommand) PayloadAdmissionError() error { return c.admissionErr }

func newBudgetPoller(t *testing.T, q queue, fetcher controlplane.Fetcher, budget *payloadbudget.Budget) Poller {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	meterProvider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })
	p, err := NewPoller(q, fetcher, logger, meterProvider.Meter("test"), time.Second, 0, 0, 0)
	if err != nil {
		t.Fatalf("new poller: %v", err)
	}
	p.(*poller).SetPayloadBudget(budget)
	return p
}

func receiveCommand(t *testing.T, ch chan controlplane.PolledCommand) controlplane.PolledCommand {
	t.Helper()
	select {
	case cmd := <-ch:
		return cmd
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for command enqueue")
		return nil
	}
}

func TestPollerThrottlesAdmissionAndRecovers(t *testing.T) {
	t.Parallel()

	ch := make(chan controlplane.PolledCommand, 4)
	budget := payloadbudget.New(64)
	cmdA := &budgetStubCommand{stubCommand: stubCommand{id: "A"}, payloadBytes: 64}
	cmdB := &budgetStubCommand{stubCommand: stubCommand{id: "B"}, payloadBytes: 64}
	fetcher := &recordingFetcher{t: t, data: []controlplane.PolledCommand{cmdA, cmdB}}
	poller := newBudgetPoller(t, &chanQueue{ch: ch}, fetcher, budget)

	ctx, cancel := context.WithCancel(context.Background())
	var wg waitGroupShim
	wg.start(func() { poller.Run(ctx) })
	defer func() {
		cancel()
		wg.wait(t)
	}()

	first := receiveCommand(t, ch).(*budgetStubCommand)
	if first.id != "A" {
		t.Fatalf("first enqueued command = %q, want A", first.id)
	}
	if first.reservation == nil {
		t.Fatal("command A enqueued without a budget reservation")
	}
	if got := budget.InFlight(); got != 64 {
		t.Fatalf("in-flight after A = %d, want 64", got)
	}

	// B cannot fit while A's bytes are held: it must not be enqueued.
	select {
	case cmd := <-ch:
		t.Fatalf("command %s enqueued before budget recovered", cmd.RequestID())
	case <-time.After(100 * time.Millisecond):
	}

	// Simulate processing finishing for A: its bytes return to the budget.
	first.ReleasePayloadReservation()

	second := receiveCommand(t, ch).(*budgetStubCommand)
	if second.id != "B" {
		t.Fatalf("second enqueued command = %q, want B", second.id)
	}
	if got := budget.InFlight(); got != 64 {
		t.Fatalf("in-flight after recovery = %d, want 64", got)
	}
	second.ReleasePayloadReservation()
	if got := budget.InFlight(); got != 0 {
		t.Fatalf("in-flight after release = %d, want 0", got)
	}
}

func TestPollerOversizeCommandEnqueuedAsRejectionWithoutReservation(t *testing.T) {
	t.Parallel()

	ch := make(chan controlplane.PolledCommand, 2)
	budget := payloadbudget.New(50)
	cmd := &budgetStubCommand{stubCommand: stubCommand{id: "huge"}, payloadBytes: 100}
	fetcher := &recordingFetcher{t: t, data: []controlplane.PolledCommand{cmd}}
	poller := newBudgetPoller(t, &chanQueue{ch: ch}, fetcher, budget)

	ctx, cancel := context.WithCancel(context.Background())
	var wg waitGroupShim
	wg.start(func() { poller.Run(ctx) })
	defer func() {
		cancel()
		wg.wait(t)
	}()

	enqueued := receiveCommand(t, ch).(*budgetStubCommand)
	if !errors.Is(enqueued.admissionErr, payloadbudget.ErrPayloadTooLarge) {
		t.Fatalf("admission error = %v, want ErrPayloadTooLarge", enqueued.admissionErr)
	}
	if enqueued.reservation != nil {
		t.Fatal("rejected command must not hold a reservation")
	}
	if got := budget.InFlight(); got != 0 {
		t.Fatalf("in-flight after rejection = %d, want 0", got)
	}
}

func TestPollerBudgetWaitCanceledOnShutdownDoesNotEnqueue(t *testing.T) {
	t.Parallel()

	ch := make(chan controlplane.PolledCommand, 1)
	budget := payloadbudget.New(50)
	// Exhaust the budget with unrelated traffic.
	held, err := budget.Acquire(context.Background(), 50)
	if err != nil {
		t.Fatalf("acquire held: %v", err)
	}
	defer held.Release()

	cmd := &budgetStubCommand{stubCommand: stubCommand{id: "waiting"}, payloadBytes: 10}
	fetcher := &recordingFetcher{t: t, data: []controlplane.PolledCommand{cmd}}
	poller := newBudgetPoller(t, &chanQueue{ch: ch}, fetcher, budget)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		poller.Run(ctx)
	}()

	// Wait until the poller has polled and is blocked awaiting budget.
	deadline := time.Now().Add(2 * time.Second)
	for {
		fetcher.mu.Lock()
		polled := len(fetcher.calls) > 0
		fetcher.mu.Unlock()
		if polled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("poller never polled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop after shutdown")
	}

	select {
	case <-ch:
		t.Fatal("command must not be enqueued when admission wait is canceled")
	default:
	}
	if cmd.reservation != nil {
		t.Fatal("command acquired a reservation despite shutdown")
	}
	if got := budget.InFlight(); got != 50 {
		t.Fatalf("in-flight after shutdown = %d, want 50 (only unrelated holder)", got)
	}
}

func TestPollerWithoutBudgetKeepsLegacyBehavior(t *testing.T) {
	t.Parallel()

	ch := make(chan controlplane.PolledCommand, 1)
	cmd := &budgetStubCommand{stubCommand: stubCommand{id: "1"}, payloadBytes: 1 << 30}
	fetcher := &recordingFetcher{t: t, data: []controlplane.PolledCommand{cmd}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	meterProvider := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })
	p, err := NewPoller(&chanQueue{ch: ch}, fetcher, logger, meterProvider.Meter("test"), time.Second, 0, 0, 0)
	if err != nil {
		t.Fatalf("new poller: %v", err)
	}
	// No SetPayloadBudget: budget disabled.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	receiveCommand(t, ch)
}

// waitGroupShim is a tiny helper to avoid sync.WaitGroup closure copies.
type waitGroupShim struct {
	ch chan struct{}
}

func (w *waitGroupShim) start(fn func()) {
	w.ch = make(chan struct{})
	go func() {
		defer close(w.ch)
		fn()
	}()
}

func (w *waitGroupShim) wait(t *testing.T) {
	t.Helper()
	select {
	case <-w.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("background goroutine did not stop")
	}
}
