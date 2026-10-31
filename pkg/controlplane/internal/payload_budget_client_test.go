package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/stretchr/testify/require"

	"github.com/openai/tunnel-client/pkg/config"
	"github.com/openai/tunnel-client/pkg/payloadbudget"
	"github.com/openai/tunnel-client/pkg/tunnelctx"
	"github.com/openai/tunnel-client/pkg/types"
)

func newBudgetTestClient(t *testing.T, server *httptest.Server, maxPollFrameBytes int64, budget *payloadbudget.Budget) *TunnelServiceClient {
	t.Helper()
	client, err := NewTunnelServiceClient(context.Background(), &config.ControlPlaneConfig{
		BaseURL:           mustParseURL(t, server.URL),
		TunnelID:          types.TunnelID("cli-tunnel"),
		APIKey:            "test-api-key",
		PollTimeout:       time.Second,
		MaxPollFrameBytes: maxPollFrameBytes,
	}, nil, newDiscardLogger(), &config.LoggingConfig{}, testMeterProvider)
	require.NoError(t, err)
	if budget != nil {
		client.SetPayloadBudget(budget)
	}
	return client
}

func frameJSONRPCPayload(id int, padding int) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"tool","arguments":{"data":"%s"}}}`,
		id, strings.Repeat("a", padding))
}

func frameForCommands(payloads ...string) []byte {
	commands := make([]string, 0, len(payloads))
	for i, payload := range payloads {
		commands = append(commands, fmt.Sprintf(
			`{"command_type":"jsonrpc","request_id":"req-%d","shard_token":"shard-123","created_at":"2025-10-29T23:08:09Z","jsonrpc":%s}`,
			i, payload))
	}
	return []byte(fmt.Sprintf(`{"commands":[%s]}`, strings.Join(commands, ",")))
}

func TestPollRejectsOversizedFrameByContentLength(t *testing.T) {
	t.Parallel()

	frame := frameForCommands(frameJSONRPCPayload(0, 4096))
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Force a declared Content-Length so the pre-read check fires.
		w.Header().Set("Content-Length", strconv.Itoa(len(frame)))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(frame)
	}))
	client := newBudgetTestClient(t, server, int64(len(frame)-1), nil)

	commands, _, err := client.Poll(context.Background(), 5)
	if !errors.Is(err, payloadbudget.ErrFrameTooLarge) {
		t.Fatalf("Poll error = %v, want ErrFrameTooLarge", err)
	}
	if commands != nil {
		t.Fatalf("oversized frame must be discarded atomically, got %d commands", len(commands))
	}
}

func TestPollRejectsOversizedChunkedFrameWhileReading(t *testing.T) {
	t.Parallel()

	frame := frameForCommands(frameJSONRPCPayload(0, 512))
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		require.True(t, ok, "test server must support flushing")
		// Flush the first byte so transfer is chunked (no Content-Length).
		_, _ = w.Write(frame[:1])
		flusher.Flush()
		_, _ = w.Write(frame[1:])
	}))
	client := newBudgetTestClient(t, server, int64(len(frame)-1), nil)

	commands, _, err := client.Poll(context.Background(), 5)
	if !errors.Is(err, payloadbudget.ErrFrameTooLarge) {
		t.Fatalf("Poll error = %v, want ErrFrameTooLarge", err)
	}
	if commands != nil {
		t.Fatalf("oversized chunked frame must be discarded, got %d commands", len(commands))
	}
}

func TestPollFrameBoundaryAtomicAndRecovers(t *testing.T) {
	t.Parallel()

	twoCommands := frameForCommands(frameJSONRPCPayload(0, 128), frameJSONRPCPayload(1, 128))
	validFrame := frameForCommands(frameJSONRPCPayload(2, 16))
	var pollCalls atomic.Int32
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if pollCalls.Add(1) == 1 {
			// One byte under the two-command frame: it must be dropped whole,
			// never partially enqueued.
			_, _ = w.Write(twoCommands)
			return
		}
		_, _ = w.Write(validFrame)
	}))
	client := newBudgetTestClient(t, server, int64(len(twoCommands)-1), nil)

	commands, _, err := client.Poll(context.Background(), 5)
	if !errors.Is(err, payloadbudget.ErrFrameTooLarge) {
		t.Fatalf("first Poll error = %v, want ErrFrameTooLarge", err)
	}
	if len(commands) != 0 {
		t.Fatalf("atomicity violated: %d commands admitted from oversized frame", len(commands))
	}

	// A subsequent valid frame decodes normally: frame boundaries are intact.
	commands, _, err = client.Poll(context.Background(), 5)
	if err != nil {
		t.Fatalf("second Poll failed: %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("expected 1 command after recovery, got %d", len(commands))
	}
}

func TestPollAcceptsFrameAtExactByteLimit(t *testing.T) {
	t.Parallel()

	frame := frameForCommands(frameJSONRPCPayload(0, 32))
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(frame)
	}))
	client := newBudgetTestClient(t, server, int64(len(frame)), nil)

	commands, _, err := client.Poll(context.Background(), 5)
	if err != nil {
		t.Fatalf("Poll at exact limit failed: %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}
}

func budgetTunnelResponse(t *testing.T, resultJSON string) *types.TunnelResponse {
	t.Helper()
	id, err := jsonrpc.MakeID("1")
	require.NoError(t, err)
	raw, err := jsonrpc.EncodeMessage(&jsonrpc.Response{ID: id, Result: json.RawMessage(resultJSON)})
	require.NoError(t, err)
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	return types.NewTunnelResponse(types.DefaultChannel, raw, http.StatusOK, headers)
}

func postContext() context.Context {
	ctx := tunnelctx.ContextWithShardToken(context.Background(), "shard-budget")
	return tunnelctx.ContextWithChannel(ctx, types.DefaultChannel)
}

func TestPostResponseDeliveryWaitsForBudgetThenCompletes(t *testing.T) {
	t.Parallel()

	var delivered atomic.Bool
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	budget := payloadbudget.New(4096)
	client := newBudgetTestClient(t, server, 0, budget)

	// Pin the whole budget with unrelated in-flight traffic.
	held, err := budget.Acquire(context.Background(), 4096)
	require.NoError(t, err)

	resp := budgetTunnelResponse(t, `{"ok":true}`)
	postErr := make(chan error, 1)
	go func() {
		_, err := client.PostResponse(postContext(), types.RequestID("req-slow"), resp)
		postErr <- err
	}()

	// Slow delivery: the response must wait for quota and not reach the server.
	time.Sleep(150 * time.Millisecond)
	if delivered.Load() {
		t.Fatal("response delivered before payload budget was available")
	}

	held.Release()
	select {
	case err := <-postErr:
		if err != nil {
			t.Fatalf("PostResponse after budget recovery failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PostResponse did not complete after budget recovery")
	}
	if got := budget.InFlight(); got != 0 {
		t.Fatalf("in-flight after delivery = %d, want 0", got)
	}
}

func TestPostResponseOversizeDeliveryGoesSoloWithOwnRequest(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	budget := payloadbudget.New(1024)
	client := newBudgetTestClient(t, server, 0, budget)

	// The request reservation held by the processor while MCP executes.
	requestReservation, err := budget.Acquire(context.Background(), 100)
	require.NoError(t, err)
	ctx := payloadbudget.ContextWithReservation(postContext(), requestReservation)

	// A response far larger than the whole budget must not deadlock waiting
	// for its own request bytes, nor be rejected.
	bigResult := fmt.Sprintf(`{"data":"%s"}`, strings.Repeat("b", 4096))
	resp := budgetTunnelResponse(t, bigResult)
	done := make(chan error, 1)
	go func() {
		_, err := client.PostResponse(ctx, types.RequestID("req-big"), resp)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("oversize PostResponse failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversize PostResponse deadlocked against its own request reservation")
	}
	if hits.Load() != 1 {
		t.Fatalf("expected exactly one delivery, got %d", hits.Load())
	}
	// Delivery bytes are released; the request reservation is still held.
	if got := budget.InFlight(); got != 100 {
		t.Fatalf("in-flight after delivery = %d, want 100 (own request only)", got)
	}
	requestReservation.Release()
}

func TestPostResponseHoldsReservationAcrossRetriesAndReleasesOnce(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	var seenInFlight atomic.Int64
	budget := payloadbudget.New(64 * 1024)
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if got := budget.InFlight(); got > 0 {
			seenInFlight.Store(got)
		}
		if n < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	client := newBudgetTestClient(t, server, 0, budget)

	_, err := client.PostResponse(postContext(), types.RequestID("req-retry"),
		budgetTunnelResponse(t, `{"ok":true}`))
	if err != nil {
		t.Fatalf("PostResponse retries failed: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 HTTP attempts, got %d (retries resend the same serialized body, never replay the tool)", got)
	}
	if seenInFlight.Load() <= 0 {
		t.Fatal("delivery reservation was not held during the retry attempts")
	}
	if got := budget.InFlight(); got != 0 {
		t.Fatalf("in-flight after retries = %d, want 0", got)
	}
}

func TestPostResponseDeliveryWaitIsCancelable(t *testing.T) {
	t.Parallel()

	var delivered atomic.Bool
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	budget := payloadbudget.New(4096)
	client := newBudgetTestClient(t, server, 0, budget)

	held, err := budget.Acquire(context.Background(), 4096)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(postContext())
	resp := budgetTunnelResponse(t, `{"ok":true}`)
	postErr := make(chan error, 1)
	go func() {
		_, err := client.PostResponse(ctx, types.RequestID("req-cancel"), resp)
		postErr <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-postErr:
		if err == nil {
			t.Fatal("expected error from canceled delivery wait")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled delivery wait did not return")
	}
	if delivered.Load() {
		t.Fatal("response must not be delivered after cancellation")
	}
	if got := budget.InFlight(); got != 4096 {
		t.Fatalf("in-flight after cancel = %d, want 4096 (unrelated holder only)", got)
	}
	held.Release()
}
