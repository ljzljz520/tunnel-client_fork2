package e2e_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openai/tunnel-client/pkg/config"
	"github.com/openai/tunnel-client/pkg/payloadbudget"
	harnesspkg "github.com/openai/tunnel-client/testsupport/e2e"
	"github.com/openai/tunnel-client/testsupport/mockmcpserver"
	"github.com/openai/tunnel-client/testsupport/mocktunnelservice"
)

// budgetToolCommand builds a tools/call tunnel command whose JSON-RPC payload
// carries padding bytes of arbitrary length.
func budgetToolCommand(tool, requestID, rpcID string, padding int) json.RawMessage {
	rpc := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":%q,"method":"tools/call","params":{"name":%q,"arguments":{"data":%q}}}`,
		rpcID, tool, strings.Repeat("a", padding))
	return mocktunnelservice.NewCommand(requestID, json.RawMessage(rpc), nil)
}

func budgetEchoCommand(requestID, rpcID string, padding int) json.RawMessage {
	return budgetToolCommand("echo", requestID, rpcID, padding)
}

func budgetEnvelopeBytes(t *testing.T, commands ...json.RawMessage) []byte {
	t.Helper()
	frame, err := json.Marshal(map[string]any{"commands": commands})
	require.NoError(t, err)
	return frame
}

// assertBudgetToolOK asserts a delivered tool response is HTTP 200 carrying a
// JSON-RPC result rather than an error envelope.
func assertBudgetToolOK(tb testing.TB, resp mocktunnelservice.ReceivedResponse) {
	tb.Helper()
	if resp.ResponseCode != http.StatusOK {
		tb.Fatalf("tool response code = %d, want 200", resp.ResponseCode)
	}
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(resp.JSONResponse, &envelope); err != nil {
		tb.Fatalf("decode tool response: %v", err)
	}
	if len(envelope.Error) != 0 {
		tb.Fatalf("tool response carried a JSON-RPC error: %s", string(envelope.Error))
	}
}

// TestPayloadBudgetOversizedFrameDroppedAtomically verifies the hard poll
// frame cap: an oversized frame carrying two tool commands is discarded as a
// whole (neither tool runs), frame boundaries stay intact, and a subsequent
// valid frame is processed normally.
func TestPayloadBudgetOversizedFrameDroppedAtomically(t *testing.T) {
	t.Parallel()

	const maxPollFrameBytes int64 = 2048

	var injected atomic.Bool
	oversizedFrame := budgetEnvelopeBytes(t,
		budgetEchoCommand("oversized-1", "big-1", 4096),
		budgetEchoCommand("oversized-2", "big-2", 16),
	)
	require.Greater(t, len(oversizedFrame), int(maxPollFrameBytes))

	smallCommand := mocktunnelservice.CommandResponse{
		Command: budgetEchoCommand("cmd-small", "small-1", 8),
		ExpectedResponses: []mocktunnelservice.ExpectedResponse{{
			RequestID: "cmd-small",
			Assert:    assertBudgetToolOK,
		}},
	}

	h := harnesspkg.NewHarness(
		t,
		harnesspkg.WithInMemoryMCPTransport(),
		harnesspkg.WithClientConfig(func(cfg *config.Config) {
			cfg.ControlPlane.MaxPollFrameBytes = maxPollFrameBytes
		}),
		harnesspkg.WithControlPlaneOptions(
			mocktunnelservice.WithInitializationPhaseCommandsWithoutSessionHeaders(),
			mocktunnelservice.WithPollHandler(func(w http.ResponseWriter, _ *http.Request) bool {
				if !injected.CompareAndSwap(false, true) {
					return false
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(oversizedFrame)
				return true
			}),
			mocktunnelservice.WithCommandResponses(smallCommand),
		),
		harnesspkg.WithMCPOptions(
			mockmcpserver.WithToolListChangedNotificationsDisabled(),
			mockmcpserver.WithCalls(mockmcpserver.Call{
				Tool:   "echo",
				Result: json.RawMessage(`{"ok":true}`),
			}),
		),
	)
	h.ExecuteScenarious(t)

	// Exactly one echo call: the two commands embedded in the oversized frame
	// must never have been partially admitted.
	requests := h.MCP.ReceivedRequests()
	if len(requests) != 1 {
		t.Fatalf("MCP tool calls = %d, want 1 (oversized frame must be dropped atomically)", len(requests))
	}
	matched := h.ControlPlane.ReceivedResponses(mocktunnelservice.ResponseMatchMatched)
	if len(matched) != 3 {
		t.Fatalf("matched responses = %d, want 3 (initialize, initialized, small tool)", len(matched))
	}
	delivered := h.ControlPlane.DeliveredCommands()
	if len(delivered) != 3 {
		t.Fatalf("delivered commands = %d, want 3 (frame rejection must not corrupt frame boundaries)", len(delivered))
	}
	if got := h.PayloadBudget.InFlight(); got != 0 {
		t.Fatalf("in-flight payload bytes = %d, want 0 (budget disabled)", got)
	}
}

// TestPayloadBudgetOversizedRequestAnswers413WithoutToolExecution verifies the
// in-flight byte budget: a command larger than the budget is rejected at
// admission and answered with HTTP 413 before any MCP connection, so the tool
// never executes; a following small command still completes once quota is free.
func TestPayloadBudgetOversizedRequestAnswers413WithoutToolExecution(t *testing.T) {
	t.Parallel()

	const maxInFlightPayloadBytes int64 = 2048

	bigCommand := mocktunnelservice.CommandResponse{
		Command: budgetEchoCommand("cmd-oversized", "oversized-rpc", 4096),
		ExpectedResponses: []mocktunnelservice.ExpectedResponse{{
			RequestID: "cmd-oversized",
			Assert: func(tb testing.TB, resp mocktunnelservice.ReceivedResponse) {
				if resp.ResponseCode != http.StatusRequestEntityTooLarge {
					tb.Fatalf("oversized command response code = %d, want 413", resp.ResponseCode)
				}
				var envelope struct {
					Error json.RawMessage `json:"error"`
				}
				if err := json.Unmarshal(resp.JSONResponse, &envelope); err != nil {
					tb.Fatalf("decode 413 response: %v", err)
				}
				if len(envelope.Error) == 0 {
					tb.Fatal("413 response must carry a JSON-RPC error payload")
				}
			},
		}},
	}
	smallCommand := mocktunnelservice.CommandResponse{
		Command: budgetEchoCommand("cmd-small", "small-rpc", 8),
		ExpectedResponses: []mocktunnelservice.ExpectedResponse{{
			RequestID: "cmd-small",
			Assert: func(tb testing.TB, resp mocktunnelservice.ReceivedResponse) {
				if resp.ResponseCode != http.StatusOK {
					tb.Fatalf("small command response code = %d, want 200", resp.ResponseCode)
				}
			},
		}},
	}

	h := harnesspkg.NewHarness(
		t,
		harnesspkg.WithClientConfig(func(cfg *config.Config) {
			cfg.ControlPlane.MaxInFlightPayloadBytes = maxInFlightPayloadBytes
		}),
		harnesspkg.WithBeforeClientStop(func(h *harnesspkg.Harness) {
			// The mock marks idle as soon as it records the final response,
			// which can be slightly before the client's release defers run.
			require.Eventually(t, func() bool {
				return h.PayloadBudget.InFlight() == 0
			}, 2*time.Second, 20*time.Millisecond, "payload reservation leaked after drain")
		}),
		harnesspkg.WithControlPlaneOptions(
			mocktunnelservice.WithSessionHeaderPropagation(),
			mocktunnelservice.WithInitializationPhaseCommands(),
			mocktunnelservice.WithCommandResponses(bigCommand, smallCommand),
		),
		harnesspkg.WithMCPOptions(
			mockmcpserver.WithCalls(mockmcpserver.Call{
				Tool:   "echo",
				Result: json.RawMessage(`{"ok":true}`),
			}),
		),
	)
	h.ExecuteScenarious(t)

	requests := h.MCP.ReceivedRequests()
	if len(requests) != 1 {
		t.Fatalf("MCP tool calls = %d, want 1 (rejected command must never reach the MCP server)", len(requests))
	}
	matched := h.ControlPlane.ReceivedResponses(mocktunnelservice.ResponseMatchMatched)
	if len(matched) != 4 {
		t.Fatalf("matched responses = %d, want 4 (initialize, initialized, 413, small tool 200)", len(matched))
	}
}

// TestPayloadBudgetSlowDeliveryWaitsThenCompletesWithoutReplay pins most of the
// budget after initialization so a tool response cannot be delivered, verifies
// no HTTP delivery is attempted while quota is exhausted, then releases quota
// and injects two 502 responses. The tool must execute exactly once even though
// its response is retried three times, and a following small request completes
// after quota recovers.
func TestPayloadBudgetSlowDeliveryWaitsThenCompletesWithoutReplay(t *testing.T) {
	t.Parallel()

	const maxInFlightPayloadBytes int64 = 4096

	var (
		linkBudget      *payloadbudget.Budget
		budgetReady     = make(chan struct{})
		pinned          = make(chan struct{})
		posts           atomic.Int32
		heldDuringRetry atomic.Bool

		holdMu sync.Mutex
		held   *payloadbudget.Reservation
	)

	setHeld := func(r *payloadbudget.Reservation) {
		holdMu.Lock()
		held = r
		holdMu.Unlock()
	}
	releaseHeld := func() {
		holdMu.Lock()
		r := held
		held = nil
		holdMu.Unlock()
		if r != nil {
			r.Release()
		}
	}

	firstTool := mocktunnelservice.CommandResponse{
		Command: budgetEchoCommand("cmd-one", "rpc-one", 8),
		ExpectedResponses: []mocktunnelservice.ExpectedResponse{{
			RequestID: "cmd-one",
			Assert:    assertBudgetToolOK,
		}},
	}
	secondTool := mocktunnelservice.CommandResponse{
		Command: budgetEchoCommand("cmd-two", "rpc-two", 8),
		ExpectedResponses: []mocktunnelservice.ExpectedResponse{{
			RequestID: "cmd-two",
			Assert:    assertBudgetToolOK,
		}},
	}

	// POST #1 initialize, #2 initialized ack: after the ack reservation is
	// acquired, pin 3200 bytes. #3/#4 are the throttled tool response's first
	// two attempts and fail with 502; #5 is its retry success; #6 is the
	// second tool's response.
	responseHandler := func(w http.ResponseWriter, r *http.Request) bool {
		n := posts.Add(1)
		switch n {
		case 2:
			select {
			case <-budgetReady:
			case <-r.Context().Done():
				return false
			case <-time.After(5 * time.Second):
				return false
			}
			reservation, err := linkBudget.Acquire(r.Context(), 3200)
			if err == nil {
				setHeld(reservation)
				close(pinned)
			}
			return false
		case 3, 4:
			if linkBudget.InFlight() > 0 {
				heldDuringRetry.Store(true)
			}
			w.WriteHeader(http.StatusBadGateway)
			return true
		default:
			return false
		}
	}

	watcherErr := make(chan error, 4)
	go func() {
		select {
		case <-pinned:
		case <-time.After(10 * time.Second):
			watcherErr <- errors.New("response budget was never pinned")
			return
		}
		// While quota is pinned, the large tool response must not even attempt
		// an HTTP delivery (posts must remain at 2: initialize + initialized).
		time.Sleep(300 * time.Millisecond)
		if got := posts.Load(); got != 2 {
			watcherErr <- fmt.Errorf("delivery attempted while budget was exhausted: posts=%d, want 2", got)
		}
		releaseHeld()
	}()

	h := harnesspkg.NewHarness(
		t,
		harnesspkg.WithScenarioTimeout(20*time.Second),
		harnesspkg.WithClientConfig(func(cfg *config.Config) {
			cfg.ControlPlane.MaxInFlightPayloadBytes = maxInFlightPayloadBytes
		}),
		harnesspkg.WithAfterClientStart(func(h *harnesspkg.Harness) {
			linkBudget = h.PayloadBudget
			close(budgetReady)
		}),
		harnesspkg.WithBeforeClientStop(func(h *harnesspkg.Harness) {
			require.Eventually(t, func() bool {
				return h.PayloadBudget.InFlight() == 0
			}, 2*time.Second, 20*time.Millisecond, "payload reservation leaked after drain")
		}),
		harnesspkg.WithControlPlaneOptions(
			mocktunnelservice.WithSessionHeaderPropagation(),
			mocktunnelservice.WithInitializationPhaseCommands(),
			mocktunnelservice.WithResponseHandler(responseHandler),
			mocktunnelservice.WithCommandResponses(firstTool, secondTool),
		),
		harnesspkg.WithMCPOptions(
			mockmcpserver.WithCalls(
				mockmcpserver.Call{
					Tool: "echo",
					DynamicResult: func(json.RawMessage) (json.RawMessage, error) {
						return json.RawMessage(fmt.Sprintf(`{"data":"%s"}`, strings.Repeat("b", 2000))), nil
					},
				},
				mockmcpserver.Call{
					Tool: "echo",
					DynamicResult: func(json.RawMessage) (json.RawMessage, error) {
						return json.RawMessage(fmt.Sprintf(`{"data":"%s"}`, strings.Repeat("b", 2000))), nil
					},
				},
			),
		),
	)
	h.ExecuteScenarious(t)
	releaseHeld()

	closeWatcherErr := func() string {
		select {
		case err := <-watcherErr:
			return err.Error()
		default:
			return ""
		}
	}
	if msg := closeWatcherErr(); msg != "" {
		t.Fatal(msg)
	}
	if got := posts.Load(); got != 6 {
		t.Fatalf("response POSTs = %d, want 6 (init x2, retried tool x3 attempts, second tool x1)", got)
	}
	if !heldDuringRetry.Load() {
		t.Fatal("delivery reservation was not held during the 502 retry attempts")
	}
	requests := h.MCP.ReceivedRequests()
	if len(requests) != 2 {
		t.Fatalf("MCP tool calls = %d, want 2: HTTP retries must resend bytes and never replay the tool", len(requests))
	}
	matched := h.ControlPlane.ReceivedResponses(mocktunnelservice.ResponseMatchMatched)
	if len(matched) != 4 {
		t.Fatalf("matched responses = %d, want 4 (initialize, initialized, tool one, tool two)", len(matched))
	}
}

// TestPayloadBudgetReleasesReservationOnShutdown verifies that an in-flight
// request reservation does not block (or survive) client shutdown while an MCP
// tool call is still executing.
func TestPayloadBudgetReleasesReservationOnShutdown(t *testing.T) {
	t.Parallel()

	releaseTool := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseTool) }) }
	// The mock MCP server's outstanding HTTP handler must finish before its
	// httptest server can close; unblock it shortly after the client begins
	// shutting down (scenario timeout is 1.5s).
	go func() {
		time.Sleep(2 * time.Second)
		release()
	}()

	slowCommand := mocktunnelservice.CommandResponse{
		Command: budgetToolCommand("slowtool", "cmd-slow", "slow-rpc", 8),
		ExpectedResponses: []mocktunnelservice.ExpectedResponse{{
			RequestID: "cmd-slow",
			Assert:    func(testing.TB, mocktunnelservice.ReceivedResponse) {},
		}},
	}

	h := harnesspkg.NewHarness(
		t,
		harnesspkg.WithScenarioTimeout(1500*time.Millisecond),
		harnesspkg.WithClientConfig(func(cfg *config.Config) {
			cfg.ControlPlane.MaxInFlightPayloadBytes = 4096
		}),
		harnesspkg.WithControlPlaneOptions(
			mocktunnelservice.WithAllowPendingCommands(),
			mocktunnelservice.WithSessionHeaderPropagation(),
			mocktunnelservice.WithInitializationPhaseCommands(),
			mocktunnelservice.WithCommandResponses(slowCommand),
		),
		harnesspkg.WithMCPOptions(
			mockmcpserver.WithCalls(mockmcpserver.Call{
				Tool: "slowtool",
				DynamicResult: func(json.RawMessage) (json.RawMessage, error) {
					<-releaseTool
					return json.RawMessage(`{"ok":true}`), nil
				},
			}),
		),
	)

	err := h.ExecuteScenario(t)
	require.Error(t, err, "scenario must time out while the tool is still executing")
	defer release()

	// Shutdown completed without waiting for the tool and released its
	// reservation; the tool itself ran exactly once.
	if got := h.PayloadBudget.InFlight(); got != 0 {
		t.Fatalf("in-flight payload bytes after shutdown = %d, want 0", got)
	}
	requests := h.MCP.ReceivedRequests()
	if len(requests) != 1 || requests[0].Tool != "slowtool" {
		t.Fatalf("MCP tool calls = %+v, want one slowtool invocation", requests)
	}
}
