package internal

import (
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"

	"github.com/openai/tunnel-client/pkg/controlplane"
	"github.com/openai/tunnel-client/pkg/controlplane/wiretypes"
	"github.com/openai/tunnel-client/pkg/payloadbudget"
	"github.com/openai/tunnel-client/pkg/types"
)

var (
	_ controlplane.ResponseDeadlineProvider  = (*basePolledCommand)(nil)
	_ controlplane.PayloadBudgetCarrier      = (*basePolledCommand)(nil)
	_ controlplane.PolledCommand             = (*oauthDiscoveryCommand)(nil)
	_ controlplane.OauthDiscoveryCommand     = (*oauthDiscoveryCommand)(nil)
	_ controlplane.PolledCommand             = (*jsonRpcCommand)(nil)
	_ controlplane.JsonRpcCommand            = (*jsonRpcCommand)(nil)
	_ controlplane.TraceContextProvider      = (*jsonRpcCommand)(nil)
	_ controlplane.PolledCommand             = (*sessionTerminationCommand)(nil)
	_ controlplane.SessionTerminationCommand = (*sessionTerminationCommand)(nil)
	_ typedCommand                           = (*oauthDiscoveryCommand)(nil)
	_ typedCommand                           = (*jsonRpcCommand)(nil)
	_ typedCommand                           = (*sessionTerminationCommand)(nil)
)

// typedCommand narrows commands to those with a known discriminator.
type typedCommand interface {
	controlplane.PolledCommand
	commandType() wiretypes.CommandType
}

// basePolledCommand contains fields common to all polled command implementations
// and implements the internal.PolledCommand interface.
type basePolledCommand struct {
	requestID  types.RequestID
	enqueued   time.Time
	polledAt   time.Time
	headers    http.Header
	sessionID  *string
	shardToken string
	channel    types.Channel
	deadline   *time.Time
	// payloadBytes is the measured wire payload size used by the optional link
	// payload byte budget. Zero means no measurement is available.
	payloadBytes int64
	// reservation holds the command's admitted byte budget while it travels
	// through queuing, MCP execution, and response delivery. It is released
	// exactly once by the processor (or the queue listener at shutdown).
	reservation *payloadbudget.Reservation
	// admissionErr is set when the command was rejected at budget admission;
	// the processor must answer it without executing any tool operation.
	admissionErr error
}

// PayloadBytes reports the measured payload size for the byte budget.
func (c *basePolledCommand) PayloadBytes() int64 { return c.payloadBytes }

func (c *basePolledCommand) setPayloadBytes(n int64) { c.payloadBytes = n }

// AttachPayloadReservation records the reservation acquired at admission.
func (c *basePolledCommand) AttachPayloadReservation(r *payloadbudget.Reservation) {
	c.reservation = r
}

// PayloadReservation returns the reservation held for this command, if any.
func (c *basePolledCommand) PayloadReservation() *payloadbudget.Reservation {
	return c.reservation
}

// ReleasePayloadReservation returns the held bytes exactly once; it is safe to
// call when no reservation is attached.
func (c *basePolledCommand) ReleasePayloadReservation() {
	if c.reservation != nil {
		c.reservation.Release()
		c.reservation = nil
	}
}

// MarkPayloadAdmissionRejected records an admission rejection (for example a
// payload larger than the whole budget) so the processor can answer without
// executing the tool.
func (c *basePolledCommand) MarkPayloadAdmissionRejected(reason error) {
	c.admissionErr = reason
}

// PayloadAdmissionError returns the admission rejection reason, or nil.
func (c *basePolledCommand) PayloadAdmissionError() error { return c.admissionErr }

func (c *basePolledCommand) RequestID() types.RequestID { return c.requestID }
func (c *basePolledCommand) EnqueuedAt() time.Time      { return c.enqueued }
func (c *basePolledCommand) PolledAt() time.Time        { return c.polledAt }
func (c *basePolledCommand) Headers() http.Header {
	if c.headers == nil {
		return nil
	}
	return c.headers
}
func (c *basePolledCommand) ShardToken() string     { return c.shardToken }
func (c *basePolledCommand) Channel() types.Channel { return c.channel }
func (c *basePolledCommand) ResponseDeadline() (time.Time, bool) {
	if c.deadline == nil {
		return time.Time{}, false
	}
	return *c.deadline, true
}
func (c *basePolledCommand) SessionID() (string, bool) {
	if c.sessionID == nil {
		return "", false
	}
	return *c.sessionID, true
}

// commandType returns an empty discriminator for the base type; concrete
// commands must override this.
func (c *basePolledCommand) commandType() wiretypes.CommandType { return "" }

// jsonRpcCommand represents a JSON-RPC command; it implements JsonRpcCommand via Message().
type jsonRpcCommand struct {
	basePolledCommand
	message             jsonrpc.Message
	traceContext        *types.TraceContext
	commandTraceContext *types.TraceContext
}

func (c *jsonRpcCommand) TraceContext() *types.TraceContext {
	if c.traceContext == nil {
		return nil
	}
	result := *c.traceContext
	return &result
}

func (c *jsonRpcCommand) CommandTraceContext() *types.TraceContext {
	if c.commandTraceContext == nil {
		return nil
	}
	result := *c.commandTraceContext
	return &result
}

// Message is only implemented by jsonRpcCommand
func (c *jsonRpcCommand) Message() jsonrpc.Message { return c.message }
func (c *jsonRpcCommand) commandType() wiretypes.CommandType {
	return wiretypes.CommandTypeJSONRPC
}

// oauthDiscoveryCommand represents a non-JSON-RPC command (OAuth discovery).
// It intentionally does NOT include a Message() method so it will not satisfy
// JsonRpcCommand, allowing the dispatcher to distinguish the type.
type oauthDiscoveryCommand struct {
	basePolledCommand
}

func (c *oauthDiscoveryCommand) commandType() wiretypes.CommandType {
	return wiretypes.CommandTypeOAuthDiscovery
}

func (c *oauthDiscoveryCommand) IsOAuthDiscovery() bool { return true }

// sessionTerminationCommand represents an explicit Streamable HTTP session close.
type sessionTerminationCommand struct {
	basePolledCommand
}

func (c *sessionTerminationCommand) commandType() wiretypes.CommandType {
	return wiretypes.CommandTypeSessionTermination
}

func (c *sessionTerminationCommand) IsSessionTermination() bool { return true }
