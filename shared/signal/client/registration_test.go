package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	"google.golang.org/grpc/metadata"

	sigProto "github.com/netbirdio/netbird/shared/signal/proto"
	"github.com/netbirdio/netbird/signal/server"
)

// stallingSignalServer holds the first ConnectStream open without ever sending
// the registration header, while the transport keeps answering keepalive pings.
// Later streams go to the real server. This is what a responder in front of or
// inside Signal looks like when it accepts a stream and never answers it.
type stallingSignalServer struct {
	*server.Server
	stalled atomic.Bool
	held    chan struct{}
}

func (s *stallingSignalServer) ConnectStream(stream sigProto.SignalExchange_ConnectStreamServer) error {
	if s.stalled.CompareAndSwap(false, true) {
		close(s.held)
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return s.Server.ConnectStream(stream)
}

// TestReceiveRetriesWhenRegistrationHeaderNeverArrives reproduces a client stuck
// forever waiting for the registration header of a stream the server side never
// answers: the receive watchdog only starts after registration, so nothing ended
// the wait and Signal never came back until the daemon was restarted. The client
// must give up on the silent stream and register on a new one.
func TestReceiveRetriesWhenRegistrationHeaderNeverArrives(t *testing.T) {
	realServer, err := server.NewServer(context.Background(), otel.Meter(""))
	require.NoError(t, err)
	stalling := &stallingSignalServer{Server: realServer, held: make(chan struct{})}
	addr := serveTestSignalExchange(t, stalling)

	key, err := wgtypes.GenerateKey()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client, err := NewClient(ctx, addr, key, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	client.registrationTimeout = 200 * time.Millisecond

	// Stop Receive before Close runs (cleanups are LIFO), so the two never overlap.
	receiveDone := make(chan struct{})
	go func() {
		defer close(receiveDone)
		_ = client.Receive(ctx, func(*sigProto.Message) error { return nil })
	}()
	t.Cleanup(func() {
		cancel()
		<-receiveDone
	})

	select {
	case <-stalling.held:
	case <-time.After(5 * time.Second):
		t.Fatal("first stream did not reach the stalling server")
	}

	streamReady := make(chan struct{})
	go func() {
		client.WaitStreamConnected(ctx)
		close(streamReady)
	}()
	select {
	case <-streamReady:
	case <-time.After(5 * time.Second):
		t.Fatal("client never registered after the first stream went unanswered")
	}
	require.True(t, client.StreamConnected(), "client should report a registered stream")
}

// headerHoldingSignalServer delivers the registration header only with the first message, like AWS ALB.
type headerHoldingSignalServer struct {
	*server.Server
}

func (s *headerHoldingSignalServer) ConnectStream(stream sigProto.SignalExchange_ConnectStreamServer) error {
	return s.Server.ConnectStream(&headerHoldingStream{stream})
}

type headerHoldingStream struct {
	sigProto.SignalExchange_ConnectStreamServer
}

// SendHeader only stages the header; gRPC flushes it with the first message.
func (s *headerHoldingStream) SendHeader(md metadata.MD) error {
	return s.SetHeader(md)
}

// TestRegistrationProbeReleasesHeldHeader covers a new peer nobody messages yet: without a
// self-probe no message ever follows the header, so registration times out on every stream.
func TestRegistrationProbeReleasesHeldHeader(t *testing.T) {
	realServer, err := server.NewServer(context.Background(), otel.Meter(""))
	require.NoError(t, err)
	addr := serveTestSignalExchange(t, &headerHoldingSignalServer{Server: realServer})

	key, err := wgtypes.GenerateKey()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client, err := NewClient(ctx, addr, key, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	client.registrationTimeout = 2 * time.Second
	client.registrationProbeDelay = 50 * time.Millisecond
	client.registrationProbeInterval = 100 * time.Millisecond

	receiveDone := make(chan struct{})
	go func() {
		defer close(receiveDone)
		_ = client.Receive(ctx, func(*sigProto.Message) error { return nil })
	}()
	t.Cleanup(func() {
		cancel()
		<-receiveDone
	})

	streamReady := make(chan struct{})
	go func() {
		client.WaitStreamConnected(ctx)
		close(streamReady)
	}()
	select {
	case <-streamReady:
	case <-time.After(client.registrationTimeout):
		t.Fatal("client did not register within the registration timeout while the header was held")
	}
	require.True(t, client.StreamConnected(), "client should report a registered stream")
}

// TestRegistrationProbeWaitsAndStopsPromptly checks the first probe waits for the delay and
// that stopping cancels a send still in flight.
func TestRegistrationProbeWaitsAndStopsPromptly(t *testing.T) {
	sent := make(chan struct{}, 1)
	send := func(ctx context.Context) error {
		sent <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}

	stop := startRegistrationProbe(context.Background(), 200*time.Millisecond, time.Hour, send)
	select {
	case <-sent:
		t.Fatal("probe sent before the delay")
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("no probe after the delay")
	}

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the in-flight probe")
	}
}
