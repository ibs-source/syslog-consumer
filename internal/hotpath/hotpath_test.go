package hotpath

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubyte-source/go-jsonfast"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// testConfig returns a minimal valid configuration for unit tests.
func testConfig() *config.Config {
	return &config.Config{
		Redis: config.RedisConfig{
			Stream:              "test-stream",
			ClaimIdle:           30 * time.Second,
			CleanupInterval:     1 * time.Minute,
			ConsumerIdleTimeout: 5 * time.Minute,
		},
		Pipeline: config.PipelineConfig{
			BufferCapacity:       100,
			MessageQueueCapacity: 4,
			ErrorBackoff:         1 * time.Second,
			AckTimeout:           5 * time.Second,
			PublishWorkers:       2,
			AckWorkers:           2,
			RefreshInterval:      1 * time.Minute,
		},
	}
}

func closeHotPath(t *testing.T, hp *HotPath) {
	t.Helper()
	if err := hp.Close(); err != nil {
		t.Errorf("hp.Close(): %v", err)
	}
}

func checkLoopExit(t *testing.T, err error) {
	t.Helper()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("unexpected loop error: %v", err)
	}
}

// --- New() validation tests ---

func TestNew_NilRedis(t *testing.T) {
	_, err := New(nil, &mockPublisher{}, testConfig(), log.New())
	if err == nil || !strings.Contains(err.Error(), "redis client must not be nil") {
		t.Errorf("New(nil redis) error = %v; want 'redis client must not be nil'", err)
	}
}

func TestNew_NilMQTT(t *testing.T) {
	_, err := New(&mockRedis{}, nil, testConfig(), log.New())
	if err == nil || !strings.Contains(err.Error(), "mqtt publisher must not be nil") {
		t.Errorf("New(nil mqtt) error = %v; want 'mqtt publisher must not be nil'", err)
	}
}

func TestNew_NilConfig(t *testing.T) {
	_, err := New(&mockRedis{}, &mockPublisher{}, nil, log.New())
	if err == nil || !strings.Contains(err.Error(), "config must not be nil") {
		t.Errorf("New(nil config) error = %v; want 'config must not be nil'", err)
	}
}

func TestNew_NilLogger(t *testing.T) {
	_, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), nil)
	if err == nil || !strings.Contains(err.Error(), "logger must not be nil") {
		t.Errorf("New(nil logger) error = %v; want 'logger must not be nil'", err)
	}
}

func TestNew_ValidDependencies(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v; want nil", err)
	}
	defer closeHotPath(t, hp)

	if hp.msgChan == nil {
		t.Error("msgChan not initialized")
	}
	if hp.ackChans == nil {
		t.Error("ackChans not initialized")
	}
	if hp.lifecycleCtx == nil {
		t.Error("lifecycleCtx not initialized")
	}
	if hp.claimTicker == nil {
		t.Error("claimTicker not initialized")
	}
	if hp.cleanupTicker == nil {
		t.Error("cleanupTicker not initialized")
	}
	if hp.refreshTicker != nil {
		t.Error("refreshTicker should be nil in single-stream mode")
	}
	if !hp.singleStream {
		t.Error("singleStream should be true when cfg.Redis.Stream != empty")
	}
}

func TestNew_MultiStreamMode(t *testing.T) {
	cfg := testConfig()
	cfg.Redis.Stream = ""
	hp, err := New(&mockRedis{}, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v; want nil", err)
	}
	defer closeHotPath(t, hp)
	if hp.singleStream {
		t.Error("singleStream should be false when cfg.Redis.Stream is empty")
	}
}

// --- Run() tests ---

func TestRun_GracefulShutdown(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	runErr := hp.Run(ctx)
	if !errors.Is(runErr, context.Canceled) {
		t.Errorf("Run() error = %v; want context.Canceled", runErr)
	}
}

func TestRun_SubscribeAckError(t *testing.T) {
	subErr := errors.New("subscribe failed")
	pub := &mockPublisher{
		subscribeAckFn: func(_ func(message.AckMessage)) error {
			return subErr
		},
	}

	hp, err := New(&mockRedis{}, pub, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx := t.Context()
	runErr := hp.Run(ctx)
	if runErr == nil || !strings.Contains(runErr.Error(), "subscribe failed") {
		t.Errorf("Run() error = %v; want 'subscribe failed'", runErr)
	}
}

func TestRun_FetchAndPublish(t *testing.T) {
	var publishCount atomic.Int32

	called := make(chan struct{}, 1)
	r := &mockRedis{
		readBatchFn: func(ctx context.Context) (message.Batch, error) {
			select {
			case <-called:
				// Already sent one batch, block until context canceled
				<-ctx.Done()
				return message.Batch{}, ctx.Err()
			default:
			}
			close(called)
			return message.Batch{
				Items: []message.Redis{
					{ID: "1-0", Stream: "s1", Object: `{"k":"v"}`},
				},
			}, nil
		},
	}

	pub := &mockPublisher{
		publishFn: func(_ context.Context, _ message.Payload) error {
			publishCount.Add(1)
			return nil
		},
	}

	hp, err := New(r, pub, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- hp.Run(ctx) }()

	// Wait for message to be published
	deadline := time.After(5 * time.Second)
	for publishCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for publish")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	<-done
	if publishCount.Load() < 1 {
		t.Errorf("expected at least 1 publish, got %d", publishCount.Load())
	}
}

// --- handleAck tests ---

func TestHandleAck_Bounded(t *testing.T) {
	cfg := testConfig()
	cfg.Pipeline.PublishWorkers = 2

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	var totalFlushed atomic.Int64

	r := &mockRedis{
		ackAndDeleteFn: func(_ context.Context, ids []string, _ string) error {
			cur := concurrent.Add(1)
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			concurrent.Add(-1)
			totalFlushed.Add(int64(len(ids)))
			return nil
		},
	}

	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	// Start ACK workers (normally started by Run)
	for i := range hp.ackWorkers {
		ch := hp.ackChans[i]
		hp.ackWg.Go(func() { hp.ackWorker(ch) })
	}

	// Fire 10 ACKs rapidly
	for range 10 {
		hp.handleAck(message.AckMessage{IDs: []string{"id"}, Stream: "s", Ack: true})
	}

	// Close all ack channels to trigger final flush and let workers drain
	for _, ch := range hp.ackChans {
		close(ch)
	}
	hp.ackWg.Wait()

	if maxConcurrent.Load() > 2 {
		t.Errorf("max concurrent ACK goroutines = %d; want <= 2", maxConcurrent.Load())
	}
	if totalFlushed.Load() != 10 {
		t.Errorf("total flushed IDs = %d; want 10", totalFlushed.Load())
	}
}

func TestFlushACKs_Success(t *testing.T) {
	var calledIDs []string
	var calledStream string
	r := &mockRedis{
		ackAndDeleteFn: func(_ context.Context, ids []string, stream string) error {
			calledIDs = ids
			calledStream = stream
			return nil
		},
	}

	hp, err := New(r, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	hp.flushACKs("stream-A", &pendingACK{ackIDs: []string{"123-0"}})

	if len(calledIDs) != 1 || calledIDs[0] != "123-0" || calledStream != "stream-A" {
		t.Errorf("AckAndDeleteBatch called with ids=%v stream=%s; want [123-0], stream-A", calledIDs, calledStream)
	}
}

func TestFlushACKs_NackOnly(t *testing.T) {
	called := false
	r := &mockRedis{
		ackAndDeleteFn: func(_ context.Context, _ []string, _ string) error {
			called = true
			return nil
		},
	}

	hp, err := New(r, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	hp.flushACKs("s", &pendingACK{nackCount: 1})

	if called {
		t.Error("AckAndDeleteBatch should NOT be called when only NACKs")
	}
}

func TestFlushACKs_LifecycleContextCancelled(t *testing.T) {
	r := &mockRedis{
		ackAndDeleteFn: func(ctx context.Context, _ []string, _ string) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	hp, err := New(r, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Cancel lifecycle immediately
	hp.lifecycleCancel()

	done := make(chan struct{})
	go func() {
		hp.flushACKs("s", &pendingACK{ackIDs: []string{"x"}})
		close(done)
	}()

	select {
	case <-done:
		// OK — returned quickly
	case <-time.After(2 * time.Second):
		t.Fatal("flushACKs did not return after lifecycleCtx canceled")
	}

	closeHotPath(t, hp)
}

// --- buildPayload tests ---

// parseLine splits a tab-separated payload line into (id, stream, json).
func parseLine(t *testing.T, line []byte) (id, stream, jsonPart string) {
	t.Helper()
	parts := strings.SplitN(string(line), "\t", 3)
	if len(parts) != 3 {
		t.Fatalf("expected 3 tab-separated parts, got %d: %q", len(parts), line)
	}
	return parts[0], parts[1], parts[2]
}

var buildPayloadTests = []struct {
	name     string
	msg      message.Redis
	wantJSON string
}{
	{
		name: "syslog with structured_data flattened",
		msg: message.Redis{
			ID:     "1-0",
			Stream: "syslog-stream",
			Object: `{"hostname":"FW01","severity":3,"structured_data":{"KV@123":{"action":"pass","srcip":"1.2.3.4"}}}`,
			Raw:    "<190>1 test raw",
		},
		wantJSON: `{"hostname":"FW01","action":"pass","srcip":"1.2.3.4","severity":"ERROR","raw":"<190>1 test raw"}`,
	},
	{
		name: "syslog without structured_data",
		msg: message.Redis{
			ID:     "2-0",
			Stream: "s",
			Object: `{"hostname":"router1","severity":6,"message":"hello"}`,
			Raw:    "raw line",
		},
		wantJSON: `{"hostname":"router1","message":"hello","severity":"INFO","raw":"raw line"}`,
	},
	{
		name: "empty fields",
		msg: message.Redis{
			ID:     "3-0",
			Stream: "s",
		},
		wantJSON: `{"raw":"-"}`,
	},
	{
		name: "empty object and raw",
		msg: message.Redis{
			ID:     "4-0",
			Stream: "s",
		},
		wantJSON: `{"raw":"-"}`,
	},
	{
		name: "empty raw replaced with dash",
		msg: message.Redis{
			ID:     "5-0",
			Stream: "s",
			Object: `{"hostname":"h1","severity":7}`,
		},
		wantJSON: `{"hostname":"h1","severity":"DEBUG","raw":"-"}`,
	},
	{
		name: "non-JSON object ignored",
		msg: message.Redis{
			ID:     "6-0",
			Stream: "s",
			Object: "not json",
		},
		wantJSON: `{"raw":"-"}`,
	},
	{
		name: "deep nested structured_data",
		msg: message.Redis{
			ID:     "7-0",
			Stream: "s",
			Object: `{"severity":0,"structured_data":{"L1":{"L2":{"key":"deep"}}}}`,
			Raw:    "r",
		},
		wantJSON: `{"key":"deep","severity":"EMERGENCY","raw":"r"}`,
	},
}

// jsonEqual compares two JSON byte strings for semantic equality,
// ignoring key order.
func jsonEqual(a, b []byte) bool {
	var ja, jb any
	if err := json.Unmarshal(a, &ja); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &jb); err != nil {
		return false
	}
	return reflect.DeepEqual(ja, jb)
}

func TestBuildPayload(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	builder := jsonfast.New(512)
	for _, tt := range buildPayloadTests {
		t.Run(tt.name, func(t *testing.T) {
			result := hp.buildPayload(builder, &tt.msg)
			gotID, gotStream, gotJSON := parseLine(t, result)
			if gotID != tt.msg.ID {
				t.Errorf("id = %q, want %q", gotID, tt.msg.ID)
			}
			if gotStream != tt.msg.Stream {
				t.Errorf("stream = %q, want %q", gotStream, tt.msg.Stream)
			}
			if !jsonEqual([]byte(gotJSON), []byte(tt.wantJSON)) {
				t.Errorf("JSON mismatch:\n  got:  %s\n  want: %s", gotJSON, tt.wantJSON)
			}
		})
	}
}

// TestBuildPayload_FieldOrder verifies that object JSON fields
// appear in iteration order and structured_data is flattened inline.
func TestBuildPayload_FieldOrder(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	msg := message.Redis{
		ID:     "1-0",
		Stream: "s",
		Object: `{"hostname":"fw01","severity":6,"facility":23}`,
		Raw:    "test",
	}
	builder := jsonfast.New(512)
	result := hp.buildPayload(builder, &msg)
	_, _, gotJSON := parseLine(t, result)

	expected := `{"hostname":"fw01","facility":23,"severity":"INFO","raw":"test"}`
	if !jsonEqual([]byte(gotJSON), []byte(expected)) {
		t.Errorf("JSON mismatch:\n  got:  %s\n  want: %s", gotJSON, expected)
	}
}

func TestBuildPayload_StreamInTabPrefix(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	// Stream name with special chars passes through literally in tab prefix.
	builder := jsonfast.New(512)
	msg := message.Redis{ID: "1-0", Stream: `path\to"stream`}
	result := hp.buildPayload(builder, &msg)
	gotID, gotStream, _ := parseLine(t, result)
	if gotID != "1-0" {
		t.Errorf("id = %q, want 1-0", gotID)
	}
	if gotStream != `path\to"stream` {
		t.Errorf("stream = %q, want path\\to\"stream", gotStream)
	}
}

// --- Close tests ---

func TestClose(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if closeErr := hp.Close(); closeErr != nil {
		t.Errorf("Close() returned error: %v", closeErr)
	}
}

// --- EscapeString via jsonfast (replaces old jsonEscape tests) ---

func TestEscapeString(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  string
	}{
		{"no special chars", "hello-world", "hello-world"},
		{"empty string", "", ""},
		{"contains quote", `say "hello"`, `say \"hello\"`},
		{"contains backslash", `path\to\file`, `path\\to\\file`},
		{"mixed", `a"b\c`, `a\"b\\c`},
		{"newline", "line1\nline2", `line1\nline2`},
		{"tab", "col1\tcol2", `col1\tcol2`},
		{"carriage return", "text\rmore", `text\rmore`},
		{"control char", "hello\x01world", `hello\u0001world`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := jsonfast.EscapeString(tt.in); got != tt.out {
				t.Errorf("EscapeString(%q) = %q; want %q", tt.in, got, tt.out)
			}
		})
	}
}

// --- startLoop tests ---

func TestStartLoop_ReportsError(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	testErr := errors.New("test loop error")
	loopFn := func(_ context.Context) error {
		return testErr
	}

	hp.startLoop(t.Context(), &wg, "test", loopFn, errCh)
	wg.Wait()

	select {
	case loopErr := <-errCh:
		if !strings.Contains(loopErr.Error(), "test loop error") {
			t.Errorf("unexpected error: %v", loopErr)
		}
	default:
		t.Error("expected error on errCh")
	}
}

func TestStartLoop_SuppressesContextCanceled(t *testing.T) {
	hp, err := New(&mockRedis{}, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	loopFn := func(_ context.Context) error {
		return context.Canceled
	}

	hp.startLoop(t.Context(), &wg, "test", loopFn, errCh)
	wg.Wait()

	select {
	case loopErr := <-errCh:
		t.Errorf("expected no error on errCh, got: %v", loopErr)
	default:
		// OK — context.Canceled was suppressed
	}
}

// --- fetchLoop tests ---

func TestFetchLoop_ReadError(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		readBatchFn: func(ctx context.Context) (message.Batch, error) {
			if callCount.Add(1) >= 2 {
				<-ctx.Done()
				return message.Batch{}, ctx.Err()
			}
			return message.Batch{}, errors.New("read error")
		},
	}

	cfg := testConfig()
	cfg.Pipeline.ErrorBackoff = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.fetchLoop(ctx))

	if callCount.Load() < 2 {
		t.Error("expected readBatch to be called again after backoff")
	}
}

func TestFetchLoop_EmptyBatch(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		readBatchFn: func(ctx context.Context) (message.Batch, error) {
			if callCount.Add(1) >= 3 {
				<-ctx.Done()
				return message.Batch{}, ctx.Err()
			}
			return message.Batch{}, nil
		},
	}

	hp, err := New(r, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.fetchLoop(ctx))

	if callCount.Load() < 3 {
		t.Errorf("expected at least 3 calls, got %d", callCount.Load())
	}
}

// --- publishLoop tests ---

func TestPublishLoop_EmptyBody(t *testing.T) {
	publishCalled := false
	pub := &mockPublisher{
		publishFn: func(_ context.Context, _ message.Payload) error {
			publishCalled = true
			return nil
		},
	}

	hp, err := New(&mockRedis{}, pub, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithCancel(t.Context())

	// Put an empty body message (both Object and Raw are empty)
	hp.msgChan <- message.Batch{Items: []message.Redis{{ID: "1", Stream: "s"}}}

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	checkLoopExit(t, hp.makePublishLoop(0)(ctx))

	if publishCalled {
		t.Error("publish should not be called for empty body message")
	}
}

func TestPublishLoop_PublishError(t *testing.T) {
	var publishCount atomic.Int32
	pub := &mockPublisher{
		publishFn: func(_ context.Context, _ message.Payload) error {
			publishCount.Add(1)
			return errors.New("publish failed")
		},
	}

	hp, err := New(&mockRedis{}, pub, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithCancel(t.Context())

	// Put a valid message
	hp.msgChan <- message.Batch{Items: []message.Redis{{ID: "1", Stream: "s", Object: `{"k":"v"}`}}}

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	checkLoopExit(t, hp.makePublishLoop(0)(ctx))

	if publishCount.Load() < 1 {
		t.Error("publish should have been called at least once")
	}
}

// --- claimLoop tests ---

func TestClaimLoop_WithItems(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		claimIdleFn: func(ctx context.Context) (message.Batch, error) {
			if callCount.Add(1) == 1 {
				return message.Batch{
					Items: []message.Redis{
						{ID: "claimed-1", Stream: "s"},
					},
				}, nil
			}
			<-ctx.Done()
			return message.Batch{}, ctx.Err()
		},
	}

	cfg := testConfig()
	cfg.Redis.ClaimIdle = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- hp.claimLoop(ctx) }()

	// Read the claimed message from the channel
	select {
	case batch := <-hp.msgChan:
		if len(batch.Items) != 1 || batch.Items[0].ID != "claimed-1" {
			t.Errorf("expected claimed-1, got %v", batch.Items)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for claimed message")
	}

	cancel()
	<-done
}

func TestClaimLoop_Error(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		claimIdleFn: func(ctx context.Context) (message.Batch, error) {
			if callCount.Add(1) == 1 {
				return message.Batch{}, errors.New("claim error")
			}
			<-ctx.Done()
			return message.Batch{}, ctx.Err()
		},
	}

	cfg := testConfig()
	cfg.Redis.ClaimIdle = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.claimLoop(ctx))

	if callCount.Load() < 1 {
		t.Error("ClaimIdle should have been called")
	}
}

// --- cleanupLoop tests ---

func TestCleanupLoop_Error(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		cleanupFn: func(ctx context.Context, _ time.Duration) error {
			if callCount.Add(1) == 1 {
				return errors.New("cleanup error")
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	cfg := testConfig()
	cfg.Redis.CleanupInterval = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.cleanupLoop(ctx))

	if callCount.Load() < 1 {
		t.Error("CleanupDeadConsumers should have been called")
	}
}

func TestCleanupLoop_Success(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		cleanupFn: func(ctx context.Context, idle time.Duration) error {
			callCount.Add(1)
			if idle != 5*time.Minute {
				return errors.New("unexpected idle timeout")
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	cfg := testConfig()
	cfg.Redis.CleanupInterval = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.cleanupLoop(ctx))

	if callCount.Load() < 1 {
		t.Error("CleanupDeadConsumers should have been called")
	}
}

// --- refreshLoop tests ---

func TestRefreshLoop_Basic(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		refreshFn: func(ctx context.Context) (int, error) {
			if callCount.Add(1) == 1 {
				return 2, nil // 2 new streams
			}
			<-ctx.Done()
			return 0, ctx.Err()
		},
	}

	cfg := testConfig()
	cfg.Redis.Stream = "" // multi-stream mode
	cfg.Pipeline.RefreshInterval = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.refreshLoop(ctx))

	if callCount.Load() < 1 {
		t.Error("RefreshStreams was not called")
	}
}

func TestRefreshLoop_Error(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		refreshFn: func(ctx context.Context) (int, error) {
			if callCount.Add(1) >= 2 {
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 0, errors.New("refresh error")
		},
	}

	cfg := testConfig()
	cfg.Redis.Stream = ""
	cfg.Pipeline.RefreshInterval = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.refreshLoop(ctx))

	if callCount.Load() < 1 {
		t.Error("RefreshStreams should have been called")
	}
}

func TestRefreshLoop_NoNewStreams(t *testing.T) {
	var callCount atomic.Int32
	r := &mockRedis{
		refreshFn: func(ctx context.Context) (int, error) {
			if callCount.Add(1) >= 2 {
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 0, nil // no new streams
		},
	}

	cfg := testConfig()
	cfg.Redis.Stream = ""
	cfg.Pipeline.RefreshInterval = 1 * time.Millisecond
	hp, err := New(r, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	checkLoopExit(t, hp.refreshLoop(ctx))

	if callCount.Load() < 1 {
		t.Error("RefreshStreams should have been called")
	}
}

// --- Run() multi-stream mode test ---

func TestRun_MultiStreamMode(t *testing.T) {
	cfg := testConfig()
	cfg.Redis.Stream = "" // multi-stream
	cfg.Pipeline.RefreshInterval = 1 * time.Millisecond

	hp, err := New(&mockRedis{}, &mockPublisher{}, cfg, log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	runErr := hp.Run(ctx)
	if !errors.Is(runErr, context.Canceled) {
		t.Errorf("Run() error = %v; want context.Canceled", runErr)
	}
}

// --- handleAck lifecycle cancellation drop test ---

func TestHandleAck_DropDuringShutdown(t *testing.T) {
	r := &mockRedis{
		ackAndDeleteFn: func(_ context.Context, _ []string, _ string) error {
			t.Error("AckAndDeleteBatch should not be called during shutdown")
			return nil
		},
	}

	hp, err := New(r, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Cancel lifecycle so handleAck drops the ACK via lifecycleCtx.Done()
	hp.lifecycleCancel()

	// This should drop the ACK, not process it
	hp.handleAck(message.AckMessage{IDs: []string{"dropped"}, Stream: "s", Ack: true})

	// Brief wait to ensure no goroutine was spawned
	time.Sleep(50 * time.Millisecond)
}

// --- flushACKs with AckAndDelete error ---

func TestFlushACKs_AckAndDeleteError(t *testing.T) {
	r := &mockRedis{
		ackAndDeleteFn: func(_ context.Context, _ []string, _ string) error {
			return errors.New("redis error")
		},
	}

	hp, err := New(r, &mockPublisher{}, testConfig(), log.New())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer closeHotPath(t, hp)

	// Should not panic — just logs error
	hp.flushACKs("s", &pendingACK{ackIDs: []string{"x"}})
}
