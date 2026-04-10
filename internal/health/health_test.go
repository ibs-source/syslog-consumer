package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type mockPinger struct {
	err error
}

func (m *mockPinger) Ping(_ context.Context) error {
	return m.err
}

type mockMQTT struct {
	connected bool
}

func (m *mockMQTT) IsConnected() bool {
	return m.connected
}

// healthzCase describes one /healthz test scenario.
type healthzCase struct {
	pinger     Pinger
	mqtt       ConnectionChecker
	name       string
	wantStatus string
	wantRedis  string
	wantMQTT   string
	wantCode   int
}

func TestHealthz(t *testing.T) {
	cases := []healthzCase{
		{
			name:       "AllOK",
			pinger:     &mockPinger{},
			mqtt:       &mockMQTT{connected: true},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantRedis:  "ok",
			wantMQTT:   "ok",
		},
		{
			name:       "RedisDegraded",
			pinger:     &mockPinger{err: errors.New("connection refused")},
			mqtt:       &mockMQTT{connected: true},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "degraded",
			wantRedis:  "connection refused",
			wantMQTT:   "ok",
		},
		{
			name:       "MQTTDisconnected",
			pinger:     &mockPinger{},
			mqtt:       &mockMQTT{connected: false},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "degraded",
			wantRedis:  "ok",
			wantMQTT:   "disconnected",
		},
		{
			name:       "NilMQTT",
			pinger:     &mockPinger{},
			mqtt:       nil,
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantRedis:  "ok",
			wantMQTT:   "ok",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { checkHealthz(t, &tc) })
	}
}

func checkHealthz(t *testing.T, tc *healthzCase) {
	t.Helper()

	srv := NewServer(":0", tc.pinger, tc.mqtt, 2*time.Second, 5*time.Second)

	req := httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != tc.wantCode {
		t.Fatalf("status = %d; want %d", rec.Code, tc.wantCode)
	}

	var resp healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != tc.wantStatus {
		t.Errorf("status = %q; want %q", resp.Status, tc.wantStatus)
	}
	if resp.Redis != tc.wantRedis {
		t.Errorf("redis = %q; want %q", resp.Redis, tc.wantRedis)
	}
	if resp.MQTT != tc.wantMQTT {
		t.Errorf("mqtt = %q; want %q", resp.MQTT, tc.wantMQTT)
	}
}

func TestHealthz_ContentType(t *testing.T) {
	srv := NewServer(":0", &mockPinger{}, &mockMQTT{connected: true}, 2*time.Second, 5*time.Second)

	req := httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", ct)
	}
}

func TestDebugVars(t *testing.T) {
	srv := NewServer(":0", &mockPinger{}, &mockMQTT{connected: true}, 2*time.Second, 5*time.Second)

	req := httptest.NewRequest(http.MethodGet, "/debug/vars", http.NoBody)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" && ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q; want application/json", ct)
	}
}

func TestListenAndServe_InvalidAddr(t *testing.T) {
	srv := NewServer("invalid-addr-no-port", &mockPinger{}, &mockMQTT{connected: true}, 2*time.Second, 5*time.Second)
	err := srv.ListenAndServe()
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestShutdown(t *testing.T) {
	srv := NewServer(":0", &mockPinger{}, &mockMQTT{connected: true}, 2*time.Second, 5*time.Second)

	// Start the server in background
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("ListenAndServe(): %v", err)
		}
	}()

	// Shutdown should succeed
	err := srv.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
