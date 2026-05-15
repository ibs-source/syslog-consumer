// Package health provides an HTTP health check endpoint and expvar metrics server.
package health

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Pinger is the subset of a backend client needed for liveness checks.
type Pinger interface {
	Ping(ctx context.Context) error
}

// ConnectionChecker reports whether an outbound connection is currently open.
type ConnectionChecker interface {
	IsConnected() bool
}

// Server exposes /healthz and /debug/vars.
type Server struct {
	httpServer  *http.Server
	redis       Pinger
	mqtt        ConnectionChecker
	pingTimeout time.Duration
}

// NewServer wires the health endpoint; addr follows the net.Listen "host:port"
// form (e.g. ":9980"). mqttChecker may be nil to skip the MQTT probe.
func NewServer(
	addr string,
	redisPinger Pinger,
	mqttChecker ConnectionChecker,
	pingTimeout time.Duration,
	readHeaderTimeout time.Duration,
) *Server {
	s := &Server{
		redis:       redisPinger,
		mqtt:        mqttChecker,
		pingTimeout: pingTimeout,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.Handle("GET /debug/vars", expvar.Handler())

	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return s
}

// ListenAndServe blocks until the server is shut down or fails.
func (s *Server) ListenAndServe(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("health server listen: %w", err)
	}
	return s.httpServer.Serve(ln)
}

// Shutdown waits for in-flight handlers until ctx fires.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

const (
	statusOK           = "ok"
	statusDegraded     = "degraded"
	statusDisconnected = "disconnected"
)

type healthResponse struct {
	Status string `json:"status"`
	Redis  string `json:"redis"`
	MQTT   string `json:"mqtt"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.pingTimeout)
	defer cancel()

	resp := healthResponse{Status: statusOK, Redis: statusOK, MQTT: statusOK}
	statusCode := http.StatusOK

	if err := s.redis.Ping(ctx); err != nil {
		resp.Status = statusDegraded
		resp.Redis = err.Error()
		statusCode = http.StatusServiceUnavailable
	}

	if s.mqtt != nil && !s.mqtt.IsConnected() {
		resp.Status = statusDegraded
		resp.MQTT = statusDisconnected
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	data, err := json.Marshal(resp)
	if err != nil {
		slog.ErrorContext(ctx, "health: marshal response", "error", err)
		return
	}
	if _, err = w.Write(data); err != nil {
		slog.ErrorContext(ctx, "health: write response", "error", err)
	}
}
