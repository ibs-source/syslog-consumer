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

// Pinger abstracts the ability to check a connection's liveness.
type Pinger interface {
	Ping(ctx context.Context) error
}

// ConnectionChecker reports whether an outbound connection is alive.
type ConnectionChecker interface {
	IsConnected() bool
}

// Server provides /healthz and /debug/vars endpoints.
type Server struct {
	httpServer  *http.Server
	redis       Pinger
	mqtt        ConnectionChecker
	pingTimeout time.Duration
}

// NewServer creates a health server listening on the given address (e.g. ":9980").
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

// ListenAndServe starts serving HTTP requests. It blocks until the server
// is shut down or encounters a fatal error.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("health server listen: %w", err)
	}
	return s.httpServer.Serve(ln)
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

type healthResponse struct {
	Status string `json:"status"`
	Redis  string `json:"redis"`
	MQTT   string `json:"mqtt"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.pingTimeout)
	defer cancel()

	resp := healthResponse{Status: "ok", Redis: "ok", MQTT: "ok"}
	statusCode := http.StatusOK

	if err := s.redis.Ping(ctx); err != nil {
		resp.Status = "degraded"
		resp.Redis = err.Error()
		statusCode = http.StatusServiceUnavailable
	}

	if s.mqtt != nil && !s.mqtt.IsConnected() {
		resp.Status = "degraded"
		resp.MQTT = "disconnected"
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	data, err := json.Marshal(resp)
	if err != nil {
		slog.Error("health: marshal response", "error", err)
		return
	}
	if _, err = w.Write(data); err != nil {
		slog.Error("health: write response", "error", err)
	}
}
