// Package main boots the syslog consumer, wiring configuration, logger, Redis, MQTT, and the stream processor.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/config"
	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/mqtt"
	core "github.com/ibs-source/syslog/consumer/golang/internal/ports"
	"github.com/ibs-source/syslog/consumer/golang/internal/processor"
	"github.com/ibs-source/syslog/consumer/golang/internal/redis"
	runtimex "github.com/ibs-source/syslog/consumer/golang/internal/runtime"
	"github.com/ibs-source/syslog/consumer/golang/pkg/circuitbreaker"
)

// Application represents the main application
type Application struct {
	config      *config.Config
	logger      core.Logger
	redisClient core.RedisClient
	mqttClient  core.MQTTClient
	processor   *processor.StreamProcessor
	healthSrv   *http.Server
	metrics     *domain.Metrics
	wg          sync.WaitGroup
}

func main() {
	os.Exit(run())
}

// run contains the program logic and returns an exit code.
// Using this pattern ensures defers run and avoids exit-after-defer lint issues.
func run() int {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to load configuration: %v\n", err)
		return 1
	}

	// Initialize logger
	logr, err := logger.NewLogrusLogger(cfg.App.LogLevel, cfg.App.LogFormat)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		return 1
	}

	// Create application
	app := &Application{
		config:  cfg,
		logger:  logr,
		metrics: domain.NewMetrics(),
	}

	// Start application
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.Start(ctx); err != nil {
		logr.Error("failed to start application", core.Field{Key: "error", Value: err})
		return 1
	}

	// Start metrics logger if debug mode
	if cfg.App.LogLevel == "debug" {
		app.wg.Add(1)
		go app.logMetrics(ctx)
	}

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logr.Info("received shutdown signal", core.Field{Key: "signal", Value: sig})

	// Cancel the main context to stop all goroutines
	cancel()

	// Shutdown application
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.App.ShutdownTimeout)
	defer shutdownCancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		logr.Error("failed to shutdown gracefully", core.Field{Key: "error", Value: err})
		return 1
	}

	logr.Info("application shutdown complete")
	return 0
}

// Start starts the application
func (app *Application) Start(ctx context.Context) error {
	app.logger.Info("starting application",
		core.Field{Key: "name", Value: app.config.App.Name},
		core.Field{Key: "environment", Value: app.config.App.Environment},
	)

	// Best-effort CPU affinity (no-op on non-Linux). Does not change config precedence.
	app.applyCPUAffinityIfConfigured()

	// Initialize Redis client
	redisClient, err := redis.NewClient(app.config, app.logger)
	if err != nil {
		return fmt.Errorf("failed to create redis client: %w", err)
	}
	app.redisClient = redisClient

	// Wait until Redis is ready or context is canceled
	if err := app.waitForRedisReady(ctx); err != nil {
		return err
	}

	// Initialize MQTT client
	mqttClient, err := mqtt.NewClient(app.config, app.logger)
	if err != nil {
		return fmt.Errorf("failed to create mqtt client: %w", err)
	}
	app.mqttClient = mqttClient

	// Wait until MQTT is ready or context is canceled
	if err := app.waitForMQTTReady(ctx); err != nil {
		return err
	}

	// Create circuit breaker for MQTT publishing
	publishCB := app.makePublishCB()

	// Create stream processor
	app.processor = processor.NewStreamProcessor(
		app.config,
		app.redisClient,
		app.mqttClient,
		app.logger,
		app.metrics,
		publishCB,
		nil,
	)

	// Start stream processor
	if err := app.processor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start stream processor: %w", err)
	}

	// Start health check server
	if app.config.Health.Enabled {
		app.startHealthServer()
	}

	app.logger.Info("application started successfully")
	return nil
}

// applyCPUAffinityIfConfigured applies process CPU affinity if CPUAffinity is provided.
// It is a best-effort operation and logs a warning on failure. No-ops on non-Linux builds.
func (app *Application) applyCPUAffinityIfConfigured() {
	if len(app.config.Pipeline.CPUAffinity) == 0 {
		return
	}
	if err := runtimex.ApplyProcessAffinity(runtimex.AffinitySpec{CPUSet: app.config.Pipeline.CPUAffinity}); err != nil {
		app.logger.Warn("failed to apply CPU affinity (best-effort)", core.Field{Key: "error", Value: err})
		return
	}
	app.logger.Info("applied CPU affinity", core.Field{Key: "cpus", Value: app.config.Pipeline.CPUAffinity})
}

// makePublishCB constructs the circuit breaker for MQTT publishing using current config.
func (app *Application) makePublishCB() core.CircuitBreaker {
	return circuitbreaker.New(
		"mqtt-publish",
		app.config.CircuitBreaker.ErrorThreshold,
		app.config.CircuitBreaker.SuccessThreshold,
		app.config.CircuitBreaker.Timeout,
		app.config.CircuitBreaker.MaxConcurrentCalls,
		app.config.CircuitBreaker.RequestVolumeThreshold,
	)
}

// Shutdown shuts down the application gracefully
func (app *Application) Shutdown(ctx context.Context) error {
	app.logger.Info("shutting down application")

	// Stop stream processor
	if app.processor != nil {
		if err := app.processor.Stop(ctx); err != nil {
			app.logger.Error("failed to stop processor", core.Field{Key: "error", Value: err})
		}
	}

	// Shutdown health server
	if app.healthSrv != nil {
		if err := app.healthSrv.Shutdown(ctx); err != nil {
			app.logger.Error("failed to shutdown health server", core.Field{Key: "error", Value: err})
		}
	}

	// Disconnect MQTT client using configured timeout (prefer MQTT.WriteTimeout, cap by App.ShutdownTimeout)
	if app.mqttClient != nil {
		to := app.config.MQTT.WriteTimeout
		if to <= 0 || to > app.config.App.ShutdownTimeout {
			to = app.config.App.ShutdownTimeout
		}
		app.mqttClient.Disconnect(to)
	}

	// Close Redis client
	if app.redisClient != nil {
		if err := app.redisClient.Close(); err != nil {
			app.logger.Error("failed to close redis client", core.Field{Key: "error", Value: err})
		}
	}

	// Wait for all goroutines
	app.wg.Wait()

	return nil
}

// startHealthServer starts the health check HTTP server
func (app *Application) startHealthServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/healthz", app.healthHandler)
	mux.HandleFunc("/heathz", app.healthHandler)
	mux.HandleFunc("/ready", app.readyHandler)
	mux.HandleFunc("/live", app.liveHandler)

	app.healthSrv = &http.Server{
		Addr:         fmt.Sprintf(":%d", app.config.Health.Port),
		Handler:      mux,
		ReadTimeout:  app.config.Health.ReadTimeout,
		WriteTimeout: app.config.Health.WriteTimeout,
	}

	app.wg.Add(1)
	go app.runHealthServer()
}

// runHealthServer runs the health server
func (app *Application) waitForRedisReady(ctx context.Context) error {
	for {
		redisCtx, redisCancel := context.WithTimeout(ctx, app.config.Health.RedisTimeout)
		err := app.redisClient.Ping(redisCtx)
		redisCancel()
		if err == nil {
			return nil
		}
		app.logger.Error("failed to connect to redis, will retry",
			core.Field{Key: "error", Value: err})
		select {
		case <-time.After(app.config.Redis.RetryInterval):
		case <-ctx.Done():
			return fmt.Errorf("context canceled before redis became ready: %w", ctx.Err())
		}
	}
}

func (app *Application) waitForMQTTReady(ctx context.Context) error {
	for {
		connectCtx, connectCancel := context.WithTimeout(ctx, app.config.MQTT.ConnectTimeout)
		err := app.mqttClient.Connect(connectCtx)
		connectCancel()
		if err == nil {
			return nil
		}
		app.logger.Warn("failed to connect to mqtt broker, will retry",
			core.Field{Key: "error", Value: err})
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return fmt.Errorf("context canceled before mqtt became ready: %w", ctx.Err())
		}
	}
}

func (app *Application) runHealthServer() {
	defer app.wg.Done()
	app.logger.Info("starting health server", core.Field{Key: "port", Value: app.config.Health.Port})

	err := app.healthSrv.ListenAndServe()
	if err == nil || err == http.ErrServerClosed {
		return
	}

	app.logger.Error("health server error", core.Field{Key: "error", Value: err})
}

// healthHandler handles health check requests
func (app *Application) healthHandler(w http.ResponseWriter, _ *http.Request) {
	health := app.checkHealth()

	if health.Healthy {
		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprintf(w, `{"status":"healthy","timestamp":"%s"}`, time.Now().Format(time.RFC3339)); err != nil {
			app.logger.Error("failed to write health response", core.Field{Key: "error", Value: err})
		}
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := fmt.Fprintf(w, `{"status":"unhealthy","message":"%s","timestamp":"%s"}`,
			health.Message, time.Now().Format(time.RFC3339)); err != nil {
			app.logger.Error("failed to write health response", core.Field{Key: "error", Value: err})
		}
	}
}

// readyHandler handles readiness check requests
func (app *Application) readyHandler(w http.ResponseWriter, _ *http.Request) {
	// Check if processor is running
	if app.processor != nil && app.processor.GetState() == "running" {
		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprintf(w, `{"status":"ready","timestamp":"%s"}`, time.Now().Format(time.RFC3339)); err != nil {
			app.logger.Error("failed to write ready response", core.Field{Key: "error", Value: err})
		}
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := fmt.Fprintf(w, `{"status":"not_ready","timestamp":"%s"}`, time.Now().Format(time.RFC3339)); err != nil {
			app.logger.Error("failed to write ready response", core.Field{Key: "error", Value: err})
		}
	}
}

// liveHandler handles liveness check requests
func (app *Application) liveHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprintf(w, `{"status":"alive","timestamp":"%s"}`, time.Now().Format(time.RFC3339)); err != nil {
		app.logger.Error("failed to write live response", core.Field{Key: "error", Value: err})
	}
}

// checkHealth performs health checks on all components
func (app *Application) checkHealth() core.HealthStatus {
	redisCtx, cancel := context.WithTimeout(context.Background(), app.config.Health.RedisTimeout)
	defer cancel()

	// Check Redis
	if err := app.redisClient.Ping(redisCtx); err != nil {
		return core.HealthStatus{
			Healthy: false,
			Message: fmt.Sprintf("redis health check failed: %v", err),
		}
	}

	// Check MQTT
	if !app.mqttClient.IsConnected() {
		return core.HealthStatus{
			Healthy: false,
			Message: "mqtt client not connected",
		}
	}

	// Check processor
	if app.processor.GetState() != "running" {
		return core.HealthStatus{
			Healthy: false,
			Message: fmt.Sprintf("processor not running (state: %s)", app.processor.GetState()),
		}
	}

	return core.HealthStatus{
		Healthy: true,
		Message: "all components healthy",
	}
}

// logMetrics periodically logs metrics to console when in debug mode
func (app *Application) logMetrics(ctx context.Context) {
	defer app.wg.Done()

	ticker := time.NewTicker(app.config.Metrics.CollectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			snapshot := app.metrics.Snapshot()
			app.logger.Debug("=== METRICS SNAPSHOT ===")
			app.logger.Debug("Messages",
				core.Field{Key: "received", Value: app.metrics.MessagesReceived.Load()},
				core.Field{Key: "published", Value: app.metrics.MessagesPublished.Load()},
				core.Field{Key: "acked", Value: app.metrics.MessagesAcked.Load()},
				core.Field{Key: "dropped", Value: app.metrics.MessagesDropped.Load()},
			)
			app.logger.Debug("Processing",
				core.Field{Key: "errors", Value: app.metrics.ProcessingErrors.Load()},
				core.Field{Key: "buffer_util", Value: app.metrics.BufferUtilization.Load()},
				core.Field{Key: "backpressure_drops", Value: app.metrics.BackpressureDropped.Load()},
			)
			app.logger.Debug("Performance",
				core.Field{Key: "throughput_rate", Value: snapshot.ThroughputRate},
				core.Field{Key: "publish_rate", Value: snapshot.PublishRate},
				core.Field{Key: "error_rate", Value: snapshot.ErrorRate},
				core.Field{Key: "active_workers", Value: snapshot.ActiveWorkers},
				core.Field{Key: "queue_depth", Value: snapshot.QueueDepth},
			)
			app.logger.Debug("Errors",
				core.Field{Key: "redis", Value: app.metrics.RedisErrors.Load()},
				core.Field{Key: "mqtt", Value: app.metrics.MQTTErrors.Load()},
			)
			app.logger.Debug("Latency (ns)",
				core.Field{Key: "processing", Value: app.metrics.ProcessingTimeNs.Load()},
				core.Field{Key: "publish", Value: app.metrics.PublishLatencyNs.Load()},
				core.Field{Key: "ack", Value: app.metrics.AckLatencyNs.Load()},
			)
			app.logger.Debug("=======================")
		case <-ctx.Done():
			return
		}
	}
}
