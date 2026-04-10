// Package main starts the syslog consumer binary.
package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/health"
	"github.com/ibs-source/syslog-consumer/internal/hotpath"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/mqtt"
	"github.com/ibs-source/syslog-consumer/internal/redis"
)

func run() int {
	// Ensure GC tuning is set even when not running in the Docker container
	// where GOGC and GOMEMLIMIT are set as environment variables.
	// Skipped when the env var is already set.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(200)
	}
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(4 << 30) // 4 GiB
	}

	logger := log.New()
	logger.Info("Starting syslog consumer")

	cfg, err := loadAndLogConfig(logger)
	if err != nil {
		return 1
	}

	// Use a signal-aware context so that retry loops can be canceled
	// during startup if the user sends SIGTERM/SIGINT.
	initCtx, initCancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	redisClient, mqttPool, hp, err := initializeServices(initCtx, cfg, logger)
	initCancel()
	if err != nil {
		return 1
	}
	defer closeServices(redisClient, mqttPool, hp, logger)

	healthSrv := health.NewServer(
		cfg.Pipeline.HealthAddr,
		redisClient,
		mqttPool,
		cfg.Pipeline.HealthPingTimeout,
		cfg.Pipeline.HealthReadHeaderTimeout,
	)
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil {
			logger.Info("Health server stopped: %v", err)
		}
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Pipeline.ShutdownTimeout)
		defer cancel()
		if err := healthSrv.Shutdown(ctx); err != nil {
			logger.Error("Health server shutdown error: %v", err)
		}
	}()
	logger.Info("Health server listening on %s", cfg.Pipeline.HealthAddr)

	return runMainLoop(hp, cfg, logger)
}

func loadAndLogConfig(logger *log.Logger) (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		logger.Error("Failed to load configuration: %v", err)
		return nil, err
	}

	logger.SetLevel(cfg.Log.Level)
	logger.Info("Configuration loaded successfully")
	logger.Info("Redis: %s, Stream: %s", cfg.Redis.Address, cfg.Redis.Stream)
	logger.Info("MQTT: %s, Publish: %s, ACK: %s", cfg.MQTT.Broker, cfg.MQTT.PublishTopic, cfg.MQTT.AckTopic)
	logger.Info("Pipeline: Buffer=%d", cfg.Pipeline.BufferCapacity)
	return cfg, nil
}

func initializeServices(
	ctx context.Context, cfg *config.Config, logger *log.Logger,
) (*redis.Client, *mqtt.Pool, *hotpath.HotPath, error) {
	redisClient, err := redis.NewClient(&cfg.Redis, logger)
	if err != nil {
		logger.Error("Failed to create Redis client: %v", err)
		return nil, nil, nil, err
	}
	logger.Info("Connected to Redis")

	mqttPool, err := mqtt.NewPool(ctx, &cfg.MQTT, cfg.MQTT.PoolSize, logger)
	if err != nil {
		logger.Error("Failed to create MQTT pool: %v", err)
		if cerr := redisClient.Close(); cerr != nil {
			logger.Error("Error closing Redis client: %v", cerr)
		}
		return nil, nil, nil, err
	}
	logger.Info("Connected to MQTT broker with %d connections", cfg.MQTT.PoolSize)

	hp, err := hotpath.New(redisClient, mqttPool, cfg, logger)
	if err != nil {
		logger.Error("Failed to create hot path: %v", err)
		if cerr := mqttPool.Close(); cerr != nil {
			logger.Error("Error closing MQTT pool: %v", cerr)
		}
		if cerr := redisClient.Close(); cerr != nil {
			logger.Error("Error closing Redis client: %v", cerr)
		}
		return nil, nil, nil, err
	}
	return redisClient, mqttPool, hp, nil
}

func closeServices(redisClient *redis.Client, mqttPool *mqtt.Pool, hp *hotpath.HotPath, logger *log.Logger) {
	if err := hp.Close(); err != nil {
		logger.Error("Error closing hot path: %v", err)
	}
	if err := mqttPool.Close(); err != nil {
		logger.Error("Error closing MQTT pool: %v", err)
	}
	if err := redisClient.Close(); err != nil {
		logger.Error("Error closing Redis client: %v", err)
	}
}

func runMainLoop(hp *hotpath.HotPath, cfg *config.Config, logger *log.Logger) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// doneCh receives the return value of hp.Run, which blocks until all
	// internal goroutines have finished (wg.Wait).
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- hp.Run(ctx)
	}()

	logger.Info("Hot path orchestrator started")

	select {
	case sig := <-sigChan:
		logger.Info("Received signal %v, initiating graceful shutdown", sig)
		cancel()

		// Wait for hp.Run to complete (it calls wg.Wait internally)
		// so all goroutines have exited before we close resources.
		timer := time.NewTimer(cfg.Pipeline.ShutdownTimeout)
		defer timer.Stop()

		select {
		case err := <-doneCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("Hot path shutdown error: %v", err)
				return 1
			}
			logger.Info("Graceful shutdown completed")
			return 0
		case <-timer.C:
			logger.Error("Shutdown timeout exceeded")
			return 1
		}

	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Hot path error: %v", err)
			return 1
		}
		return 0
	}
}

func main() {
	// Keep main minimal to ensure defers in run() execute correctly.
	os.Exit(run())
}
