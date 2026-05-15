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

	"github.com/ibs-source/syslog-consumer/internal/compress"
	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/health"
	"github.com/ibs-source/syslog-consumer/internal/hotpath"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/mqtt"
	"github.com/ibs-source/syslog-consumer/internal/redis"
)

func run(ctx context.Context) int {
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(200)
	}
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(2 << 30)
	}

	logger := log.New()
	logger.Infof(ctx, "Starting syslog consumer")

	cfg, err := loadAndLogConfig(ctx, logger)
	if err != nil {
		return 1
	}

	compress.Init(&cfg.Compress)

	initCtx, initCancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	redisClient, mqttPool, hp, err := initializeServices(initCtx, cfg, logger)
	initCancel()
	if err != nil {
		return 1
	}
	defer closeServices(ctx, redisClient, mqttPool, hp, logger)

	healthSrv := health.NewServer(
		cfg.Pipeline.HealthAddr,
		redisClient,
		mqttPool,
		cfg.Pipeline.HealthPingTimeout,
		cfg.Pipeline.HealthReadHeaderTimeout,
	)
	go func() {
		if err := healthSrv.ListenAndServe(ctx); err != nil {
			logger.Infof(ctx, "Health server stopped: %v", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, cfg.Pipeline.ShutdownTimeout)
		defer cancel()
		if err := healthSrv.Shutdown(shutdownCtx); err != nil {
			logger.Errorf(ctx, "Health server shutdown error: %v", err)
		}
	}()
	logger.Infof(ctx, "Health server listening on %s", cfg.Pipeline.HealthAddr)

	return runMainLoop(ctx, hp, cfg, logger)
}

func loadAndLogConfig(ctx context.Context, logger *log.Logger) (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		logger.Errorf(ctx, "Failed to load configuration: %v", err)
		return nil, err
	}

	logger.SetLevel(cfg.Log.Level)
	logger.Infof(ctx, "Configuration loaded successfully")
	logger.Infof(ctx, "Redis: %s, Stream: %s", cfg.Redis.Address, cfg.Redis.Stream)
	logger.Infof(ctx, "MQTT: %s, Publish: %s, ACK: %s", cfg.MQTT.Broker, cfg.MQTT.PublishTopic, cfg.MQTT.AckTopic)
	logger.Infof(ctx, "Pipeline: Buffer=%d", cfg.Pipeline.BufferCapacity)
	return cfg, nil
}

func initializeServices(
	ctx context.Context, cfg *config.Config, logger *log.Logger,
) (*redis.Client, *mqtt.Pool, *hotpath.HotPath, error) {
	redisClient, err := redis.NewClient(ctx, &cfg.Redis, logger)
	if err != nil {
		logger.Errorf(ctx, "Failed to create Redis client: %v", err)
		return nil, nil, nil, err
	}
	logger.Infof(ctx, "Connected to Redis")

	mqttPool, err := mqtt.NewPool(ctx, &cfg.MQTT, cfg.MQTT.PoolSize, logger)
	if err != nil {
		logger.Errorf(ctx, "Failed to create MQTT pool: %v", err)
		if cerr := redisClient.Close(); cerr != nil {
			logger.Errorf(ctx, "Error closing Redis client: %v", cerr)
		}
		return nil, nil, nil, err
	}
	logger.Infof(ctx, "Connected to MQTT broker with %d connections", cfg.MQTT.PoolSize)

	hp, err := hotpath.New(redisClient, mqttPool, cfg, logger)
	if err != nil {
		logger.Errorf(ctx, "Failed to create hot path: %v", err)
		if cerr := mqttPool.Close(); cerr != nil {
			logger.Errorf(ctx, "Error closing MQTT pool: %v", cerr)
		}
		if cerr := redisClient.Close(); cerr != nil {
			logger.Errorf(ctx, "Error closing Redis client: %v", cerr)
		}
		return nil, nil, nil, err
	}
	return redisClient, mqttPool, hp, nil
}

func closeServices(
	ctx context.Context, redisClient *redis.Client, mqttPool *mqtt.Pool, hp *hotpath.HotPath, logger *log.Logger,
) {
	if err := hp.Close(); err != nil {
		logger.Errorf(ctx, "Error closing hot path: %v", err)
	}
	if err := mqttPool.Close(); err != nil {
		logger.Errorf(ctx, "Error closing MQTT pool: %v", err)
	}
	if err := redisClient.Close(); err != nil {
		logger.Errorf(ctx, "Error closing Redis client: %v", err)
	}
}

func runMainLoop(ctx context.Context, hp *hotpath.HotPath, cfg *config.Config, logger *log.Logger) int {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- hp.Run(runCtx)
	}()

	logger.Infof(ctx, "Hot path orchestrator started")

	select {
	case sig := <-sigChan:
		logger.Infof(ctx, "Received signal %v, initiating graceful shutdown", sig)
		cancel()

		timer := time.NewTimer(cfg.Pipeline.ShutdownTimeout)
		defer timer.Stop()

		select {
		case err := <-doneCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Errorf(ctx, "Hot path shutdown error: %v", err)
				return 1
			}
			logger.Infof(ctx, "Graceful shutdown completed")
			return 0
		case <-timer.C:
			logger.Errorf(ctx, "Shutdown timeout exceeded")
			return 1
		}

	case err := <-doneCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Errorf(ctx, "Hot path error: %v", err)
			return 1
		}
		return 0
	}
}

func main() {
	os.Exit(run(context.Background()))
}
