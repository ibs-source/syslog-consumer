// Package main starts the syslog consumer binary.
package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/hotpath"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/mqtt"
	"github.com/ibs-source/syslog-consumer/internal/redis"
)

func run() int {
	logger := log.New()
	logger.Info("Starting syslog consumer")

	cfg, err := loadAndLogConfig(logger)
	if err != nil {
		return 1
	}

	redisClient, mqttPool, hp, err := initializeServices(cfg, logger)
	if err != nil {
		return 1
	}
	defer closeServices(redisClient, mqttPool, hp, logger)

	return runMainLoop(hp, cfg, logger)
}

func loadAndLogConfig(logger *log.Logger) (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration: %v", err)
	}

	logger.Info("Configuration loaded successfully")
	logger.Info("Redis: %s, Stream: %s (dynamic groups: group-{stream})", cfg.Redis.Address, cfg.Redis.Stream)
	logger.Info("MQTT: %s, Publish: %s, ACK: %s", cfg.MQTT.Broker, cfg.MQTT.PublishTopic, cfg.MQTT.AckTopic)
	logger.Info("Pipeline: Buffer=%d", cfg.Pipeline.BufferCapacity)
	return cfg, nil
}

func initializeServices(cfg *config.Config, logger *log.Logger) (*redis.Client, *mqtt.Pool, *hotpath.HotPath, error) {
	redisClient, err := redis.NewClient(&cfg.Redis, logger)
	if err != nil {
		logger.Fatal("Failed to create Redis client: %v", err)
	}
	logger.Info("Connected to Redis")

	mqttPool, err := mqtt.NewPool(&cfg.MQTT, cfg.MQTT.PoolSize, logger)
	if err != nil {
		logger.Fatal("Failed to create MQTT pool: %v", err)
	}
	logger.Info("Connected to MQTT broker with %d connections", cfg.MQTT.PoolSize)

	hp := hotpath.New(redisClient, mqttPool, cfg, logger)
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

	errChan := make(chan error, 1)
	go func() {
		if err := hp.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- err
		}
	}()

	logger.Info("Hot path orchestrator started")

	select {
	case sig := <-sigChan:
		logger.Info("Received signal %v, initiating graceful shutdown", sig)
		cancel()
		return handleGracefulShutdown(cfg, logger)

	case err := <-errChan:
		logger.Error("Hot path error: %v", err)
		cancel()
		return 1
	}
}

func handleGracefulShutdown(cfg *config.Config, logger *log.Logger) int {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Pipeline.ShutdownTimeout)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond) // Give goroutines time to exit
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Graceful shutdown completed")
		logger.Info("Consumer stopped")
		return 0
	case <-shutdownCtx.Done():
		logger.Error("Shutdown timeout exceeded")
		return 1
	}
}

func main() {
	// Keep main minimal to ensure defers in run() execute correctly.
	os.Exit(run())
}
