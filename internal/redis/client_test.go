package redis

import (
	"errors"
	"testing"

	"github.com/ibs-source/syslog-consumer/internal/log"
	goredis "github.com/redis/go-redis/v9"
)

// Note: Most functions in Client require a live Redis connection (NewClient, ReadBatch,
// ClaimIdle, AckAndDelete, ensureGroup) and are better tested via integration tests.
// These functions interact with Redis streams and cannot be easily mocked without
// significant complexity or using a Redis mock server.
//
// The core business logic is straightforward:
// - NewClient: Creates Redis client and ensures consumer group exists
// - ReadBatch: Reads messages using XREADGROUP
// - ClaimIdle: Claims idle messages using XPENDING and XCLAIM
// - AckAndDelete: Acknowledges and deletes messages using XACK and XDEL
// - Close: Closes the Redis connection
//
// These are best verified through integration tests with an actual Redis instance.
// Pure unit-testable functions (isNoGroupError, handleReadError) are tested below.

func TestIsNoGroupError(t *testing.T) {
	tests := []struct {
		err  error
		name string
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "NOGROUP error from Redis",
			err: errors.New(
				"NOGROUP No such key 'syslog-stream' or consumer group " +
					"'group-syslog-stream' in XREADGROUP with GROUP option"),
			want: true,
		},
		{
			name: "NOGROUP prefix only",
			err:  errors.New("NOGROUP"),
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "NOGROUP in middle of message should not match",
			err:  errors.New("some wrapper: NOGROUP something"),
			want: false,
		},
		{
			name: "BUSYGROUP error",
			err:  errors.New("BUSYGROUP Consumer Group name already exists"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNoGroupError(tt.err); got != tt.want {
				t.Errorf("isNoGroupError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// --- handleReadError tests (no live Redis needed) ---

func TestHandleReadError_RedisNil(t *testing.T) {
	c := &Client{
		log:     log.New(),
		streams: []string{"s1"},
	}
	err := c.handleReadError(t.Context(), goredis.Nil)
	if err != nil {
		t.Errorf("handleReadError(redis.Nil) = %v; want nil", err)
	}
}

func TestHandleReadError_GenericError(t *testing.T) {
	c := &Client{
		log:     log.New(),
		streams: []string{"s1"},
	}
	origErr := errors.New("connection refused")
	err := c.handleReadError(t.Context(), origErr)
	if err == nil {
		t.Fatal("expected error for generic Redis error")
	}
	if !errors.Is(err, origErr) {
		t.Errorf("expected wrapped original error, got %v", err)
	}
}

// --- Close tests ---

func TestClose_NilRDB(t *testing.T) {
	c := &Client{} // rdb is nil
	if err := c.Close(); err != nil {
		t.Errorf("Close() = %v; want nil for nil rdb", err)
	}
}
