//go:build !linux

// Package runtimex provides optional CPU affinity helpers (best-effort).
// Non-Linux stub: APIs are defined as no-ops for portability.
package runtimex

// AffinitySpec describes the desired CPU set for the process or thread.
type AffinitySpec struct {
	CPUSet []int
}

// ApplyProcessAffinity is a no-op on non-Linux builds.
func ApplyProcessAffinity(_ AffinitySpec) error { return nil }

// PinCurrentThreadToCPU is a no-op on non-Linux builds.
func PinCurrentThreadToCPU(_ int) error { return nil }
