//go:build linux

// Package runtimex provides optional CPU affinity helpers (best-effort).
// For now, Linux implementation is a no-op to avoid build-time dependency
// on specific golang.org/x/sys APIs. This preserves portability and allows
// enabling real affinity later without changing call sites.
package runtimex

// AffinitySpec describes the desired CPU set for the process or thread.
type AffinitySpec struct {
	CPUSet []int // CPU indices to allow; kept for API compatibility
}

// ApplyProcessAffinity is a no-op on Linux in this build.
func ApplyProcessAffinity(_ AffinitySpec) error {
	return nil
}

// PinCurrentThreadToCPU is a no-op on Linux in this build.
func PinCurrentThreadToCPU(_ int) error {
	return nil
}
