# internal/runtime

Best‑effort CPU affinity helpers. Current Linux build is a safe no‑op to preserve portability and avoid hard dependencies. Public API remains stable so a real affinity implementation can be introduced later without changing call sites.

Scope

- Define affinity intent via AffinitySpec { CPUSet []int }.
- Provide process/thread affinity helpers that are currently no‑ops on Linux and non‑Linux stubs.
- Keep API stable for future native implementations (e.g., using golang.org/x/sys).

API

- ApplyProcessAffinity(spec AffinitySpec) error
- PinCurrentThreadToCPU(cpu int) error

Behavior

- On Linux: functions return nil without modifying affinity (no‑op).
- On non‑Linux: stubbed behavior; also no‑op. Call sites should treat these as best‑effort.

Configuration (ENV, Flags, Description)
Precedence: defaults → environment variables → CLI flags.

CPU Affinity (Process-level)
| ENV | Flag | Default | Description |
|---|---|---|---|
| PIPELINE_CPU_AFFINITY | --pipeline-cpu-affinity | | Comma-separated CPU IDs for process affinity (best‑effort). |

Notes

- Affinity is applied early during application start (cmd/consumer) if a non‑empty CPU set is provided.
- The logger records success/failure; failures are warnings and do not block startup.
- Future implementations may add per‑thread pinning for worker pools, respecting the same AffinitySpec.
