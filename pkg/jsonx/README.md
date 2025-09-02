# pkg/jsonx

Thin wrappers around the Go standard library’s `encoding/json` plus a few fast‑path helpers used by the processing pipeline to minimize allocations and enable zero‑copy flows where possible.

Goals

- Centralize JSON operations behind a small API for potential drop‑in acceleration later.
- Provide utility functions to detect/replace top‑level JSON keys and cheap “is JSON” heuristics.
- Keep compatibility high by defaulting to stdlib behavior.

API (selected)

- Marshal(v any) ([]byte, error)
- Unmarshal(data []byte, v any) error
- GetTopLevelString(data []byte, key string) (string, bool)
- ReplaceTopLevelKey(data []byte, key string, raw []byte) ([]byte, bool)
- IsLikelyJSONBytes(b []byte) bool
- IsLikelyJSONString(s string) bool

Behavior

- Marshal/Unmarshal delegate to the standard library for maximum compatibility.
- GetTopLevelString unmarshals to a `map[string]any]` and returns the string value if present.
- ReplaceTopLevelKey:
  - If `data` is a JSON object, unmarshals it, decodes `raw` into `any`, replaces/inserts the key, then re‑marshals.
  - If `data` is not an object, creates a new object like `{"payload": <data>, "<key>": <raw>}`.
  - Returns the original `data` with `false` on any failure.
- IsLikelyJSONBytes/IsLikelyJSONString are cheap heuristics (whitespace skip + first byte check) useful for fast‑paths.

Example

```go
// Detect if a field contains embedded JSON and normalize it into the top-level document.
payload := []byte(`{"struct_data":"{\"k\":\"v\"}","other":1}`)

if s, ok := jsonx.GetTopLevelString(payload, "struct_data"); ok && jsonx.IsLikelyJSONString(s) {
  if newPayload, replaced := jsonx.ReplaceTopLevelKey(payload, "struct_data", []byte(s)); replaced {
    payload = newPayload
  }
}
```

Notes

- These helpers trade completeness for speed on hot paths; they rely on stdlib parsing but keep fast pre‑checks and minimal allocations.
- When you need full JSON manipulation capabilities, prefer using the standard library or a dedicated JSON library higher in the stack.
