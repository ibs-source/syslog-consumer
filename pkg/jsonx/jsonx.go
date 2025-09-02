// Package jsonx provides thin wrappers around encoding/json and some fast-path helpers.
package jsonx

// Thin wrapper to centralize JSON usage and allow future drop-in acceleration.
// Currently uses the Go stdlib to avoid platform/toolchain issues.

import (
	stdjson "encoding/json"
)

// Marshal encodes v into JSON using the standard library.
func Marshal(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}

// Unmarshal decodes JSON data into v using the standard library.
func Unmarshal(data []byte, v any) error {
	return stdjson.Unmarshal(data, v)
}

// GetTopLevelString returns the top-level string value for a key if it exists and is a string.
// Stdlib implementation for maximum compatibility.
func GetTopLevelString(data []byte, key string) (string, bool) {
	var m map[string]any
	if err := stdjson.Unmarshal(data, &m); err != nil {
		return "", false
	}
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// ReplaceTopLevelKey replaces or inserts a top-level key with an arbitrary JSON value (raw is JSON).
// raw must be valid JSON encoding (object, array, string, number, true, false, null).
// Returns updated JSON bytes or original if replacement fails.
// Stdlib implementation for maximum compatibility.
func ReplaceTopLevelKey(data []byte, key string, raw []byte) ([]byte, bool) {
	// First try parsing as an object
	var obj map[string]any
	if err := stdjson.Unmarshal(data, &obj); err == nil {
		// Decode raw into interface{}
		var v any
		if err := stdjson.Unmarshal(raw, &v); err != nil {
			return data, false
		}
		obj[key] = v
		out, err := stdjson.Marshal(obj)
		if err != nil {
			return data, false
		}
		return out, true
	}

	// Fallback: parse original generically, then create an object embedding original under "payload"
	var generic any
	if err := stdjson.Unmarshal(data, &generic); err != nil {
		return data, false
	}
	var v any
	if err := stdjson.Unmarshal(raw, &v); err != nil {
		return data, false
	}
	outObj := map[string]any{
		"payload": generic,
		key:       v,
	}
	out, err := stdjson.Marshal(outObj)
	if err != nil {
		return data, false
	}
	return out, true
}

// IsLikelyJSONBytes checks if data appears to be a JSON value (cheap heuristic).
func IsLikelyJSONBytes(b []byte) bool {
	i := 0
	for i < len(b) {
		switch b[i] {
		case ' ', '\n', '\r', '\t':
			i++
		default:
			goto CHECK
		}
	}
CHECK:
	if i >= len(b) {
		return false
	}
	switch b[i] {
	case '{', '[', '"', 't', 'f', 'n':
		return true
	default:
		return b[i] >= '0' && b[i] <= '9'
	}
}

// IsLikelyJSONString checks if string appears to be a JSON value (cheap heuristic).
func IsLikelyJSONString(s string) bool {
	i := 0
	n := len(s)
	for i < n {
		switch s[i] {
		case ' ', '\n', '\r', '\t':
			i++
		default:
			goto CHECK
		}
	}
CHECK:
	if i >= n {
		return false
	}
	switch s[i] {
	case '{', '[', '"', 't', 'f', 'n':
		return true
	default:
		return s[i] >= '0' && s[i] <= '9'
	}
}
