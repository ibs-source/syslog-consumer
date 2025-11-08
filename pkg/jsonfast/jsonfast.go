/*
Package jsonfast offers a minimal JSON builder optimized for low-allocation encoding paths.
*/
package jsonfast

// Package jsonfast provides a tiny, allocation-aware JSON builder optimized for fixed schemas.

import "time"

// Builder is a minimal JSON builder that operates on a reusable byte slice.
// It avoids allocations by appending directly into the buffer.
// Not a fully general-purpose JSON writer; tailored for known field sets.
type Builder struct {
	buf    []byte
	opened bool
	first  bool
}

// New creates a new builder with initial capacity.
func New(capacity int) *Builder {
	if capacity <= 0 {
		capacity = 256
	}
	return &Builder{
		buf:    make([]byte, 0, capacity),
		opened: false,
		first:  true,
	}
}

// Reset clears the builder for reuse.
func (b *Builder) Reset() {
	b.buf = b.buf[:0]
	b.opened = false
	b.first = true
}

// Bytes returns the underlying buffer (do not modify after use).
func (b *Builder) Bytes() []byte {
	return b.buf
}

// BeginObject starts a JSON object.
func (b *Builder) BeginObject() {
	b.buf = append(b.buf, '{')
	b.opened = true
	b.first = true
}

// EndObject ends a JSON object.
func (b *Builder) EndObject() {
	b.buf = append(b.buf, '}')
	b.opened = false
}

// AddStringField adds a "name":"value" string field with escaping.
func (b *Builder) AddStringField(name, value string) {
	b.sep()
	b.buf = append(b.buf, '"')
	b.buf = append(b.buf, name...)
	b.buf = append(b.buf, '"', ':', '"')
	b.escapeString(value)
	b.buf = append(b.buf, '"')
}

// AddRawJSONField adds a "name":<raw json> field without escaping.
// The value must be valid JSON.
func (b *Builder) AddRawJSONField(name string, rawJSON []byte) {
	b.sep()
	b.buf = append(b.buf, '"')
	b.buf = append(b.buf, name...)
	b.buf = append(b.buf, '"', ':')
	b.buf = append(b.buf, rawJSON...)
}

// AddIntField adds a "name":int field.
func (b *Builder) AddIntField(name string, v int) {
	b.sep()
	b.buf = append(b.buf, '"')
	b.buf = append(b.buf, name...)
	b.buf = append(b.buf, '"', ':')
	b.buf = append(b.buf, itoa(v)...)
}

// AddNestedStringMapField adds a "name":{"key1":{"k":"v"},"key2":{...}} field.
// Specifically handles map[string]map[string]string as found in RFC5424 structured data.
func (b *Builder) AddNestedStringMapField(name string, m map[string]map[string]string) {
	if len(m) == 0 {
		return
	}
	b.sep()
	b.buf = append(b.buf, '"')
	b.buf = append(b.buf, name...)
	b.buf = append(b.buf, '"', ':', '{')

	firstOuter := true
	for outerKey, innerMap := range m {
		if !firstOuter {
			b.buf = append(b.buf, ',')
		}
		firstOuter = false

		// Outer key
		b.buf = append(b.buf, '"')
		b.escapeString(outerKey)
		b.buf = append(b.buf, '"', ':', '{')

		firstInner := true
		for innerKey, innerVal := range innerMap {
			if !firstInner {
				b.buf = append(b.buf, ',')
			}
			firstInner = false

			// Inner key-value
			b.buf = append(b.buf, '"')
			b.escapeString(innerKey)
			b.buf = append(b.buf, '"', ':', '"')
			b.escapeString(innerVal)
			b.buf = append(b.buf, '"')
		}

		b.buf = append(b.buf, '}')
	}

	b.buf = append(b.buf, '}')
}

func (b *Builder) sep() {
	if !b.opened {
		b.BeginObject()
		return
	}
	if b.first {
		b.first = false
		return
	}
	b.buf = append(b.buf, ',')
}

// escapeString escapes JSON special characters.
func (b *Builder) escapeString(s string) {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', '"':
			b.buf = append(b.buf, '\\', c)
		case '\b':
			b.buf = append(b.buf, '\\', 'b')
		case '\f':
			b.buf = append(b.buf, '\\', 'f')
		case '\n':
			b.buf = append(b.buf, '\\', 'n')
		case '\r':
			b.buf = append(b.buf, '\\', 'r')
		case '\t':
			b.buf = append(b.buf, '\\', 't')
		default:
			// Control characters (0x00..0x1f) need escaping
			if c < 0x20 {
				// \u00XX
				b.buf = append(b.buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0x0f])
			} else {
				b.buf = append(b.buf, c)
			}
		}
	}
}

// AddTimeRFC3339Field adds a "name":"RFC3339" field without using time.Format.
func (b *Builder) AddTimeRFC3339Field(name string, t time.Time) {
	b.sep()
	b.buf = append(b.buf, '"')
	b.buf = append(b.buf, name...)
	b.buf = append(b.buf, '"', ':', '"')
	// Use UTC for deterministic formatting
	t = t.UTC()
	year, month, day := t.Date()
	hour, minute, sec := t.Clock()
	// YYYY-MM-DD
	b.append4(year)
	b.buf = append(b.buf, '-')
	b.append2(int(month))
	b.buf = append(b.buf, '-')
	b.append2(day)
	// T
	b.buf = append(b.buf, 'T')
	// HH:MM:SS
	b.append2(hour)
	b.buf = append(b.buf, ':')
	b.append2(minute)
	b.buf = append(b.buf, ':')
	b.append2(sec)
	// Z
	b.buf = append(b.buf, 'Z')
	b.buf = append(b.buf, '"')
}

func (b *Builder) append2(v int) {
	b.buf = append(b.buf, byte('0'+(v/10)%10), byte('0'+v%10))
}

func (b *Builder) append4(v int) {
	b.buf = append(b.buf,
		byte('0'+(v/1000)%10),
		byte('0'+(v/100)%10),
		byte('0'+(v/10)%10),
		byte('0'+v%10),
	)
}

// itoa converts a small int to ascii without allocation.
func itoa(x int) []byte {
	if x == 0 {
		return []byte{'0'}
	}
	var tmp [20]byte
	i := len(tmp)
	neg := x < 0
	u := uint64(x)
	if neg {
		u = uint64(-x)
	}
	for u > 0 {
		i--
		tmp[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		tmp[i] = '-'
	}
	return tmp[i:]
}

var hex = "0123456789abcdef"
