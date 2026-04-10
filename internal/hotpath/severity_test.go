package hotpath

import "testing"

func TestSeverityName(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		// Fast path: single-digit ASCII
		{"0 EMERGENCY", []byte("0"), "EMERGENCY"},
		{"1 ALERT", []byte("1"), "ALERT"},
		{"2 CRITICAL", []byte("2"), "CRITICAL"},
		{"3 ERROR", []byte("3"), "ERROR"},
		{"4 WARNING", []byte("4"), "WARNING"},
		{"5 NOTICE", []byte("5"), "NOTICE"},
		{"6 INFO", []byte("6"), "INFO"},
		{"7 DEBUG", []byte("7"), "DEBUG"},

		// Slow path: multi-byte numeric strings
		{"multi-byte 0", []byte("00"), "EMERGENCY"},
		{"multi-byte 7", []byte("07"), "DEBUG"},

		// Out-of-range → default INFO
		{"8 out of range", []byte("8"), "INFO"},
		{"9 out of range", []byte("9"), "INFO"},
		{"negative", []byte("-1"), "INFO"},
		{"large number", []byte("99"), "INFO"},

		// Invalid input → default INFO
		{"empty", []byte(""), "INFO"},
		{"non-numeric", []byte("abc"), "INFO"},
		{"float", []byte("3.5"), "INFO"},
		{"space", []byte(" "), "INFO"},
		{"null bytes", []byte("\x00"), "INFO"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := severityName(tt.raw)
			if got != tt.want {
				t.Errorf("severityName(%q) = %q; want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestSeverityNames_Table(t *testing.T) {
	// Verify the severityNames array is correctly defined
	expected := [8]string{
		"EMERGENCY", "ALERT", "CRITICAL", "ERROR",
		"WARNING", "NOTICE", "INFO", "DEBUG",
	}
	if severityNames != expected {
		t.Errorf("severityNames = %v; want %v", severityNames, expected)
	}
}

func TestSeverityInfoConstant(t *testing.T) {
	if severityInfo != "INFO" {
		t.Errorf("severityInfo = %q; want %q", severityInfo, "INFO")
	}
}

var severitySink string

func BenchmarkSeverityName_FastPath(b *testing.B) {
	raw := []byte("6")
	b.ReportAllocs()
	for b.Loop() {
		severitySink = severityName(raw)
	}
}

func BenchmarkSeverityName_SlowPath(b *testing.B) {
	raw := []byte("06")
	b.ReportAllocs()
	for b.Loop() {
		severitySink = severityName(raw)
	}
}

func BenchmarkSeverityName_Invalid(b *testing.B) {
	raw := []byte("abc")
	b.ReportAllocs()
	for b.Loop() {
		severitySink = severityName(raw)
	}
}
