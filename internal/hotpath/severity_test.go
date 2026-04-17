package hotpath

import "testing"

func TestSeverityName(t *testing.T) {
	tests := []struct {
		name string
		want string
		raw  []byte
	}{
		// Fast path: single-digit ASCII
		{raw: []byte("0"), name: "0 EMERGENCY", want: "EMERGENCY"},
		{raw: []byte("1"), name: "1 ALERT", want: "ALERT"},
		{raw: []byte("2"), name: "2 CRITICAL", want: "CRITICAL"},
		{raw: []byte("3"), name: "3 ERROR", want: "ERROR"},
		{raw: []byte("4"), name: "4 WARNING", want: "WARNING"},
		{raw: []byte("5"), name: "5 NOTICE", want: "NOTICE"},
		{raw: []byte("6"), name: "6 INFO", want: "INFO"},
		{raw: []byte("7"), name: "7 DEBUG", want: "DEBUG"},

		// Slow path: multi-byte numeric strings
		{raw: []byte("00"), name: "multi-byte 0", want: "EMERGENCY"},
		{raw: []byte("07"), name: "multi-byte 7", want: "DEBUG"},

		// Out-of-range → default INFO
		{raw: []byte("8"), name: "8 out of range", want: "INFO"},
		{raw: []byte("9"), name: "9 out of range", want: "INFO"},
		{raw: []byte("-1"), name: "negative", want: "INFO"},
		{raw: []byte("99"), name: "large number", want: "INFO"},

		// Invalid input → default INFO
		{raw: []byte(""), name: "empty", want: "INFO"},
		{raw: []byte("abc"), name: "non-numeric", want: "INFO"},
		{raw: []byte("3.5"), name: "float", want: "INFO"},
		{raw: []byte(" "), name: "space", want: "INFO"},
		{raw: []byte("\x00"), name: "null bytes", want: "INFO"},
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
