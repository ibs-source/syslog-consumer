package hotpath

import "strconv"

const severityInfo = "INFO"

var severityNames = [8]string{
	"EMERGENCY", "ALERT", "CRITICAL", "ERROR",
	"WARNING", "NOTICE", severityInfo, "DEBUG",
}

// severityName converts raw JSON severity bytes (0–7) to a name.
// Fast path for single-digit values avoids strconv overhead.
func severityName(raw []byte) string {
	if len(raw) == 1 && raw[0] >= '0' && raw[0] <= '7' {
		return severityNames[raw[0]-'0']
	}
	n, err := strconv.Atoi(string(raw))
	if err != nil || n < 0 || n >= len(severityNames) {
		return severityInfo
	}
	return severityNames[n]
}
