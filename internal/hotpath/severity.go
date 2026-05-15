package hotpath

import "strconv"

// Syslog severity indices (RFC 5424 §6.2.1).
const (
	sevEmergency = iota
	sevAlert
	sevCritical
	sevError
	sevWarning
	sevNotice
	sevInfo
	sevDebug
	sevCount
)

// Syslog severity display names.
const (
	severityEmergency = "EMERGENCY"
	severityAlert     = "ALERT"
	severityCritical  = "CRITICAL"
	severityError     = "ERROR"
	severityWarning   = "WARNING"
	severityNotice    = "NOTICE"
	severityInfo      = "INFO"
	severityDebug     = "DEBUG"
)

var severityNames = [sevCount]string{
	sevEmergency: severityEmergency,
	sevAlert:     severityAlert,
	sevCritical:  severityCritical,
	sevError:     severityError,
	sevWarning:   severityWarning,
	sevNotice:    severityNotice,
	sevInfo:      severityInfo,
	sevDebug:     severityDebug,
}

// severityName converts raw JSON severity bytes (0–7) to a name.
// Fast path for single-digit values avoids strconv overhead.
func severityName(raw []byte) string {
	if len(raw) == 1 && raw[0] >= '0' && raw[0] <= '7' {
		return severityNames[raw[0]-'0']
	}
	n, err := strconv.Atoi(string(raw))
	if err != nil || n < 0 || n >= sevCount {
		return severityInfo
	}
	return severityNames[n]
}
