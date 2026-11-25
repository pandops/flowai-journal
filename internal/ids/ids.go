// Package ids validates FlowAI integer identifiers: positive 64-bit
// integers, decimal without sign or leading zeros.
package ids

import (
	"encoding/json"
	"strconv"
)

// ParseDecimal parses a decimal identifier token.
func ParseDecimal(s string) (int64, bool) {
	if s == "" || len(s) > 19 || s[0] == '0' {
		return 0, false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

// FromJSON validates a JSON-decoded identifier value. Only an integral
// JSON number in range is valid; decode with UseNumber so large values
// keep their digits.
func FromJSON(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		return ParseDecimal(n.String())
	case float64:
		if n != float64(int64(n)) || n < 1 || n > 9007199254740991 {
			return 0, false
		}
		return int64(n), true
	case int64:
		return n, n > 0
	case int:
		return int64(n), n > 0
	}
	return 0, false
}
