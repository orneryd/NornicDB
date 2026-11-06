package cypher

import "errors"

// boundRuleError extracts the classification the original PR tests compare.
func boundRuleError(err error) (code, detail, message string) {
	if err == nil {
		return
	}
	message = err.Error()
	var classified interface {
		BoltErrorCode() string
	}
	if errors.As(err, &classified) {
		code = classified.BoltErrorCode()
	}
	var detailed interface{ BoltErrorDetail() string }
	if errors.As(err, &detailed) {
		detail = detailed.BoltErrorDetail()
	}
	return
}
