package bolt

import (
	"errors"

	"github.com/orneryd/nornicdb/pkg/resultstream"
)

func (s *Session) sendRunQueryFailure(err error, query string) error {
	code, message := mapBoltQueryErrorForQuery(err, query)
	var oversized *resultstream.PageSizeExceededError
	if !errors.As(err, &oversized) {
		return s.sendRunFailureWithDetail(code, message, boltErrorDetail(err))
	}
	s.failedUntilReset = true
	return s.sendFailureWithMetadata(code, message, boltErrorDetail(err), map[string]any{
		"request_status": "page_size_exceeded", "max_page_size": int64(oversized.MaxPageSize), "http_code": int64(400),
	})
}
