// Author: Codex, NOR-180 localized status diagnostics, 2026-10-27.
package bolt

import (
	"errors"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

func (s *Session) sendRunQueryFailure(err error, query string) error {
	err = nornicerrors.NormalizeTerminalOutcome(err)
	code, message := nornicerrors.RenderStatusWithFallback(err, s.localize, boltResultStreamStatus)
	var oversized *resultstream.PageSizeExceededError
	if !errors.As(err, &oversized) {
		return s.sendRunFailureWithDetail(code, message, boltErrorDetail(err))
	}
	s.failedUntilReset = true
	return s.sendFailureWithMetadata(code, message, boltErrorDetail(err), map[string]any{
		"request_status": "page_size_exceeded", "max_page_size": int64(oversized.MaxPageSize), "http_code": int64(400),
	})
}
