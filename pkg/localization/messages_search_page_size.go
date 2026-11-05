package localization

import "strconv"

// MessageSearchPageSizeExceeded identifies a continuation page above the limit.
const MessageSearchPageSizeExceeded MessageID = "search.page_size_exceeded"

// SearchPageSizeExceeded identifies a continuation page above the server limit.
func SearchPageSizeExceeded(max int) Message {
	return Message{
		ID:       MessageSearchPageSizeExceeded,
		Fallback: "page_size_exceeded: search continuation n exceeds max_page_size=" + strconv.Itoa(max),
		Data:     map[string]any{"MaxPageSize": max},
	}
}
