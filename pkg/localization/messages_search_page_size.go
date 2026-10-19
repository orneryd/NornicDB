package localization

import "strconv"

// SearchPageSizeExceeded identifies a continuation page above the server limit.
func SearchPageSizeExceeded(max int) Message {
	return Message{
		ID:       "search.page_size_exceeded",
		Fallback: "page_size_exceeded: search continuation n exceeds max_page_size=" + strconv.Itoa(max),
		Data:     map[string]any{"MaxPageSize": max},
	}
}
