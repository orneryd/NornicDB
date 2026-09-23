package cypher

import "strings"

// stripCypherComments removes line and block comments outside quoted text.
// Queries without comments are returned directly without allocating.
func stripCypherComments(query string) string {
	comment := firstCypherComment(query)
	if comment < 0 {
		return query
	}

	var output strings.Builder
	output.Grow(len(query))
	output.WriteString(query[:comment])
	for index := comment; index < len(query); {
		if index+1 < len(query) && query[index] == '/' && query[index+1] == '/' {
			index += 2
			for index < len(query) && query[index] != '\n' && query[index] != '\r' {
				index++
			}
			if index < len(query) {
				output.WriteByte(query[index])
				index++
			}
			continue
		}
		if index+1 < len(query) && query[index] == '/' && query[index+1] == '*' {
			output.WriteByte(' ')
			index += 2
			for index < len(query) {
				if index+1 < len(query) && query[index] == '*' && query[index+1] == '/' {
					index += 2
					break
				}
				if query[index] == '\n' || query[index] == '\r' {
					output.WriteByte(query[index])
				}
				index++
			}
			continue
		}
		quote := query[index]
		if quote == '\'' || quote == '"' || quote == '`' {
			end := skipCypherQuotedText(query, index, quote)
			output.WriteString(query[index:end])
			index = end
			continue
		}
		output.WriteByte(query[index])
		index++
	}
	return output.String()
}

func firstCypherComment(query string) int {
	for index := 0; index+1 < len(query); {
		quote := query[index]
		if quote == '\'' || quote == '"' || quote == '`' {
			index = skipCypherQuotedText(query, index, quote)
			continue
		}
		if query[index] == '/' && (query[index+1] == '/' || query[index+1] == '*') {
			return index
		}
		index++
	}
	return -1
}

func skipCypherQuotedText(query string, start int, quote byte) int {
	for index := start + 1; index < len(query); index++ {
		if query[index] == '\\' && quote != '`' && index+1 < len(query) {
			index++
			continue
		}
		if query[index] != quote {
			continue
		}
		if index+1 < len(query) && query[index+1] == quote {
			index++
			continue
		}
		return index + 1
	}
	return len(query)
}
