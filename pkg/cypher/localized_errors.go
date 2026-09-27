package cypher

import "github.com/orneryd/nornicdb/pkg/localization"

func localizedError(message localization.Message, cause error) error {
	return localization.NewLocalizedError(string(message.ID), message, cause)
}

// localizedStatusError is a localized client error with its Neo4j status
// code and conformance detail (Bolt and HTTP report the code).
func localizedStatusError(code, detail string, message localization.Message) error {
	return &classifiedCypherError{cause: localizedError(message, nil), code: code, detail: detail}
}
