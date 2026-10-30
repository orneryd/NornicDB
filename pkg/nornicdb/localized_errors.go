package nornicdb

import "github.com/orneryd/nornicdb/pkg/localization"

func localizedError(message localization.Message, cause error) error {
	return localization.NewLocalizedError(string(message.ID), message, cause)
}
