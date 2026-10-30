package multidb

import (
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

func localizedError(message localization.Message, cause error) error {
	return nornicerrors.NewLocalized(string(message.ID), message, cause)
}
