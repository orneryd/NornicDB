package storage

import "github.com/orneryd/nornicdb/pkg/localization"

func localizedError(message localization.Message, cause error) error {
	return localization.NewLocalizedError(string(message.ID), message, cause)
}

type schemaAdmissionError struct {
	code  string
	cause error
}

func (err *schemaAdmissionError) Error() string         { return err.cause.Error() }
func (err *schemaAdmissionError) Unwrap() error         { return err.cause }
func (err *schemaAdmissionError) BoltErrorCode() string { return err.code }

func newSchemaAdmissionError(code string, message localization.Message) error {
	return &schemaAdmissionError{code: "Neo.ClientError.Schema." + code, cause: localizedError(message, nil)}
}
