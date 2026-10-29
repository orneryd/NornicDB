package storage

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// retainedError is an error kept in a compiled plan as immutable data. Its
// one field is a string: the error's chain, encoded level by level, outermost
// first. It holds no map, slice, pointer, interface or function, so there is
// nothing in it a caller can reach or change. err is the one rebuild: it
// decodes a brand-new error, with new causes, data and lists, on every call.
// The zero value holds no error.
type retainedError struct {
	encoded string
}

// The encoding. Each level is retainedLocalizedLevel with the code, message
// ID, fallback, data and plural count of a localized error, or
// retainedTextLevel with any other error's text. A field is its length, ':'
// and its bytes; a value is a tag byte and its fields:
//
//	0 nil            s string         b bool        i int
//	I int64          f float64        S []string    l []any
//	m map[string]any e error (its encoding, as one field)
//
// A value of any other type is kept as its text. The compile errors a plan
// retains carry only string data.
const (
	retainedLocalizedLevel = 'L'
	retainedTextLevel      = 'T'
)

// retainError encodes err's chain; nil encodes nothing.
func retainError(err error) retainedError {
	var encoded []byte
	for err != nil {
		if reported, ok := err.(contractError); ok {
			// Already encoded, from this level down.
			encoded = append(encoded, reported.encoded...)
			break
		}
		localized, ok := err.(*localization.LocalizedError)
		if !ok || localized == nil {
			encoded = appendRetainedField(append(encoded, retainedTextLevel), err.Error())
			err = errors.Unwrap(err)
			continue
		}
		encoded = append(encoded, retainedLocalizedLevel)
		encoded = appendRetainedField(encoded, localized.Code)
		encoded = appendRetainedField(encoded, string(localized.Message.ID))
		encoded = appendRetainedField(encoded, localized.Message.Fallback)
		if localized.Message.Data == nil {
			encoded = append(encoded, '0')
		} else {
			encoded = appendRetainedValue(encoded, localized.Message.Data)
		}
		encoded = appendRetainedValue(encoded, localized.Message.PluralCount)
		err = localized.Cause
	}
	return retainedError{encoded: string(encoded)}
}

func appendRetainedField(encoded []byte, field string) []byte {
	encoded = strconv.AppendInt(encoded, int64(len(field)), 10)
	return append(append(encoded, ':'), field...)
}

func appendRetainedValue(encoded []byte, value any) []byte {
	switch typed := value.(type) {
	case nil:
		return append(encoded, '0')
	case string:
		return appendRetainedField(append(encoded, 's'), typed)
	case bool:
		return appendRetainedField(append(encoded, 'b'), strconv.FormatBool(typed))
	case int:
		return appendRetainedField(append(encoded, 'i'), strconv.Itoa(typed))
	case int64:
		return appendRetainedField(append(encoded, 'I'), strconv.FormatInt(typed, 10))
	case float64:
		return appendRetainedField(append(encoded, 'f'), strconv.FormatFloat(typed, 'g', -1, 64))
	case []string:
		encoded = appendRetainedField(append(encoded, 'S'), strconv.Itoa(len(typed)))
		for _, item := range typed {
			encoded = appendRetainedField(encoded, item)
		}
		return encoded
	case []any:
		encoded = appendRetainedField(append(encoded, 'l'), strconv.Itoa(len(typed)))
		for _, item := range typed {
			encoded = appendRetainedValue(encoded, item)
		}
		return encoded
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		encoded = appendRetainedField(append(encoded, 'm'), strconv.Itoa(len(keys)))
		for _, key := range keys {
			encoded = appendRetainedValue(appendRetainedField(encoded, key), typed[key])
		}
		return encoded
	case error:
		return appendRetainedField(append(encoded, 'e'), retainError(typed).encoded)
	}
	return appendRetainedField(append(encoded, 's'), fmt.Sprint(value))
}

// err rebuilds the retained error: a new error, with new causes, decoded from
// the record.
func (retained retainedError) err() error {
	reader := retainedReader{encoded: retained.encoded}
	return reader.error()
}

// retainedReader decodes one record; it lives only for one rebuild.
type retainedReader struct {
	encoded string
	at      int
}

// error decodes the level at the reader, then its causes.
func (reader *retainedReader) error() error {
	if reader.at == len(reader.encoded) {
		return nil
	}
	if reader.tag() == retainedTextLevel {
		text := reader.field()
		return &retainedTextError{text: text, cause: reader.error()}
	}
	code := reader.field()
	message := localization.Message{ID: localization.MessageID(reader.field())}
	message.Fallback = reader.field()
	message.Data, _ = reader.value().(map[string]any)
	message.PluralCount = reader.value()
	return localization.NewLocalizedError(code, message, reader.error())
}

func (reader *retainedReader) tag() byte {
	tag := reader.encoded[reader.at]
	reader.at++
	return tag
}

func (reader *retainedReader) field() string {
	colon := reader.at + strings.IndexByte(reader.encoded[reader.at:], ':')
	length, _ := strconv.Atoi(reader.encoded[reader.at:colon])
	reader.at = colon + 1 + length
	return reader.encoded[colon+1 : reader.at]
}

func (reader *retainedReader) count() int {
	count, _ := strconv.Atoi(reader.field())
	return count
}

func (reader *retainedReader) value() any {
	switch reader.tag() {
	case 's':
		return reader.field()
	case 'b':
		return reader.field() == "true"
	case 'i':
		value, _ := strconv.Atoi(reader.field())
		return value
	case 'I':
		value, _ := strconv.ParseInt(reader.field(), 10, 64)
		return value
	case 'f':
		value, _ := strconv.ParseFloat(reader.field(), 64)
		return value
	case 'S':
		list := make([]string, reader.count())
		for index := range list {
			list[index] = reader.field()
		}
		return list
	case 'l':
		list := make([]any, reader.count())
		for index := range list {
			list[index] = reader.value()
		}
		return list
	case 'm':
		count := reader.count()
		values := make(map[string]any, count)
		for ; count > 0; count-- {
			key := reader.field()
			values[key] = reader.value()
		}
		return values
	case 'e':
		nested := retainedReader{encoded: reader.field()}
		return nested.error()
	}
	return nil
}

// contractError is how a constraint contract reports an entry that does not
// compile, to every caller: the complete error, encoded, and nothing else. It
// has no exported field and refers to no memory a caller could change, so
// keeping, sharing or returning the same value again changes nothing anyone
// sees. Every accessor decodes a new copy with its own reader, whatever the
// plan's rebuild does: Error its text, Unwrap a new cause chain, and As a new
// error of the requested type at any level (a *localization.LocalizedError
// for the code, message and data).
type contractError struct {
	encoded string
}

// reportedContractError is err as a contract reports it: encoded once here,
// so nothing err refers to is reachable from what the caller receives.
func reportedContractError(err error) error {
	return contractError{encoded: retainError(err).encoded}
}

func (err contractError) decoded() error {
	reader := retainedReader{encoded: err.encoded}
	return reader.error()
}

func (err contractError) Error() string      { return err.decoded().Error() }
func (err contractError) Unwrap() error      { return errors.Unwrap(err.decoded()) }
func (err contractError) As(target any) bool { return errors.As(err.decoded(), target) }

// retainedTextError is a rebuilt error that was not localized: its text and
// its rebuilt cause, in private fields.
type retainedTextError struct {
	text  string
	cause error
}

func (err *retainedTextError) Error() string { return err.text }
func (err *retainedTextError) Unwrap() error { return err.cause }
