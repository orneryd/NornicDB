package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/contextslot"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/observability"
)

func withStatementCredentials(ctx context.Context, query string) context.Context {
	ctx, source := cyphertext.WithSource(ctx, query)
	if set := statementCredentials(ctx); set != nil {
		if set.Source == nil {
			set.Source = source
		}
		return ctx
	}
	ctx = contextslot.Ensure[cyphertext.CredentialSet](ctx)
	statementCredentials(ctx).Source = source
	// The canonical native DDL tokenizer retains decoded URL values before
	// legacy name compaction can erase their delimiters. No value is inferred.
	_, _, _ = tokenizeSource(source.Text, statementCredentials(ctx))
	return ctx
}

func statementCredentials(ctx context.Context) *cyphertext.CredentialSet {
	return contextslot.Get[cyphertext.CredentialSet](ctx)
}

type adminCredentialReader struct {
	source  string
	mapping *cyphertext.Source
	base    int
	spans   []sourceToken
	set     *cyphertext.CredentialSet
}

func (reader *adminCredentialReader) record(index int, value string, password bool) {
	if reader == nil || reader.set == nil || index < 0 || index >= len(reader.spans) {
		return
	}
	span := reader.spans[index]
	if reader.mapping != nil {
		reader.set.RecordSource(reader.mapping, reader.base+span.start, reader.base+span.end, value, password)
	} else {
		reader.set.Record(reader.source, span.start, span.end, value, password)
	}
}

// The value readers supply credentials. The only recovery rule is positional:
// any diagnostic for a source with PASSWORD has no statement-derived strings.
// Fixed, parameter-free diagnostic text comes only from the embedded catalog.
// This also covers errors before consumption and malformed/duplicate clauses.
// RedactError retains localized identity, causes and transport classification.
func redactAdminCommandError(query string, retErr *error, sets ...*cyphertext.CredentialSet) {
	if *retErr == nil {
		return
	}
	var source *cyphertext.Source
	for _, set := range sets {
		if set != nil && set.Source != nil && (set.Source.Original == query || set.Source.Text == query) {
			source = set.Source
			break
		}
	}
	if source == nil {
		source = cyphertext.NormalizeSource(query)
	}
	redactAdminCommandErrorSource(source, query, retErr, sets...)
}

func redactAdminCommandErrorSource(source *cyphertext.Source, query string, retErr *error, sets ...*cyphertext.CredentialSet) {
	if *retErr == nil {
		return
	}
	values := statementCredentialValues(query, sets...)
	if source.PasswordBoundary() >= 0 {
		*retErr = observability.RedactErrorDetails(*retErr)
		return
	}

	*retErr = observability.RedactError(*retErr, values...)
}

// QueryDiagnosticParameters projects query-owned credentials from parameters for
// display without changing execution inputs. It retains the request's source
// context and uses the existing pure composite preparation reader for explicit
// PASSWORD values. No catalog command is executed. For example, a password read
// from CREATE COMPOSITE DATABASE remains secret under an ordinary parameter key.
func QueryDiagnosticParameters(ctx context.Context, query string, params map[string]any) map[string]any {
	ctx, source := cyphertext.WithSource(ctx, query)
	var set *cyphertext.CredentialSet
	if source.PasswordBoundary() >= 0 {
		ctx = withStatementCredentials(ctx, query)
		set = statementCredentials(ctx)
		// A nil manager only disables the IF NOT EXISTS lookup; the same
		// preparation grammar reads values, without applying the command.
		_, _, _ = (&StorageExecutor{}).prepareCompositeCommand(ctx, query)
	}
	return observability.RedactValue(params, statementCredentialValues(query, set)...).(map[string]any)
}

func statementCredentialValues(query string, sets ...*cyphertext.CredentialSet) []string {
	values := observability.ContextCredentials(query)
	for _, set := range sets {
		if set == nil {
			continue
		}
		for _, span := range set.Spans {
			if span.Password {
				values = append(values, span.Raw, span.Value)
			}
			values = append(values, observability.ContextCredentials(span.Value)...)
		}
	}
	return values
}
