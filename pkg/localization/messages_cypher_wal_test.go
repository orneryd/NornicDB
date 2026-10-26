// Author: Codex, NOR-180 implementation lane, 2026-10-26.
package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestWALTerminalMessagesRetainIdentityAndRender(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	cause := errors.New("wal: closed")
	for _, test := range []struct {
		message Message
		english string
	}{
		{CypherTransactionsWALAbortFailed("tx-180", cause), "failed to write WAL tx abort for tx-180: wal: closed"},
		{CypherTransactionsWALCommitFailed("tx-180", cause), "transaction tx-180 committed, but failed to write WAL tx commit: wal: closed"},
	} {
		t.Run(string(test.message.ID), func(t *testing.T) {
			require.Equal(t, "tx-180", test.message.Data["TxID"])
			require.Equal(t, cause.Error(), test.message.Data["Cause"])
			failure := NewLocalizedError(string(test.message.ID), test.message, cause)
			require.ErrorIs(t, failure, cause)
			for _, tag := range []language.Tag{language.AmericanEnglish, language.EuropeanSpanish, language.MustParse("en-XA")} {
				text, actual, err := manager.Render(WithPreferences(context.Background(), tag), MessageRequest(test.message))
				require.NoError(t, err)
				require.Equal(t, tag, actual)
				require.Contains(t, text, "tx-180")
				require.Contains(t, text, cause.Error())
				if tag == language.AmericanEnglish {
					require.Equal(t, test.english, text)
				}
			}
		})
	}
}
