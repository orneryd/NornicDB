package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeCompositeReservedKeywordsAreASCII(t *testing.T) {
	previous := config.GetParserType()

	t.Cleanup(func() { config.SetParserType(previous) })
	config.SetParserType("nornic")
	for _, operation := range []string{"CREATE", "ALTER"} {
		for _, token := range []string{"UſER", "PAſſWORD", "USERx", "PASSWORDx", "uSeR", "pAsSwOrD"} {
			t.Run(operation+"/"+token, func(t *testing.T) {
				proof := newConstituentCredentialProof(t)
				cfg := multidb.DefaultConfig()
				cfg.RemoteEngineFactory = proof.factory
				cfg.RemoteCredentialEncryptionKey = "synthetic-ascii-keyword-key"
				manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), cfg)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				engine, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				executor := NewStorageExecutor(engine)
				executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
				prefix := "CREATE COMPOSITE DATABASE ascii_keyword "
				if operation == "ALTER" {
					_, err = executor.Execute(t.Context(), prefix+"ALIAS seed FOR DATABASE nornic", nil)
					require.NoError(t, err)
					prefix = "ALTER COMPOSITE DATABASE ascii_keyword ADD "
				}
				user, password := "USER", "PASSWORD"
				invalid := token != "uSeR" && token != "pAsSwOrD"
				if token == "UſER" || token == "uSeR" || token == "USERx" {
					user = token
				} else {
					password = token
				}
				_, err = executor.Execute(t.Context(), prefix+"ALIAS `rſK` FOR DATABASE nornic AT 'http://127.0.0.1:1' "+user+" 'uſK' "+password+" 'synthetic-paſK' ACCESS read", nil)
				if invalid {
					require.Error(t, err, "non-ASCII reserved token accepted")
					refs, readErr := manager.GetCompositeConstituents("ascii_keyword")
					if operation == "CREATE" {
						require.Error(t, readErr)
					} else {
						require.NoError(t, readErr)
						require.Len(t, refs, 1)
						require.Equal(t, "seed", refs[0].Alias)
					}
				} else {
					require.NoError(t, err)
					refs, readErr := manager.GetCompositeConstituents("ascii_keyword")
					require.NoError(t, readErr)
					require.Equal(t, "rſK", refs[len(refs)-1].Alias)
					proof.assertConnection(manager, "ascii_keyword", multidb.ConstituentRef{Alias: "rſK", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://127.0.0.1:1", AuthMode: "user_password", User: "uſK", Password: "synthetic-paſK"})
				}
			})
		}
	}
}

func TestAdministrationRoutingReservedKeywordsAreASCII(t *testing.T) {
	for _, query := range []string{"CREATE UſER ordinary", "SHOW UſERS", "ALTER UſER ordinary SET PASSWORD 'synthetic'"} {
		require.Equal(t, "nornic", StatementDatabase("nornic", query), query)
	}
	require.Equal(t, "system", StatementDatabase("nornic", "cReAtE uSeR `uſK`"))
}
