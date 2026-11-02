package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestHTTPTransactionMemoryOverrideOwnership(t *testing.T) {
	for _, name := range []string{"inherit", "later_database_open", "remove", "remove_one", "transaction_only", "database_only", "explicit_unlimited", "equal_value_add", "equal_value_remove"} {
		t.Run(name, func(t *testing.T) {
			cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{})
			t.Cleanup(func() {
				cypher.SetDatabaseTransactionMemoryLimits("nornic", cypher.DatabaseTransactionMemoryLimits{})
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{})
			})
			server, client, token := startMemoryHTTPServer(t)
			queries := 0
			query := func(setting string) {
				queries++
				status, body, _ := client.post("/db/nornic/tx/commit", fmt.Sprintf("UNWIND range(1, 200000) AS x RETURN size(collect(x)) AS generation_%d", queries))
				require.Equal(t, http.StatusOK, status)
				if setting == "" {
					require.Empty(t, body.Errors)
					require.NotEmpty(t, body.Results)
				} else {
					require.Len(t, body.Errors, 1)
					require.Contains(t, body.Errors[0].Message, setting)
				}
			}
			put := func(overrides map[string]string) {
				response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config", map[string]any{"overrides": overrides}, token)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			}
			// Establish the ordinary cached protocol executor before configuration changes.
			query("")
			switch name {
			case "inherit":
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{TransactionMax: 1 << 20})
				query("db.memory.transaction.max")
			case "later_database_open":
				config := nornicdb.DefaultConfig()
				config.Memory.DecayEnabled = false
				config.Memory.AutoLinksEnabled = false
				config.Memory.TransactionMax = 1 << 20
				database, err := nornicdb.Open(t.TempDir(), config)
				require.NoError(t, err)
				defer database.Close()
				query("db.memory.transaction.max")
			case "remove":
				put(map[string]string{"db.memory.transaction.max": "1m"})
				query("db.memory.transaction.max")
				put(nil)
				query("") // Removing a finite override must take effect before another reload.
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{TransactionMax: 1 << 20})
				query("db.memory.transaction.max")
			case "remove_one":
				put(map[string]string{"db.memory.transaction.max": "1m", "db.memory.transaction.total.max": "64m"})
				query("db.memory.transaction.max")
				put(map[string]string{"db.memory.transaction.total.max": "64m"})
				query("")
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{TransactionMax: 1 << 20})
				query("db.memory.transaction.max")
			case "transaction_only":
				put(map[string]string{"db.memory.transaction.max": "64m"})
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{DatabaseTotalMax: 1 << 20})
				query("db.memory.transaction.total.max")
			case "database_only":
				put(map[string]string{"db.memory.transaction.total.max": "64m"})
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{TransactionMax: 1 << 20})
				query("db.memory.transaction.max")
			case "explicit_unlimited":
				put(map[string]string{"db.memory.transaction.max": "0", "db.memory.transaction.total.max": "0"})
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{TransactionMax: 1 << 20, DatabaseTotalMax: 1 << 20})
				query("")
			case "equal_value_add":
				put(map[string]string{"db.memory.transaction.max": "0"})
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{TransactionMax: 1 << 20})
				query("")
			case "equal_value_remove":
				put(map[string]string{"db.memory.transaction.max": "0"})
				put(nil)
				cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{TransactionMax: 1 << 20})
				query("db.memory.transaction.max")
			}
		})
	}
}
