package cypher

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// LOAD CSV is a remote pass-through clause. Exercise its complete grammar
// without adding a CSV implementation to the local storage executor.
func TestFabricQueryStructureLoadCSVSeparators(t *testing.T) {
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("remote LOAD CSV oracle requires --ref neo-ref")
	}
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	target, err := url.Parse(uri)
	require.NoError(t, err)
	route, err := net.Dial("udp", target.Host)
	require.NoError(t, err)
	host := route.LocalAddr().(*net.UDPAddr).IP.String()
	require.NoError(t, route.Close())
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	csv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/headers" {
			fmt.Fprint(w, "v\n7\n")
		} else {
			fmt.Fprint(w, "7\n")
		}
	}))
	require.NoError(t, csv.Listener.Close())
	csv.Listener = listener
	csv.Start()
	defer csv.Close()
	source := "http://" + net.JoinHostPort(host, fmt.Sprint(listener.Addr().(*net.TCPAddr).Port))
	manager := base.dbManager.(*testDatabaseManagerAdapter).manager
	require.NoError(t, manager.AddConstituent("comp", multidb.ConstituentRef{Alias: "csv", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri}))
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, separator := range []string{" ", "  ", "\n", "\t", " /* UNION USE CALL */ ", " // UNION USE CALL\n"} {
				for _, headers := range []bool{false, true} {
					t.Run(fmt.Sprintf("headers_%v/%q", headers, separator), func(t *testing.T) {
						clause := "LOAD" + separator + "CSV FROM '" + source + "/plain' AS row RETURN toInteger(row[0]) AS k"
						if headers {
							clause = "LOAD" + separator + "CSV WITH" + separator + "HEADERS FROM '" + source + "/headers' AS row RETURN toInteger(row.v) AS k"
						}
						// The remote query itself has no nested CALL: Neo4j's single database is
						// both the source constituent and the direct reference execution target.
						fabricCompareGrammarCase(t, base, driver, "USE comp.csv "+clause, clause)
					})
				}
			}
			for _, word := range fabricGrammarWords() {
				for _, quoted := range []bool{false, true} {
					name := strings.ToLower(word)
					if quoted {
						name = "`" + name + "`"
					}
					t.Run(fmt.Sprintf("binding_%s/quoted_%v", word, quoted), func(t *testing.T) {
						clause := "LOAD CSV FROM '" + source + "/plain' AS " + name + " RETURN " + name + " AS k"
						fabricCompareGrammarCase(t, base, driver, "USE comp.csv "+clause, clause)
					})
				}
			}
		})
	}
}
