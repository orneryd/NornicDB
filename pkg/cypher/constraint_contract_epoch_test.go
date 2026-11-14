package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/temporalguide"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestConstraintContractEpochGroupingIsLocalToEntry(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		for _, relationship := range []bool{false, true} {
			for _, width := range []int{1, 3} {
				t.Run(fmt.Sprintf("relationship=%t/width=%d", relationship, width), func(t *testing.T) {
					exec, engine := newConstraintGapExecutor(t)
					variable, pattern, create, match := "n", "(n:Period)", "CREATE (:Period $props)", "MATCH (n:Period)"
					if relationship {
						variable, pattern, create, match = "r", "()-[r:PERIOD]-()", "MATCH (a:Endpoint {id:1}),(b:Endpoint {id:2}) CREATE (a)-[:PERIOD $props]->(b)", "MATCH ()-[r:PERIOD]->()"
						_, err := exec.Execute(t.Context(), "CREATE (:Endpoint {id:1}),(:Endpoint {id:2})", nil)
						require.NoError(t, err)
					}
					refs := make([]string, width)
					for i := range refs {
						refs[i] = fmt.Sprintf("%s.k%d", variable, i)
					}
					ddl := fmt.Sprintf(`CREATE CONSTRAINT periods FOR %s REQUIRE {
      %s.id IS UNIQUE
      temporal.epoch(%s.starts_at,%s.ends_at) GROUP BY %s
      %s.status IN ['active','inactive']
     }`, pattern, variable, variable, variable, strings.Join(refs, ","), variable)
					_, err := exec.Execute(t.Context(), ddl, nil)
					require.NoError(t, err)
					contracts, err := exec.Execute(t.Context(), "SHOW CONSTRAINT CONTRACTS", nil)
					require.NoError(t, err)
					require.Len(t, contracts.Rows, 1)
					require.EqualValues(t, 3, contracts.Rows[0][3])
					require.EqualValues(t, 2, contracts.Rows[0][4])
					require.EqualValues(t, 1, contracts.Rows[0][5])
					require.Contains(t, contracts.Rows[0][6], "GROUP BY")
					var temporal *storage.Constraint
					for _, c := range engine.GetSchema().GetAllConstraints() {
						if c.Type == storage.ConstraintTemporal {
							copy := c
							temporal = &copy
						}
					}
					require.NotNil(t, temporal, "epoch must compile to the native constraint, not an opaque expression")
					require.Len(t, temporal.Properties, width+2)
					props := func(id, group, start, end, status string) map[string]interface{} {
						p := map[string]interface{}{"id": id, "starts_at": start, "ends_at": end, "status": status}
						for i := 0; i < width; i++ {
							p[fmt.Sprintf("k%d", i)] = "same"
						}
						p[fmt.Sprintf("k%d", width-1)] = group
						return p
					}
					write := func(p map[string]interface{}) error {
						_, err := exec.Execute(t.Context(), create, map[string]interface{}{"props": p})
						return err
					}
					require.NoError(t, write(props("first", "same", "2026-01-01", "2026-01-10", "active")))
					require.Error(t, write(props("overlap", "same", "2026-01-05", "2026-01-15", "active")))
					require.NoError(t, write(props("adjacent", "same", "2026-01-10", "2026-01-20", "inactive")))
					require.NoError(t, write(props("other", "different", "2026-01-05", "2026-01-15", "active")))
					require.Error(t, write(props("invalid", "different", "2026-02-01", "2026-02-10", "paused")), "following boolean entry must still enforce its own rule")
					_, err = exec.Execute(t.Context(), fmt.Sprintf("%s WHERE %s.id='adjacent' SET %s.starts_at='2026-01-05'", match, variable, variable), nil)
					require.Error(t, err)
					rows, err := exec.Execute(t.Context(), fmt.Sprintf("%s RETURN %s.id AS id ORDER BY id", match, variable), nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"adjacent"}, {"first"}, {"other"}}, rows.Rows)
					recreated, _ := newConstraintGapExecutor(t)
					_, err = recreated.Execute(t.Context(), contracts.Rows[0][6].(string), nil)
					require.NoError(t, err, "SHOW definition must round-trip")
				})
			}
		}
	})
}

func TestConstraintContractEpochRejectsInvalidGroupingWithoutEffects(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		for _, ddl := range []string{
			`CREATE CONSTRAINT bad FOR ()-[r:PERIOD]-() REQUIRE { temporal.epoch(r.starts,r.ends) }`,
			`CREATE CONSTRAINT bad FOR ()-[r:PERIOD]-() REQUIRE { temporal.epoch(r.starts,r.ends) GROUP BY other.id }`,
			`CREATE CONSTRAINT bad FOR ()-[r:PERIOD]-() REQUIRE { temporal.epoch(r.starts,r.ends) } GROUP BY r.id`,
			`CREATE CONSTRAINT bad FOR ()-[r:PERIOD]-() REQUIRE { temporal.epoch(r.starts,r.ends) GROUP BY r.id; r.hours > 0 }; RETURN 1`,
		} {
			exec, engine := newConstraintGapExecutor(t)
			_, err := exec.Execute(t.Context(), ddl, nil)
			require.Error(t, err, ddl)
			require.Empty(t, engine.GetSchema().GetAllConstraints())
			contracts, err := exec.Execute(t.Context(), "SHOW CONSTRAINT CONTRACTS", nil)
			require.NoError(t, err)
			require.Empty(t, contracts.Rows)
		}
	})
}

// This is the complete relationship contract shown in the ledger guide.
func TestEmploymentContractWithEpoch(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		ddl := temporalguide.Example(t, "managing-constraints", "employment-contract")
		_, err := exec.Execute(t.Context(), ddl, nil)
		require.NoError(t, err)
		shown, err := exec.Execute(t.Context(), "SHOW CONSTRAINT CONTRACTS", nil)
		require.NoError(t, err)
		require.Len(t, shown.Rows, 1)
		require.EqualValues(t, 9, shown.Rows[0][3])
		require.EqualValues(t, 5, shown.Rows[0][4])
		require.EqualValues(t, 4, shown.Rows[0][5])
		_, err = exec.Execute(t.Context(), `CREATE (:Person {id:'p-1',tenant:'t1'}),(:Company {id:'c-1',tenant:'t1'})`, nil)
		require.NoError(t, err)
		create := `MATCH (a:Person {id:'p-1'}),(b:Company {id:'c-1'}) CREATE (a)-[:WORKS_AT $props]->(b)`
		props := map[string]interface{}{"id": "w-1", "tenant": "t1", "externalId": "rel-1", "employeeId": "p-1", "startedAt": "2026-01-01", "endedAt": "2026-02-01", "role": "Engineer", "status": "active", "hoursPerWeek": int64(40)}
		_, err = exec.Execute(t.Context(), create, map[string]interface{}{"props": props})
		require.NoError(t, err)
		props["id"], props["externalId"], props["startedAt"], props["endedAt"] = "w-2", "rel-2", "2026-01-15", "2026-03-01"
		_, err = exec.Execute(t.Context(), create, map[string]interface{}{"props": props})
		require.Error(t, err, "overlapping epochs for one employee must fail")
		props["startedAt"] = "2026-02-01"
		_, err = exec.Execute(t.Context(), create, map[string]interface{}{"props": props})
		require.NoError(t, err, "adjacent employment epochs must succeed")
		props["id"], props["externalId"], props["startedAt"], props["endedAt"], props["hoursPerWeek"] = "w-3", "rel-3", "2026-03-01", "2026-04-01", int64(0)
		_, err = exec.Execute(t.Context(), create, map[string]interface{}{"props": props})
		require.ErrorContains(t, err, "r.hoursPerWeek > 0")
	})
}

func TestConstraintContractMultipleEpochEntriesKeepSeparateGroups(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		exec, engine := newConstraintGapExecutor(t)
		_, err := exec.Execute(t.Context(), `CREATE CONSTRAINT assignments FOR ()-[r:ASSIGNED]-() REQUIRE {
   temporal.epoch(r.starts,r.ends) GROUP BY r.assetId;
   temporal.epoch(r.starts,r.ends) GROUP BY r.employeeId;
   r.hours > 0
  }`, nil)
		require.NoError(t, err)
		require.Len(t, engine.GetSchema().GetAllConstraints(), 2)
		_, err = exec.Execute(t.Context(), `CREATE (:Endpoint {id:1}),(:Endpoint {id:2})`, nil)
		require.NoError(t, err)
		create := `MATCH (a:Endpoint {id:1}),(b:Endpoint {id:2}) CREATE (a)-[:ASSIGNED $props]->(b)`
		for i, tc := range []struct {
			asset, employee string
			accepted        bool
		}{
			{"a1", "e1", true}, {"a1", "e2", false}, {"a2", "e1", false}, {"a2", "e2", true},
		} {
			_, err = exec.Execute(t.Context(), create, map[string]interface{}{"props": map[string]interface{}{"assetId": tc.asset, "employeeId": tc.employee, "starts": "2026-01-01", "ends": "2026-02-01", "hours": int64(40)}})
			if tc.accepted {
				require.NoError(t, err, "case %d", i)
			} else {
				require.Error(t, err, "case %d must conflict with its own epoch group", i)
			}
		}
		result, err := exec.Execute(t.Context(), `MATCH ()-[r:ASSIGNED]->() RETURN r.assetId AS asset,r.employeeId AS employee ORDER BY asset`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a1", "e1"}, {"a2", "e2"}}, result.Rows)
	})
}
