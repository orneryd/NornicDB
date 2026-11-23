package cypher

import (
    "fmt"

    nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

func ei3Rows(r *ExecuteResult, err error) string {
    if err != nil {
        code, _ := nornicerrors.Neo4jStatus(err)
        return fmt.Sprintf("ERROR status=%s err=%v", code, err)
    }
    if r == nil {
        return "nil result"
    }
    return fmt.Sprintf("rows=%v", r.Rows)
}
