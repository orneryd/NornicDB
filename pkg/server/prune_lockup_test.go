package server

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// While the MVCC prune walks a large history, HTTP writes, explicit BEGIN and
// uncached Basic logins must answer at once, and so must every authenticated
// request while such a login is in flight. Before this fix they all waited for
// the whole walk: the prune held the lock that transactions take to register
// their snapshot, and a waiting login held the Authenticator's lock. Regression
// of 7b14b9133 (fix for GitHub #732); no NOR issue, recorded on NOR-405.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pruneLockupBound is far below the prune's walk over the seeded history,
// which takes seconds on the release head (plain build; see the race bound
// below).
const pruneLockupBound = time.Second

func TestPruneDoesNotHoldBackHTTPWritesBeginOrLogins(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "60s")
	s, authenticator := setupTestServerWithDatabaseConfig(t, func(c *nornicdb.Config) {
		c.Database.MVCCRetentionMaxVersions = 2
		c.Database.MVCCLifecycleEnabled = true
		c.Database.MVCCLifecycleCycleInterval = 0
	}, nil)
	engine, err := s.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	trigger, ok := s.db.GetBaseStorageForManager().(interface{ TriggerPruneNow(context.Context) error })
	require.True(t, ok, "the base engine runs lifecycle prunes")

	// History: every node has one archived version (walked, nothing to
	// delete) and a hot set has versions beyond retention.
	const total, hot = 100000, 2000
	ids := make([]storage.NodeID, total)
	for offset := 0; offset < total; offset += 5000 {
		nodes := make([]*storage.Node, 0, 5000)
		for i := offset; i < offset+5000; i++ {
			ids[i] = storage.NodeID(fmt.Sprintf("prune-lockup-%d", i))
			nodes = append(nodes, &storage.Node{ID: ids[i], Labels: []string{"PruneLockup"}, Properties: map[string]any{"v": 0}})
		}
		require.NoError(t, engine.BulkCreateNodes(context.Background(), nodes))
	}
	var seedWG sync.WaitGroup
	seedErr := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		seedWG.Add(1)
		go func(worker int) {
			defer seedWG.Done()
			for i := worker; i < total; i += 8 {
				rounds := 1
				if i < hot {
					rounds = 4
				}
				for v := 1; v <= rounds; v++ {
					if err := engine.UpdateNode(context.Background(), storage.NodeUpdateRequest{After: &storage.Node{ID: ids[i], Labels: []string{"PruneLockup"}, Properties: map[string]any{"v": v}}}); err != nil {
						seedErr <- err
						return
					}
				}
			}
		}(worker)
	}
	seedWG.Wait()
	close(seedErr)
	for err := range seedErr {
		require.NoError(t, err)
	}

	router := s.buildRouter()
	bearer := "Bearer " + getAuthToken(t, authenticator, "admin")
	uncachedBasic := "Basic " + base64.StdEncoding.EncodeToString([]byte("reader:password123"))
	request := func(method, path, auth string, statements ...string) (int, string) {
		var body bytes.Buffer
		list := make([]map[string]any, 0, len(statements))
		for _, statement := range statements {
			list = append(list, map[string]any{"statement": statement})
		}
		_ = json.NewEncoder(&body).Encode(map[string]any{"statements": list})
		req := httptest.NewRequest(method, path, &body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", auth)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		return recorder.Code, recorder.Body.String()
	}

	pruneStart := time.Now()
	pruneDone := make(chan error, 1)
	var pruneTook time.Duration
	go func() {
		err := trigger.TriggerPruneNow(context.Background())
		pruneTook = time.Since(pruneStart)
		pruneDone <- err
	}()
	time.Sleep(150 * time.Millisecond)

	type probe struct {
		name  string
		delay time.Duration
		want  int
		run   func() (int, string)
	}
	probes := []probe{
		{"HTTP write", 0, http.StatusOK, func() (int, string) {
			return request(http.MethodPost, "/db/nornic/tx/commit", bearer, "CREATE (:PruneLockupProbe {v: 1})")
		}},
		{"HTTP BEGIN", 0, http.StatusCreated, func() (int, string) {
			return request(http.MethodPost, "/db/nornic/tx", bearer)
		}},
		{"HTTP uncached Basic login", 0, http.StatusOK, func() (int, string) {
			return request(http.MethodPost, "/db/nornic/tx/commit", uncachedBasic, "RETURN 1")
		}},
		{"HTTP Bearer request during the login", 50 * time.Millisecond, http.StatusOK, func() (int, string) {
			return request(http.MethodPost, "/db/nornic/tx/commit", bearer, "RETURN 1")
		}},
	}
	type outcome struct {
		code    int
		body    string
		elapsed time.Duration
	}
	outcomes := make([]outcome, len(probes))
	var wg sync.WaitGroup
	for i, p := range probes {
		wg.Add(1)
		go func(i int, p probe) {
			defer wg.Done()
			time.Sleep(p.delay)
			start := time.Now()
			code, body := p.run()
			outcomes[i] = outcome{code, body, time.Since(start)}
		}(i, p)
	}
	wg.Wait()
	require.NoError(t, <-pruneDone)
	// The bound stays far below the prune's walk. Under the race detector the
	// whole server runs several times slower, so there it is a third of the
	// measured prune, and never less than the plain build's bound. On the
	// release head these requests finish only when the prune does.
	bound := pruneLockupBound
	if relative := pruneTook / 3; pluginTestRaceBuild && relative > bound {
		bound = relative
	}
	t.Logf("prune over %d nodes took %s; request bound %s", total, pruneTook.Round(time.Millisecond), bound)
	for i, p := range probes {
		o := outcomes[i]
		t.Logf("%s: status %d in %s", p.name, o.code, o.elapsed.Round(time.Millisecond))
		require.Equal(t, p.want, o.code, "%s: %s", p.name, o.body)
		require.Less(t, o.elapsed, bound, "%s waited for the prune", p.name)
	}
}
