package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCredentialSettingsURLHTTPPublic(t *testing.T) {
	server,authenticator:=setupTestServer(t)
	token:=getAuthToken(t,authenticator,"admin")
	const secret="synthetic/url-token-871429"
	const apiKey="synthetic-opaque-key-923581"
	endpoint:=&url.URL{Scheme:"http",Host:"example.invalid",User:url.UserPassword(secret,""),RawQuery:"region=public&api_key="+url.QueryEscape(secret)}
	configured:=map[string]string{"db.nornic.search.rerank.api.url":endpoint.String(),"db.nornic.search.rerank.api.key":apiKey}
	response:=makeRequest(t,server,http.MethodPut,"/admin/databases/nornic/config",map[string]any{"overrides":configured},"Bearer "+token)
	require.Equal(t,http.StatusOK,response.Code)
	for _,method:=range []string{http.MethodPut,http.MethodGet} {
		if method==http.MethodGet {response=makeRequest(t,server,method,"/admin/databases/nornic/config",nil,"Bearer "+token)}
		require.Equal(t,http.StatusOK,response.Code)
		for _,value:=range []string{secret,url.QueryEscape(secret),apiKey,"enc:v1:"} {
			if strings.Contains(response.Body.String(),value){t.Error("HTTP settings response contains credential material; capture withheld")}
		}
	}
	for _,statement:=range []string{"SHOW SETTINGS", "CALL dbms.listConfig()"} {
		response=makeRequest(t,server,http.MethodPost,"/db/nornic/tx/commit",map[string]any{"statements":[]map[string]any{{"statement":statement}}},"Bearer "+token)
		require.Equal(t,http.StatusOK,response.Code)
		var body map[string]any
		require.NoError(t,json.Unmarshal(response.Body.Bytes(),&body))
		require.Empty(t,body["errors"])
		require.Contains(t,response.Body.String(),"db.nornic.search.rerank.api.url")
		for _,value:=range []string{secret,url.QueryEscape(secret),apiKey,"enc:v1:"} {
			if strings.Contains(response.Body.String(),value){t.Error("HTTP settings Cypher read contains credential material; capture withheld")}
		}
	}
	engine,err:=server.dbManager.GetStorage("system")
	require.NoError(t,err)
	node,err:=engine.GetNode(storage.NodeID("db_config:nornic"))
	require.NoError(t,err)
	var stored map[string]string
	require.NoError(t,json.Unmarshal([]byte(node.Properties["overrides"].(string)),&stored))
	require.Equal(t,configured,stored) // The configured credential store retains original values by design.
	require.Equal(t,configured,server.dbConfigStore.GetOverrides("nornic"))
}
