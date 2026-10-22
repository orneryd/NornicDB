package main

import (
	"strings"
	"testing"
)

func TestCredentialConfiguredURLErrors(t *testing.T) {
	const endpoint="https://synthetic-user:synthetic-password-871429@example.invalid/%zz?api_key=synthetic-query-317"
	for _,operation:=range []func(string)error{validateURL,checkHealth,createSampleData} {
		err:=operation(endpoint)
		if err==nil{t.Fatal("malformed configured endpoint accepted")}
		for _,secret:=range []string{"synthetic-user","synthetic-password-871429","synthetic-query-317"} {
			if strings.Contains(err.Error(),secret){t.Error("configured endpoint error contains credentials; capture withheld")}
		}
	}
}
