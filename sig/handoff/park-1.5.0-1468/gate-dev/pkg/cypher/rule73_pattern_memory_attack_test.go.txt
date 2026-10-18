package cypher

import (
	"context"
	"fmt"
	"testing"
)

func TestRule73PatternMemoryAttack(t *testing.T){
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0,TransactionMemoryLimits{})
	e,_:=rule73ResourceFixture(t)
	ConfigureTransactionMemory(0,TransactionMemoryLimits{TransactionMax:64<<10})
	for index,q:=range rule73ResourceQueries(){t.Run(fmt.Sprint(index),func(t *testing.T){
		r,err:=e.Execute(context.Background(),q,nil)
		var rows interface{}
		if r!=nil{rows=r.Rows}
		t.Logf("R73_MEMORY query=%q budget=65536 rows=%v error=%v",q,rows,err)
		if err!=nil{t.Errorf("bound result exceeded 64 KiB: %v",err)}
	})}
}
