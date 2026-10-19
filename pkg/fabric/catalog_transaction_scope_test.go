package fabric

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeCoordinatorNeverOwnsPhysicalTransaction(t *testing.T) {
	for _, coordinatorFirst := range []bool{false,true} {
		t.Run(map[bool]string{false:"physical first",true:"coordinator first"}[coordinatorFirst],func(t *testing.T){
			catalog := NewCatalog()
			coordinator := &LocationLocal{DBName:"comp"}
			one,two := &LocationLocal{DBName:"one"},&LocationLocal{DBName:"two"}
			catalog.RegisterComposite("comp",coordinator)
			catalog.RegisterComposite("comp_alias",coordinator)
			catalog.Register("comp.one",one)
			catalog.Register("comp.two",two)
			tx := NewFabricTransaction("identity")
			require.NoError(t,catalog.BindTransactionScopes(tx))
			joinCoordinator:=func(){
				scope,err:=tx.GetOrOpenLocation(coordinator,true)
				require.NoError(t,err)
				require.False(t,scope.IsWrite)
				require.True(t,tx.IsCoordinatorScope(scope.ShardName))
			}
			if coordinatorFirst {joinCoordinator();require.Empty(t,tx.WriteShard());require.Empty(t,tx.Participants())}
			writer,err:=tx.GetOrOpenLocation(one,true)
			require.NoError(t,err)
			if !coordinatorFirst {joinCoordinator()}
			require.Equal(t,writer.ShardName,tx.WriteShard())
			require.Equal(t,[]string{writer.ShardName},tx.Participants())
			_,err=tx.GetOrOpenLocation(two,true)
			require.ErrorIs(t,err,ErrSecondWriteShard)
			var completed []string
			require.NoError(t,tx.Commit(func(sub *SubTransaction)error{completed=append(completed,sub.ShardName);return nil},nil))
			require.Equal(t,[]string{writer.ShardName},completed)
		})
	}
}
