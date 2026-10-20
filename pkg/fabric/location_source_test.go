package fabric

import (
    "testing"
    "github.com/stretchr/testify/require"
)

// Literal expectations preserve the shipped format independently of either adapter.
func TestPhysicalLocationSourceOwnerPreservesShippedKeys(t *testing.T) {
    for _, tt := range []struct { location Location; want string }{
        {&LocationLocal{DBName: "tenant"}, "local:tenant"},
        {&LocationLocal{DBName: " tenant "}, "local:tenant"},
        {&LocationLocal{}, "local:"},
        {&LocationRemote{URI: "http://one.example", DBName: "remote_db"}, "remote:http://one.example|remote_db"},
        {&LocationRemote{URI: " http://two.example ", DBName: " remote_db "}, "remote:http://two.example|remote_db"},
        {&LocationRemote{URI: "", DBName: "remote_db"}, "remote:|remote_db"},
        {&LocationRemote{}, "remote:|"},
        {nil, "unknown:<nil>"},
    } {
        require.Equal(t, tt.want, participantKeyFromLocation(tt.location))
        require.Equal(t, tt.want, LocationSourceKey(tt.location))
    }
}
