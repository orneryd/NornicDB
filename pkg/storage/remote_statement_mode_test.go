package storage

import "testing"

func TestRemoteHTTPModeEvidence(t *testing.T) {
 yes,no:=true,false
 for _,tc:=range []struct{name string; stats *remoteQueryStats; want int32}{
  {"missing",nil,remoteModeUnknown},
  {"empty",&remoteQueryStats{},remoteModeUnknown},
  {"zero counters",&remoteQueryStats{ContainsUpdates:&no},remoteModeUnknown},
  {"positive standard counters",&remoteQueryStats{ContainsUpdates:&yes},remoteModeWrite},
  {"system updates",&remoteQueryStats{ContainsSystemUpdates:&yes},remoteModeWrite},
  {"resolved read",&remoteQueryStats{ContainsUpdates:&no,QueryType:"r"},remoteModeRead},
  {"resolved write without counters",&remoteQueryStats{ContainsUpdates:&no,QueryType:"w"},remoteModeWrite},
  {"invalid mode",&remoteQueryStats{QueryType:"invalid"},remoteModeUnknown},
  {"updates override read",&remoteQueryStats{ContainsUpdates:&yes,QueryType:"r"},remoteModeWrite},
 } { t.Run(tc.name,func(t *testing.T){ if got:=remoteHTTPQueryMode(tc.stats); got!=tc.want { t.Fatalf("got %d, want %d",got,tc.want) } }) }
}