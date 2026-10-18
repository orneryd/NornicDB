package resultstream

import (
 "context"
 "encoding/base64"
 "strings"
 "testing"
 "github.com/stretchr/testify/require"
)
type blockedFirstDeliveryStream struct {
 lifecycleStream
 entered chan struct{}
 resume chan struct{}
}
func(s *blockedFirstDeliveryStream) Pull(ctx context.Context,position uint64,n int)(*Page,error) {
 close(s.entered)
 <-s.resume
 return s.lifecycleStream.Pull(ctx,position,n)
}
func TestStreamDeliveryDatabaseDiscardDuringStart(t *testing.T) {
 r,err:=NewRegistry(Config{MaxStreams:1});require.NoError(t,err);t.Cleanup(r.Close)
 scope:=Scope{Owner:"alice",Database:"one"}
 stream:=&blockedFirstDeliveryStream{entered:make(chan struct{}),resume:make(chan struct{})}
 done:=make(chan error,1)
 go func(){_,err:=r.Start(context.Background(),scope,stream,1);done<-err}()
 <-stream.entered
 r.DiscardDatabase("one")
 close(stream.resume)
 require.ErrorIs(t,<-done,ErrInvalidated)
 require.EqualValues(t,1,stream.closes.Load())
 require.Zero(t,r.count.Load())
 require.Zero(t,r.retained.Load())
 _,err=r.Start(context.Background(),scope,&lifecycleStream{},1);require.NoError(t,err,"a new database generation can admit")
}
func TestStreamDeliveryRejectOversizedTokenBeforeDecode(t *testing.T) {
 var secret [32]byte
 var instance [8]byte
 oversized:=strings.Repeat("A",base64.RawURLEncoding.EncodedLen(tokenBytes+(64<<10))+1)
 allocations:=testing.AllocsPerRun(20,func(){
  _,err:=decodeToken(secret,instance,oversized)
  if err!=ErrInvalidQID { panic("oversized token accepted") }
 })
 require.Zero(t,allocations,"reject before allocating decoded client input")
 r,err:=NewRegistry(Config{});require.NoError(t,err);t.Cleanup(r.Close)
 stream:=&lifecycleStream{}
 _,err=r.Start(context.Background(),Scope{Owner:"alice",Database:strings.Repeat("d",(64<<10)+1)},stream,1)
 require.ErrorIs(t,err,ErrInvalidScope)
 require.EqualValues(t,1,stream.closes.Load())
}

