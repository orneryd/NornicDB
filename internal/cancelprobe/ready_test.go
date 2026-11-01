package cancelprobe

import "testing"

func TestReady(t *testing.T) {
	if Ready(nil) {
		t.Fatal("nil Done channel must remain live")
	}
	done := make(chan struct{})
	if Ready(done) {
		t.Fatal("open Done channel must remain live")
	}
	close(done)
	for range 2 {
		if !Ready(done) {
			t.Fatal("closed Done channel must stay ready")
		}
	}
}
