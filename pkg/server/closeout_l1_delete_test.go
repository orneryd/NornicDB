package server

import "testing"

func TestCloseoutHTTPDeleteServerTimeoutOutcome(t *testing.T) {
	for name, count := range map[string]int{"two_thousand": 2000, "sixty_thousand": 60000} {
		t.Run(name, func(t *testing.T) {
			zzei2L527Sweep(t, []int{count}, []string{"server-timeout"})
		})
	}
}
