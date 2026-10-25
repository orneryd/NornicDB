// Package lockeffect classifies sync lock method effects for source analyses.
package lockeffect

// Classify reports acquisition, release or deferred release and exclusivity.
// A deferred acquisition does not establish a held span in the caller.
func Classify(method string, deferred bool) (action string, exclusive bool) {
	switch method {
	case "Lock", "RLock":
		if !deferred {
			return "lock", method == "Lock"
		}
	case "Unlock", "RUnlock":
		if deferred {
			return "defer", false
		}
		return "unlock", false
	}
	return "", false
}
