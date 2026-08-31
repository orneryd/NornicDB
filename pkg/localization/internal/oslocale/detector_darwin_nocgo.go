//go:build darwin && !cgo

package oslocale

func preferenceStrings() ([]string, error) {
	return environmentPreferences()
}
