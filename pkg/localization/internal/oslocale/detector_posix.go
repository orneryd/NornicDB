//go:build !darwin && !windows

package oslocale

func preferenceStrings() ([]string, error) {
	return environmentPreferences()
}
