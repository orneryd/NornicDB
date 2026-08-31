//go:build windows

package oslocale

import "golang.org/x/sys/windows"

func preferenceStrings() ([]string, error) {
	preferences, err := windows.GetUserPreferredUILanguages(windows.MUI_LANGUAGE_NAME)
	if err == nil && len(preferences) > 0 {
		return preferences, nil
	}
	fallback, fallbackErr := environmentPreferences()
	if fallbackErr == nil {
		return fallback, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, ErrNotDetected
}
