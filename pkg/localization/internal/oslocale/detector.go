package oslocale

import "golang.org/x/text/language"

// Preferences returns the operating system's ordered language preferences.
func Preferences() ([]language.Tag, error) {
	values, err := preferenceStrings()
	if err != nil {
		return nil, err
	}
	tags := make([]language.Tag, 0, len(values))
	for _, value := range values {
		tag, err := parse(value)
		if err != nil || tag == language.Und {
			continue
		}
		tags = append(tags, tag)
	}
	if len(tags) == 0 {
		return nil, ErrNotDetected
	}
	return tags, nil
}
