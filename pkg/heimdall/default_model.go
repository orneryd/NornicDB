package heimdall

import (
	_ "embed"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

//go:embed default_model.json
var packagedModelJSON []byte

// LocalModelDefaults is the same manifest consumed by the packaged images.
type LocalModelDefaults struct {
	Name                string `json:"name"`
	URL                 string `json:"url"`
	Source              string `json:"source"`
	License             string `json:"license"`
	NativeContextTokens int    `json:"native_context_tokens"`
}

var packagedModel = loadLocalModelDefaults(packagedModelJSON)

func loadLocalModelDefaults(data []byte) LocalModelDefaults {
	var model LocalModelDefaults
	if err := valueconvert.PropertyJSON.Unmarshal(data, &model); err != nil {
		panic(err)
	}
	return model
}

func DefaultLocalModel() string { return packagedModel.Name }
