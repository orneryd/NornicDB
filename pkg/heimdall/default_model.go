package heimdall

import (
	_ "embed"
	"encoding/json"
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

var packagedModel = func() LocalModelDefaults {
	var model LocalModelDefaults
	if err := json.Unmarshal(packagedModelJSON, &model); err != nil {
		panic(err)
	}
	return model
}()

func DefaultLocalModel() string { return packagedModel.Name }
