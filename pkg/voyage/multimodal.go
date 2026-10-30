package voyage

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

const (
	// DefaultMultimodalModel is Voyage's current recommended multimodal model.
	DefaultMultimodalModel = "voyage-multimodal-3.5"
	// MultimodalContentProperty is the managed-node property containing an
	// ordered JSON array of Voyage text and image parts.
	MultimodalContentProperty = "_embedding_content"
	// MultimodalMaxInputs is Voyage's request-level input limit.
	MultimodalMaxInputs            = 1000
	multimodalMaxDecodedImageBytes = 20_000_000
)

type validationError struct{ message string }

func (e validationError) Error() string   { return e.message }
func (e validationError) Retryable() bool { return false }

func invalidMultimodal(format string, args ...any) error {
	return validationError{message: fmt.Sprintf(format, args...)}
}

// MultimodalPart is one text, remote image, or base64 data-URI image. Voyage
// fetches remote URLs; NornicDB never dereferences them.
type MultimodalPart struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	ImageURL    string `json:"image_url,omitempty"`
	ImageBase64 string `json:"image_base64,omitempty"`
}

// MultimodalInput preserves the ordered content that produces one vector.
type MultimodalInput struct {
	Content []MultimodalPart `json:"content"`
}

// ParseMultimodalContent decodes and validates a managed node's structured
// content. It accepts either a JSON array or the equivalent Go slice value.
func ParseMultimodalContent(value any) ([]MultimodalPart, error) {
	var data []byte
	var err error
	if raw, ok := value.(string); ok {
		data = []byte(raw)
	} else {
		data, err = json.Marshal(value)
		if err != nil {
			return nil, invalidMultimodal("encode multimodal content: %v", err)
		}
	}
	var parts []MultimodalPart
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parts); err != nil {
		return nil, invalidMultimodal("decode multimodal content: %v", err)
	}
	if err := ValidateMultimodalContent(parts); err != nil {
		return nil, err
	}
	return parts, nil
}

// ValidateMultimodalContent enforces the deterministic portion of Voyage's
// request contract. Pixel and token counts remain provider-authoritative.
func ValidateMultimodalContent(parts []MultimodalPart) error {
	if len(parts) == 0 {
		return invalidMultimodal("multimodal content must contain at least one part")
	}
	usesURL, usesBase64 := false, false
	for index, part := range parts {
		switch part.Type {
		case "text":
			if strings.TrimSpace(part.Text) == "" || part.ImageURL != "" || part.ImageBase64 != "" {
				return invalidMultimodal("multimodal content part %d is not valid text", index)
			}
		case "image_url":
			if part.Text != "" || part.ImageBase64 != "" || !validMultimodalImageURL(part.ImageURL) {
				return invalidMultimodal("multimodal content part %d is not a valid image URL", index)
			}
			usesURL = true
		case "image_base64":
			if part.Text != "" || part.ImageURL != "" || !validMultimodalImageDataURI(part.ImageBase64) {
				return invalidMultimodal("multimodal content part %d is not a valid image data URI", index)
			}
			usesBase64 = true
		default:
			return invalidMultimodal("multimodal content part %d has unsupported type %q", index, part.Type)
		}
	}
	if usesURL && usesBase64 {
		return invalidMultimodal("multimodal content cannot mix URL and base64 representations in one request")
	}
	return nil
}

func validMultimodalImageURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Hostname() != "" && parsed.User == nil && parsed.Fragment == "" &&
		(parsed.Scheme == "https" || parsed.Scheme == "http")
}

func validMultimodalImageDataURI(value string) bool {
	header, encoded, ok := strings.Cut(value, ",")
	if !ok || encoded == "" {
		return false
	}
	switch header {
	case "data:image/png;base64", "data:image/jpeg;base64", "data:image/webp;base64", "data:image/gif;base64":
	default:
		return false
	}
	if len(encoded) > base64.StdEncoding.EncodedLen(multimodalMaxDecodedImageBytes) {
		return false
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	return err == nil && len(decoded) > 0 && len(decoded) <= multimodalMaxDecodedImageBytes
}

func validateMultimodalInputs(inputs []any) error {
	if len(inputs) == 0 || len(inputs) > MultimodalMaxInputs {
		return invalidMultimodal("multimodal request must contain 1 to %d inputs", MultimodalMaxInputs)
	}
	for index, input := range inputs {
		data, err := json.Marshal(input)
		if err != nil {
			return invalidMultimodal("encode multimodal input %d: %v", index, err)
		}
		var typed MultimodalInput
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&typed); err != nil {
			return invalidMultimodal("decode multimodal input %d: %v", index, err)
		}
		if err := ValidateMultimodalContent(typed.Content); err != nil {
			return invalidMultimodal("multimodal input %d: %v", index, err)
		}
	}
	return nil
}
