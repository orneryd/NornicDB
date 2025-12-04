# Vision-Language Pipeline Proposal for NornicDB

**Status:** PROPOSAL  
**Version:** 1.0.0  
**Date:** December 2024  
**Author:** Architecture Review

---

## Executive Summary

This proposal outlines the addition of a **Vision-Language (VL) model** as a third model slot in NornicDB's model stack, enabling automatic image understanding and semantic search across image content.

### What We're Building

Adding a VL model creates a powerful image understanding pipeline:
- Detect nodes with `:Image` label or image properties
- Scale images to ≤3.2MP (like Mimir does for multimodal)
- Run through VL model (Qwen2.5-VL-2B) to get text description
- Combine description with node properties
- Generate text embedding using existing BGE-M3
- Store embedding for semantic search

---

## 1. Architecture Overview

### Current Model Stack (2 slots)

```
┌──────────────┐    ┌──────────────┐
│  Embedding   │    │   Reasoning  │
│    Model     │    │     SLM      │
│  (BGE-M3)    │    │  (Heimdall)  │
└──────────────┘    └──────────────┘
```

### Proposed Model Stack (3 slots)

```
┌─────────────────────────────────────────────────────────────────┐
│                    NornicDB Model Stack                         │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  Embedding   │    │   Reasoning  │    │  Vision-Language │  │
│  │    Model     │    │     SLM      │    │      Model       │  │
│  │  (BGE-M3)    │    │  (Heimdall)  │    │  (Qwen2.5-VL)    │  │
│  │  1024 dims   │    │   0.5B-3B    │    │     2B-7B        │  │
│  └──────┬───────┘    └──────┬───────┘    └────────┬─────────┘  │
│         │                   │                      │            │
│         └───────────────────┴──────────────────────┘            │
│                             │                                    │
│                    ┌────────▼────────┐                          │
│                    │  Model Manager  │                          │
│                    │  (3 slots now)  │                          │
│                    └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Image Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  CREATE (n:Image {data: $base64, filename: 'photo.jpg'})        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Node Detector                                                  │
│  └─ Is label :Image? Or has image_data/image_url property?      │
│  └─ YES → Route to Vision Pipeline                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Image Preprocessor                                             │
│  └─ Decode base64 or fetch URL                                  │
│  └─ Scale to ≤3.2MP (preserve aspect ratio)                     │
│  └─ Convert to RGB if needed                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  VL Model (Qwen2.5-VL-2B)                                       │
│  └─ Input: scaled image + prompt                                │
│  └─ Output: text description                                    │
│     "A sunset over mountains with orange and purple clouds..."  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Text Combiner                                                  │
│  └─ description + node.filename + node.tags + node.caption      │
│  └─ Result: "Image: sunset over mountains... filename: photo... │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Text Embedder (BGE-M3)                                         │
│  └─ Generate 1024-dim embedding from combined text              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Store: node._embedding = [0.123, 0.456, ...]                   │
│  Store: node._vl_description = "A sunset over mountains..."     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_VISION_ENABLED` | `false` | Enable vision pipeline |
| `NORNICDB_VISION_MODEL` | `qwen2.5-vl-2b-instruct` | VL model to use |
| `NORNICDB_VISION_GPU_LAYERS` | `-1` | GPU layers (-1 = auto) |
| `NORNICDB_VISION_MAX_PIXELS` | `3200000` | Max pixels before scaling (3.2MP) |
| `NORNICDB_VISION_PROMPT` | (see below) | Custom prompt for VL |

### Default Vision Prompt

```
Describe this image in detail, including objects, colors, composition, and any text visible.
```

### Go Configuration Types

```go
// pkg/config/features.go

type FeatureFlags struct {
    // ... existing fields ...
    
    // Vision-Language Model
    VisionEnabled    bool    `json:"vision_enabled" env:"NORNICDB_VISION_ENABLED"`
    VisionModel      string  `json:"vision_model" env:"NORNICDB_VISION_MODEL"`
    VisionGPULayers  int     `json:"vision_gpu_layers" env:"NORNICDB_VISION_GPU_LAYERS"`
    VisionMaxPixels  int     `json:"vision_max_pixels" env:"NORNICDB_VISION_MAX_PIXELS"` // Default: 3200000 (3.2MP)
    VisionPrompt     string  `json:"vision_prompt" env:"NORNICDB_VISION_PROMPT"` // Custom prompt for VL
}

// Defaults
const (
    DefaultVisionModel     = "qwen2.5-vl-2b-instruct"
    DefaultVisionMaxPixels = 3200000 // 3.2MP
    DefaultVisionPrompt    = "Describe this image in detail, including objects, colors, composition, and any text visible."
)
```

---

## 4. Node Detection Strategy

### Detection Logic

Nodes are processed by the vision pipeline if they match ANY of these criteria:

1. **Labels**: `:Image`, `:Photo`, `:Picture`
2. **Properties**: `image_data`, `image_url`, `base64`
3. **Filename extension**: `.jpg`, `.jpeg`, `.png`, `.gif`, `.webp`, `.bmp`

### Implementation

```go
// pkg/vision/detector.go

package vision

import (
    "path/filepath"
    "strings"
    
    "github.com/orneryd/nornicdb/pkg/storage"
)

// IsImageNode checks if a node should be processed by the vision pipeline.
// Checks both labels and properties.
func IsImageNode(node *storage.Node) bool {
    // Check labels
    for _, label := range node.Labels {
        if label == "Image" || label == "Photo" || label == "Picture" {
            return true
        }
    }
    
    // Check for image data properties
    if _, hasData := node.Properties["image_data"]; hasData {
        return true
    }
    if _, hasURL := node.Properties["image_url"]; hasURL {
        return true
    }
    if _, hasBase64 := node.Properties["base64"]; hasBase64 {
        return true
    }
    
    // Check for common image extensions in filename
    if filename, ok := node.Properties["filename"].(string); ok {
        ext := strings.ToLower(filepath.Ext(filename))
        switch ext {
        case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp":
            return true
        }
    }
    
    return false
}

// ImageNodeLabels returns all labels that trigger vision processing.
func ImageNodeLabels() []string {
    return []string{"Image", "Photo", "Picture"}
}

// ImageNodeProperties returns all property names that trigger vision processing.
func ImageNodeProperties() []string {
    return []string{"image_data", "image_url", "base64"}
}

// ImageExtensions returns all file extensions that trigger vision processing.
func ImageExtensions() []string {
    return []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
}
```

---

## 5. Types and Interfaces

```go
// pkg/vision/types.go

package vision

import (
    "context"
    "time"
)

// Config for the vision pipeline.
type Config struct {
    // Enabled activates the vision pipeline
    Enabled bool
    
    // Model is the VL model name (without .gguf extension)
    Model string
    
    // ModelsDir is the directory containing GGUF models
    // Uses NORNICDB_MODELS_DIR (same as embedder and Heimdall)
    ModelsDir string
    
    // GPULayers controls GPU offloading (-1 = auto)
    GPULayers int
    
    // MaxPixels is the maximum pixels before scaling (default: 3.2MP)
    MaxPixels int
    
    // Prompt is sent with the image to the VL model
    Prompt string
}

// DefaultConfig returns sensible defaults for the vision pipeline.
func DefaultConfig() Config {
    return Config{
        Enabled:   false,
        Model:     "qwen2.5-vl-2b-instruct",
        ModelsDir: "", // Use NORNICDB_MODELS_DIR
        GPULayers: -1, // Auto
        MaxPixels: 3200000, // 3.2MP
        Prompt:    "Describe this image in detail, including objects, colors, composition, and any text visible.",
    }
}

// ImageInput represents an image to be processed.
type ImageInput struct {
    // Data is the raw image bytes (decoded from base64 or fetched from URL)
    Data []byte
    
    // MimeType identifies the image format ("image/jpeg", "image/png", etc.)
    MimeType string
    
    // Width is the original image width in pixels
    Width int
    
    // Height is the original image height in pixels
    Height int
    
    // Source describes where the image came from (for logging)
    Source string // "base64", "url", "file"
}

// VisionResult contains the VL model output.
type VisionResult struct {
    // Description is the generated text describing the image
    Description string
    
    // Duration is the processing time
    Duration time.Duration
    
    // Scaled indicates whether the image was scaled down
    Scaled bool
    
    // FinalWidth is the width after scaling (same as original if not scaled)
    FinalWidth int
    
    // FinalHeight is the height after scaling (same as original if not scaled)
    FinalHeight int
    
    // OriginalWidth is the original image width
    OriginalWidth int
    
    // OriginalHeight is the original image height
    OriginalHeight int
}

// VisionGenerator interface for VL models.
type VisionGenerator interface {
    // DescribeImage generates a text description of an image.
    // The prompt guides what aspects of the image to describe.
    DescribeImage(ctx context.Context, img *ImageInput, prompt string) (*VisionResult, error)
    
    // ModelInfo returns information about the loaded model.
    ModelInfo() ModelInfo
    
    // Close releases model resources.
    Close() error
}

// ModelInfo contains metadata about the loaded VL model.
type ModelInfo struct {
    Name       string
    Path       string
    SizeBytes  int64
    GPULayers  int
    LoadedAt   time.Time
}

// ImageProcessor handles image scaling and format conversion.
type ImageProcessor interface {
    // Scale resizes an image to fit within maxPixels while preserving aspect ratio.
    Scale(img *ImageInput, maxPixels int) (*ImageInput, error)
    
    // Decode parses image bytes and returns dimensions.
    Decode(data []byte) (*ImageInput, error)
    
    // SupportedFormats returns the list of supported MIME types.
    SupportedFormats() []string
}
```

---

## 6. Integration with Embedding Pipeline

```go
// pkg/embed/embedder.go - Modified to support vision

package embed

import (
    "context"
    "fmt"
    "strings"
    
    "github.com/orneryd/nornicdb/pkg/storage"
    "github.com/orneryd/nornicdb/pkg/vision"
)

// Embedder generates embeddings for nodes.
type Embedder struct {
    // ... existing fields ...
    
    // Vision support
    visionEnabled bool
    visionConfig  vision.Config
    visionGen     vision.VisionGenerator
    imgProcessor  vision.ImageProcessor
}

// GenerateNodeEmbedding creates an embedding for a node.
// Automatically detects image nodes and routes them through the vision pipeline.
func (e *Embedder) GenerateNodeEmbedding(ctx context.Context, node *storage.Node) ([]float32, error) {
    // Check if this is an image node
    if vision.IsImageNode(node) && e.visionEnabled {
        return e.generateImageEmbedding(ctx, node)
    }
    
    // Standard text embedding
    return e.generateTextEmbedding(ctx, node)
}

// generateImageEmbedding processes an image node through the vision pipeline.
func (e *Embedder) generateImageEmbedding(ctx context.Context, node *storage.Node) ([]float32, error) {
    // 1. Extract image data from node
    imgData, mimeType, err := e.extractImageData(node)
    if err != nil {
        return nil, fmt.Errorf("failed to extract image: %w", err)
    }
    
    // 2. Create ImageInput
    img := &vision.ImageInput{
        Data:     imgData,
        MimeType: mimeType,
    }
    
    // 3. Decode to get dimensions
    img, err = e.imgProcessor.Decode(imgData)
    if err != nil {
        return nil, fmt.Errorf("failed to decode image: %w", err)
    }
    
    // 4. Scale image if needed
    if img.Width*img.Height > e.visionConfig.MaxPixels {
        img, err = e.imgProcessor.Scale(img, e.visionConfig.MaxPixels)
        if err != nil {
            return nil, fmt.Errorf("failed to scale image: %w", err)
        }
    }
    
    // 5. Get description from VL model
    result, err := e.visionGen.DescribeImage(ctx, img, e.visionConfig.Prompt)
    if err != nil {
        return nil, fmt.Errorf("vision model failed: %w", err)
    }
    
    // 6. Combine description with node properties
    combinedText := e.combineImageContext(result.Description, node)
    
    // 7. Store description on node for reference
    node.Properties["_vl_description"] = result.Description
    node.Properties["_vl_processed"] = true
    
    // 8. Generate text embedding from combined context
    return e.generateTextEmbeddingFromString(ctx, combinedText)
}

// extractImageData gets image bytes from a node's properties.
func (e *Embedder) extractImageData(node *storage.Node) ([]byte, string, error) {
    // Try base64 encoded data
    if data, ok := node.Properties["image_data"].(string); ok {
        decoded, err := base64.StdEncoding.DecodeString(data)
        if err != nil {
            return nil, "", fmt.Errorf("invalid base64: %w", err)
        }
        mimeType := detectMimeType(decoded)
        return decoded, mimeType, nil
    }
    
    // Try base64 property
    if data, ok := node.Properties["base64"].(string); ok {
        decoded, err := base64.StdEncoding.DecodeString(data)
        if err != nil {
            return nil, "", fmt.Errorf("invalid base64: %w", err)
        }
        mimeType := detectMimeType(decoded)
        return decoded, mimeType, nil
    }
    
    // Try URL
    if url, ok := node.Properties["image_url"].(string); ok {
        // Fetch from URL (with timeout)
        data, mimeType, err := e.fetchImageFromURL(url)
        if err != nil {
            return nil, "", fmt.Errorf("failed to fetch image: %w", err)
        }
        return data, mimeType, nil
    }
    
    return nil, "", fmt.Errorf("no image data found in node properties")
}

// combineImageContext merges the VL description with node properties.
func (e *Embedder) combineImageContext(description string, node *storage.Node) string {
    var parts []string
    
    // Add VL description first (most important)
    parts = append(parts, "Image description: "+description)
    
    // Add filename if present
    if filename, ok := node.Properties["filename"].(string); ok {
        parts = append(parts, "Filename: "+filename)
    }
    
    // Add user-provided caption if present
    if caption, ok := node.Properties["caption"].(string); ok {
        parts = append(parts, "Caption: "+caption)
    }
    
    // Add alt text if present
    if alt, ok := node.Properties["alt"].(string); ok {
        parts = append(parts, "Alt text: "+alt)
    }
    
    // Add tags if present
    if tags, ok := node.Properties["tags"].([]interface{}); ok {
        tagStrs := make([]string, len(tags))
        for i, t := range tags {
            tagStrs[i] = fmt.Sprint(t)
        }
        parts = append(parts, "Tags: "+strings.Join(tagStrs, ", "))
    }
    
    // Add title if present
    if title, ok := node.Properties["title"].(string); ok {
        parts = append(parts, "Title: "+title)
    }
    
    return strings.Join(parts, "\n")
}

// detectMimeType identifies the image format from magic bytes.
func detectMimeType(data []byte) string {
    if len(data) < 4 {
        return "application/octet-stream"
    }
    
    // Check magic bytes
    switch {
    case data[0] == 0xFF && data[1] == 0xD8:
        return "image/jpeg"
    case data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47:
        return "image/png"
    case data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46:
        return "image/gif"
    case data[0] == 0x52 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x46:
        return "image/webp"
    case data[0] == 0x42 && data[1] == 0x4D:
        return "image/bmp"
    default:
        return "application/octet-stream"
    }
}
```

---

## 7. Image Scaling Implementation

```go
// pkg/vision/scaler.go

package vision

import (
    "bytes"
    "fmt"
    "image"
    "image/jpeg"
    "image/png"
    "math"
    
    // For additional format support
    _ "image/gif"
    _ "golang.org/x/image/webp"
)

// StandardImageProcessor implements ImageProcessor using Go's image package.
type StandardImageProcessor struct{}

// NewImageProcessor creates a new image processor.
func NewImageProcessor() *StandardImageProcessor {
    return &StandardImageProcessor{}
}

// Decode parses image bytes and returns an ImageInput with dimensions.
func (p *StandardImageProcessor) Decode(data []byte) (*ImageInput, error) {
    reader := bytes.NewReader(data)
    cfg, format, err := image.DecodeConfig(reader)
    if err != nil {
        return nil, fmt.Errorf("failed to decode image config: %w", err)
    }
    
    mimeType := "image/" + format
    
    return &ImageInput{
        Data:     data,
        MimeType: mimeType,
        Width:    cfg.Width,
        Height:   cfg.Height,
        Source:   "decoded",
    }, nil
}

// Scale resizes an image to fit within maxPixels while preserving aspect ratio.
func (p *StandardImageProcessor) Scale(img *ImageInput, maxPixels int) (*ImageInput, error) {
    currentPixels := img.Width * img.Height
    if currentPixels <= maxPixels {
        // No scaling needed
        return img, nil
    }
    
    // Calculate scale factor
    scaleFactor := math.Sqrt(float64(maxPixels) / float64(currentPixels))
    newWidth := int(float64(img.Width) * scaleFactor)
    newHeight := int(float64(img.Height) * scaleFactor)
    
    // Decode original image
    reader := bytes.NewReader(img.Data)
    original, format, err := image.Decode(reader)
    if err != nil {
        return nil, fmt.Errorf("failed to decode image: %w", err)
    }
    
    // Create scaled image using simple bilinear interpolation
    scaled := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
    
    // Simple scaling (could use more sophisticated algorithms)
    for y := 0; y < newHeight; y++ {
        for x := 0; x < newWidth; x++ {
            srcX := int(float64(x) / scaleFactor)
            srcY := int(float64(y) / scaleFactor)
            scaled.Set(x, y, original.At(srcX, srcY))
        }
    }
    
    // Encode back to bytes
    var buf bytes.Buffer
    switch format {
    case "jpeg":
        err = jpeg.Encode(&buf, scaled, &jpeg.Options{Quality: 85})
    case "png":
        err = png.Encode(&buf, scaled)
    default:
        // Default to JPEG for other formats
        err = jpeg.Encode(&buf, scaled, &jpeg.Options{Quality: 85})
    }
    if err != nil {
        return nil, fmt.Errorf("failed to encode scaled image: %w", err)
    }
    
    return &ImageInput{
        Data:     buf.Bytes(),
        MimeType: img.MimeType,
        Width:    newWidth,
        Height:   newHeight,
        Source:   "scaled",
    }, nil
}

// SupportedFormats returns the list of supported MIME types.
func (p *StandardImageProcessor) SupportedFormats() []string {
    return []string{
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "image/bmp",
    }
}
```

---

## 8. VL Model Integration with llama.cpp

```go
// pkg/vision/llama_vision.go

package vision

import (
    "context"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "time"
    
    "github.com/orneryd/nornicdb/pkg/localllm"
)

// LlamaVisionGenerator implements VisionGenerator using llama.cpp.
type LlamaVisionGenerator struct {
    model     *localllm.Model
    modelInfo ModelInfo
    config    Config
}

// NewLlamaVisionGenerator creates a new VL generator.
func NewLlamaVisionGenerator(cfg Config) (*LlamaVisionGenerator, error) {
    // Find model file
    modelPath := filepath.Join(cfg.ModelsDir, cfg.Model+".gguf")
    if _, err := os.Stat(modelPath); os.IsNotExist(err) {
        return nil, fmt.Errorf("vision model not found: %s", modelPath)
    }
    
    // Load model via llama.cpp
    // Note: This requires llama.cpp with vision support (LLaVA architecture)
    model, err := localllm.LoadModel(modelPath, localllm.ModelOptions{
        GPULayers: cfg.GPULayers,
        Threads:   4,
        Vision:    true, // Enable vision mode
    })
    if err != nil {
        return nil, fmt.Errorf("failed to load vision model: %w", err)
    }
    
    fileInfo, _ := os.Stat(modelPath)
    
    return &LlamaVisionGenerator{
        model:  model,
        config: cfg,
        modelInfo: ModelInfo{
            Name:      cfg.Model,
            Path:      modelPath,
            SizeBytes: fileInfo.Size(),
            GPULayers: cfg.GPULayers,
            LoadedAt:  time.Now(),
        },
    }, nil
}

// DescribeImage generates a text description of an image.
func (g *LlamaVisionGenerator) DescribeImage(ctx context.Context, img *ImageInput, prompt string) (*VisionResult, error) {
    start := time.Now()
    
    // Format prompt for vision model
    // Most VL models expect: <image>\n{prompt}
    fullPrompt := fmt.Sprintf("<image>\n%s", prompt)
    
    // Run inference with image
    response, err := g.model.GenerateWithImage(ctx, fullPrompt, img.Data, localllm.GenerateOptions{
        MaxTokens:   512,
        Temperature: 0.1,
        StopTokens:  []string{"<|endoftext|>", "<|im_end|>"},
    })
    if err != nil {
        return nil, fmt.Errorf("vision inference failed: %w", err)
    }
    
    return &VisionResult{
        Description:    response,
        Duration:       time.Since(start),
        Scaled:         img.Source == "scaled",
        FinalWidth:     img.Width,
        FinalHeight:    img.Height,
        OriginalWidth:  img.Width, // Would need to track this separately
        OriginalHeight: img.Height,
    }, nil
}

// ModelInfo returns information about the loaded model.
func (g *LlamaVisionGenerator) ModelInfo() ModelInfo {
    return g.modelInfo
}

// Close releases model resources.
func (g *LlamaVisionGenerator) Close() error {
    if g.model != nil {
        return g.model.Close()
    }
    return nil
}
```

---

## 9. Docker Configuration

### New Build Target

```dockerfile
# docker/Dockerfile.arm64-metal-bge-heimdall-vision

FROM timothyswt/nornicdb-arm64-metal-bge-heimdall:latest

# Add vision model
# Qwen2.5-VL-2B is ~2GB
COPY models/qwen2.5-vl-2b-instruct.gguf /app/models/

# Enable vision by default
ENV NORNICDB_VISION_ENABLED=true
ENV NORNICDB_VISION_MODEL=qwen2.5-vl-2b-instruct
ENV NORNICDB_VISION_MAX_PIXELS=3200000

# Total image size: ~3.7GB (1.1GB base + 2.6GB VL model)
```

### Docker Compose

```yaml
# docker-compose.vision.yml

version: '3.8'

services:
  nornicdb-vision:
    image: timothyswt/nornicdb-arm64-metal-bge-heimdall-vision:latest
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
      - ./custom-models:/app/models  # For BYOM
    environment:
      NORNICDB_HEIMDALL_ENABLED: "true"
      NORNICDB_VISION_ENABLED: "true"
      NORNICDB_VISION_MODEL: "qwen2.5-vl-2b-instruct"
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]

volumes:
  nornicdb-data:
```

---

## 10. Usage Examples

### Creating Image Nodes

```cypher
// Create an image node with base64 data
CREATE (img:Image {
    filename: 'vacation.jpg',
    image_data: $base64Data,
    caption: 'Beach sunset in Hawaii',
    tags: ['vacation', 'beach', 'sunset']
})

// The vision pipeline automatically:
// 1. Detects :Image label
// 2. Scales image to ≤3.2MP if needed
// 3. Generates VL description: "A stunning sunset over a tropical beach..."
// 4. Combines description with properties
// 5. Generates text embedding
// 6. Stores _vl_description and _embedding on node
```

### Creating Image Nodes from URLs

```cypher
// Create an image node from URL
CREATE (img:Image {
    image_url: 'https://example.com/photo.jpg',
    title: 'Product Photo',
    alt: 'Red sneakers on white background'
})
```

### Semantic Search on Images

```cypher
// Find images similar to a text query
// First, embed the query text
CALL db.index.vector.queryNodes('images', 10, $queryEmbedding)
YIELD node, score
RETURN node.filename, node._vl_description, score
ORDER BY score DESC
```

### Querying VL Descriptions

```cypher
// Find images by their generated descriptions
MATCH (img:Image)
WHERE img._vl_description CONTAINS 'sunset'
RETURN img.filename, img._vl_description
```

### Mixed Content Search

```cypher
// Search across images and text content together
CALL db.index.vector.queryNodes('content', 20, $queryEmbedding)
YIELD node, score
RETURN 
    CASE 
        WHEN 'Image' IN labels(node) THEN 'IMAGE'
        ELSE 'TEXT'
    END as type,
    node.filename,
    node.content,
    node._vl_description,
    score
ORDER BY score DESC
```

---

## 11. Model Recommendations

### Recommended VL Models

| Model | Size | Quality | Speed | Use Case |
|-------|------|---------|-------|----------|
| `qwen2.5-vl-2b-instruct` | ~2 GB | Good | Fast | **Recommended** - balanced |
| `qwen2.5-vl-7b-instruct` | ~7 GB | Better | Slower | Higher quality descriptions |
| `llava-v1.6-mistral-7b` | ~7 GB | Good | Medium | Alternative option |
| `moondream2` | ~1.5 GB | Basic | Fast | Lightweight option |
| `bakllava-1` | ~4 GB | Good | Medium | Good balance |

### Download Commands

```bash
# Qwen2.5-VL-2B (Recommended)
curl -L -o models/qwen2.5-vl-2b-instruct.gguf \
  "https://huggingface.co/Qwen/Qwen2.5-VL-2B-Instruct-GGUF/resolve/main/qwen2.5-vl-2b-instruct-q4_k_m.gguf"

# Qwen2.5-VL-7B (Higher quality)
curl -L -o models/qwen2.5-vl-7b-instruct.gguf \
  "https://huggingface.co/Qwen/Qwen2.5-VL-7B-Instruct-GGUF/resolve/main/qwen2.5-vl-7b-instruct-q4_k_m.gguf"

# MoonDream2 (Lightweight)
curl -L -o models/moondream2.gguf \
  "https://huggingface.co/vikhyatk/moondream2/resolve/main/moondream2-gguf/moondream2-q4_k_m.gguf"
```

### Quantization Options

| Quantization | Quality | Size | Speed |
|--------------|---------|------|-------|
| `q4_k_m` | Good | ~40% | Fast | **Recommended** |
| `q5_k_m` | Better | ~50% | Medium |
| `q8_0` | Best | ~80% | Slower |
| `f16` | Original | 100% | Slowest |

---

## 12. BYOM (Bring Your Own Model)

### Custom Model Setup

```bash
# 1. Download or train your VL model in GGUF format
# 2. Place in models directory
cp my-custom-vl-model.gguf /path/to/models/

# 3. Configure NornicDB
export NORNICDB_VISION_MODEL=my-custom-vl-model

# 4. Optionally customize the prompt
export NORNICDB_VISION_PROMPT="Describe this image focusing on: objects, text, colors, and mood."
```

### Docker with Custom Model

```bash
docker run -d \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -v /path/to/models:/app/models \
  -e NORNICDB_VISION_ENABLED=true \
  -e NORNICDB_VISION_MODEL=my-custom-vl-model \
  timothyswt/nornicdb-arm64-metal-bge-heimdall
```

---

## 13. Performance Considerations

### Memory Requirements

| Model | VRAM (GPU) | RAM (CPU fallback) |
|-------|------------|-------------------|
| qwen2.5-vl-2b | ~3 GB | ~4 GB |
| qwen2.5-vl-7b | ~8 GB | ~10 GB |
| moondream2 | ~2 GB | ~3 GB |

### Processing Time

| Image Size | Scale Time | VL Inference | Embedding | Total |
|------------|------------|--------------|-----------|-------|
| 1MP | 0ms | ~500ms | ~50ms | ~550ms |
| 3.2MP | 0ms | ~600ms | ~50ms | ~650ms |
| 12MP | ~100ms | ~600ms | ~50ms | ~750ms |
| 48MP | ~200ms | ~600ms | ~50ms | ~850ms |

### Optimization Tips

1. **Use GPU acceleration**: Set `NORNICDB_VISION_GPU_LAYERS=-1` for auto
2. **Batch processing**: Process multiple images in parallel
3. **Pre-scale images**: If you control input, scale before storing
4. **Use smaller models**: moondream2 is 3x faster than qwen2.5-vl-7b
5. **Cache descriptions**: `_vl_description` is stored, no re-processing needed

---

## 14. Multi-Model Memory Management Strategy

### The Problem

Running 3 models simultaneously is memory-intensive:

| Model | VRAM | RAM (CPU) |
|-------|------|-----------|
| BGE-M3 (Embedding) | ~1 GB | ~1.5 GB |
| Qwen2.5-0.5B (Heimdall) | ~1 GB | ~1.5 GB |
| Qwen2.5-VL-2B (Vision) | ~3 GB | ~4 GB |
| **Total (all loaded)** | **~5 GB** | **~7 GB** |

With larger models:
| Model | VRAM | RAM (CPU) |
|-------|------|-----------|
| BGE-M3 (Embedding) | ~1 GB | ~1.5 GB |
| Qwen2.5-3B (Heimdall) | ~4 GB | ~5 GB |
| Qwen2.5-VL-7B (Vision) | ~8 GB | ~10 GB |
| **Total (all loaded)** | **~13 GB** | **~16.5 GB** |

Most systems can't afford to keep all models loaded simultaneously.

### Solution: Adaptive Model Lifecycle Manager

```
┌─────────────────────────────────────────────────────────────────┐
│                   Model Lifecycle Manager                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Memory Budget Controller                    │   │
│  │  └─ Max VRAM: 8GB    └─ Max RAM: 12GB                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│           ┌──────────────────┼──────────────────┐              │
│           ▼                  ▼                  ▼              │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐     │
│  │   Embedding    │ │    Heimdall    │ │     Vision     │     │
│  │   (Priority 1) │ │   (Priority 2) │ │   (Priority 3) │     │
│  │   ALWAYS HOT   │ │   WARM/COLD    │ │   COLD/UNLOAD  │     │
│  └────────────────┘ └────────────────┘ └────────────────┘     │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              LRU Eviction Queue                          │   │
│  │  [Vision: 5min idle] → [Heimdall: 2min idle]            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Model Priority Levels

| Priority | Model | Behavior | Rationale |
|----------|-------|----------|-----------|
| **1 (Highest)** | Embedding (BGE-M3) | Always loaded | Used for every node creation, query, search |
| **2 (Medium)** | Heimdall SLM | Load on demand, keep warm | Used for chat, less frequent than embeddings |
| **3 (Lowest)** | Vision VL | Load on demand, unload quickly | Only for image nodes, most memory-intensive |

### Configuration

```go
// pkg/models/lifecycle.go

type LifecycleConfig struct {
    // Memory budgets
    MaxVRAM         int64         `env:"NORNICDB_MAX_VRAM"`          // Max GPU memory (bytes), 0 = unlimited
    MaxRAM          int64         `env:"NORNICDB_MAX_RAM"`           // Max CPU memory (bytes), 0 = unlimited
    
    // Keep-alive durations (how long to keep model loaded after last use)
    EmbeddingKeepAlive  time.Duration `env:"NORNICDB_EMBEDDING_KEEPALIVE"`  // Default: forever (0)
    HeimdallKeepAlive   time.Duration `env:"NORNICDB_HEIMDALL_KEEPALIVE"`   // Default: 5 minutes
    VisionKeepAlive     time.Duration `env:"NORNICDB_VISION_KEEPALIVE"`     // Default: 2 minutes
    
    // Preloading
    PreloadEmbedding bool `env:"NORNICDB_PRELOAD_EMBEDDING"` // Default: true
    PreloadHeimdall  bool `env:"NORNICDB_PRELOAD_HEIMDALL"`  // Default: false
    PreloadVision    bool `env:"NORNICDB_PRELOAD_VISION"`    // Default: false
    
    // Concurrent model limit (for memory-constrained systems)
    MaxConcurrentModels int `env:"NORNICDB_MAX_CONCURRENT_MODELS"` // Default: 3
}

// Defaults optimized for 8GB systems
func DefaultLifecycleConfig() LifecycleConfig {
    return LifecycleConfig{
        MaxVRAM:             0, // Unlimited (auto-detect)
        MaxRAM:              0, // Unlimited (auto-detect)
        EmbeddingKeepAlive:  0, // Never unload
        HeimdallKeepAlive:   5 * time.Minute,
        VisionKeepAlive:     2 * time.Minute,
        PreloadEmbedding:    true,
        PreloadHeimdall:     false,
        PreloadVision:       false,
        MaxConcurrentModels: 3,
    }
}
```

### Environment Variable Examples

```bash
# Memory-constrained system (8GB total)
export NORNICDB_MAX_VRAM=4294967296          # 4GB VRAM limit
export NORNICDB_MAX_RAM=6442450944           # 6GB RAM limit
export NORNICDB_HEIMDALL_KEEPALIVE=2m        # Unload Heimdall after 2 min idle
export NORNICDB_VISION_KEEPALIVE=30s         # Unload Vision after 30 sec
export NORNICDB_MAX_CONCURRENT_MODELS=2      # Only 2 models at once

# High-memory system (32GB+)
export NORNICDB_PRELOAD_HEIMDALL=true        # Keep Heimdall always loaded
export NORNICDB_PRELOAD_VISION=true          # Keep Vision always loaded
export NORNICDB_HEIMDALL_KEEPALIVE=0         # Never unload
export NORNICDB_VISION_KEEPALIVE=0           # Never unload

# Embedding-only mode (minimal memory)
export NORNICDB_HEIMDALL_ENABLED=false       # Disable Heimdall
export NORNICDB_VISION_ENABLED=false         # Disable Vision
# Only embedding model loaded (~1.5GB)
```

### Model States

```go
type ModelState string

const (
    ModelStateUnloaded  ModelState = "unloaded"   // Not in memory
    ModelStateLoading   ModelState = "loading"    // Currently loading
    ModelStateHot       ModelState = "hot"        // Loaded, recently used
    ModelStateWarm      ModelState = "warm"       // Loaded, idle but within keep-alive
    ModelStateCold      ModelState = "cold"       // Loaded, past keep-alive, candidate for eviction
    ModelStateEvicting  ModelState = "evicting"   // Being unloaded
)
```

### Lifecycle Manager Interface

```go
// pkg/models/manager.go

type ModelManager interface {
    // Acquire gets a model, loading it if necessary.
    // Blocks until model is ready or context is cancelled.
    Acquire(ctx context.Context, modelType ModelType) (Model, error)
    
    // Release signals that the caller is done with the model.
    // Model may be kept warm or scheduled for eviction.
    Release(modelType ModelType)
    
    // Preload loads a model without using it (for startup).
    Preload(ctx context.Context, modelType ModelType) error
    
    // Evict forces a model to unload immediately.
    Evict(modelType ModelType) error
    
    // Status returns the current state of all models.
    Status() map[ModelType]ModelStatus
    
    // MemoryUsage returns current memory consumption.
    MemoryUsage() MemoryStats
}

type ModelStatus struct {
    State       ModelState
    LoadedAt    time.Time
    LastUsedAt  time.Time
    UseCount    int64
    MemoryVRAM  int64
    MemoryRAM   int64
}

type MemoryStats struct {
    TotalVRAM     int64
    UsedVRAM      int64
    AvailableVRAM int64
    TotalRAM      int64
    UsedRAM       int64
    AvailableRAM  int64
    LoadedModels  []ModelType
}
```

### Eviction Algorithm

```go
// pkg/models/eviction.go

// EvictIfNeeded checks memory budget and evicts models if necessary.
// Uses priority-based LRU eviction.
func (m *Manager) EvictIfNeeded(requiredVRAM, requiredRAM int64) error {
    stats := m.MemoryUsage()
    
    // Check if we have enough memory
    vramNeeded := (stats.UsedVRAM + requiredVRAM) - m.config.MaxVRAM
    ramNeeded := (stats.UsedRAM + requiredRAM) - m.config.MaxRAM
    
    if vramNeeded <= 0 && ramNeeded <= 0 {
        return nil // No eviction needed
    }
    
    // Build eviction candidates (sorted by priority, then LRU)
    candidates := m.getEvictionCandidates()
    
    for _, candidate := range candidates {
        if vramNeeded <= 0 && ramNeeded <= 0 {
            break
        }
        
        // Don't evict embedding model (priority 1)
        if candidate.Type == ModelTypeEmbedding {
            continue
        }
        
        // Don't evict models currently in use
        if candidate.InUse {
            continue
        }
        
        // Evict this model
        if err := m.evictModel(candidate.Type); err != nil {
            log.Printf("[ModelManager] Failed to evict %s: %v", candidate.Type, err)
            continue
        }
        
        vramNeeded -= candidate.MemoryVRAM
        ramNeeded -= candidate.MemoryRAM
        
        log.Printf("[ModelManager] Evicted %s to free memory (VRAM: %d MB, RAM: %d MB)",
            candidate.Type,
            candidate.MemoryVRAM / 1024 / 1024,
            candidate.MemoryRAM / 1024 / 1024)
    }
    
    if vramNeeded > 0 || ramNeeded > 0 {
        return fmt.Errorf("unable to free enough memory: need VRAM=%d MB, RAM=%d MB",
            vramNeeded / 1024 / 1024, ramNeeded / 1024 / 1024)
    }
    
    return nil
}

// getEvictionCandidates returns models sorted by eviction priority.
// Lower priority + older last use = evicted first.
func (m *Manager) getEvictionCandidates() []EvictionCandidate {
    var candidates []EvictionCandidate
    
    for modelType, status := range m.Status() {
        if status.State == ModelStateUnloaded {
            continue
        }
        
        candidates = append(candidates, EvictionCandidate{
            Type:       modelType,
            Priority:   m.getPriority(modelType),
            LastUsed:   status.LastUsedAt,
            InUse:      status.UseCount > 0,
            MemoryVRAM: status.MemoryVRAM,
            MemoryRAM:  status.MemoryRAM,
        })
    }
    
    // Sort: lower priority first, then older last-use first
    sort.Slice(candidates, func(i, j int) bool {
        if candidates[i].Priority != candidates[j].Priority {
            return candidates[i].Priority > candidates[j].Priority // Higher number = lower priority
        }
        return candidates[i].LastUsed.Before(candidates[j].LastUsed)
    })
    
    return candidates
}
```

### Keep-Alive Timer

```go
// pkg/models/keepalive.go

// startKeepAliveTimer starts a goroutine that monitors idle time.
func (m *Manager) startKeepAliveTimer(modelType ModelType) {
    keepAlive := m.getKeepAlive(modelType)
    if keepAlive == 0 {
        return // Never evict
    }
    
    go func() {
        ticker := time.NewTicker(keepAlive / 2)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                status := m.getStatus(modelType)
                if status.State == ModelStateUnloaded {
                    return
                }
                
                idleTime := time.Since(status.LastUsedAt)
                if idleTime > keepAlive && status.UseCount == 0 {
                    log.Printf("[ModelManager] %s idle for %v, evicting", modelType, idleTime)
                    m.evictModel(modelType)
                    return
                }
                
            case <-m.ctx.Done():
                return
            }
        }
    }()
}
```

### Request Flow with Lifecycle Management

```
┌─────────────────────────────────────────────────────────────────┐
│  User creates :Image node                                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  ModelManager.Acquire(Vision)                                   │
│  └─ Vision model not loaded                                     │
│  └─ Check memory budget: need 3GB VRAM                          │
│  └─ Current: Embedding(1GB) + Heimdall(1GB) = 2GB               │
│  └─ Budget: 4GB → OK, load Vision                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Load Vision model (~2 seconds)                                 │
│  └─ GPU memory: 3GB allocated                                   │
│  └─ State: Hot                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Process image → Generate description → Generate embedding      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  ModelManager.Release(Vision)                                   │
│  └─ State: Hot → Warm                                           │
│  └─ Start keep-alive timer (2 minutes)                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                     (2 minutes pass, no more images)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Keep-alive timer fires                                         │
│  └─ Vision idle for 2m                                          │
│  └─ Evict Vision model                                          │
│  └─ Free 3GB VRAM                                               │
│  └─ State: Unloaded                                             │
└─────────────────────────────────────────────────────────────────┘
```

### Memory Profiles

Pre-configured profiles for common system sizes:

```yaml
# profiles.yaml

profiles:
  # 8GB system (e.g., MacBook Air M1)
  constrained:
    max_vram: 4GB
    max_ram: 6GB
    max_concurrent_models: 2
    embedding_keepalive: 0      # Always loaded
    heimdall_keepalive: 2m
    vision_keepalive: 30s
    preload_heimdall: false
    preload_vision: false
    
  # 16GB system (e.g., MacBook Pro M1)
  balanced:
    max_vram: 8GB
    max_ram: 12GB
    max_concurrent_models: 3
    embedding_keepalive: 0      # Always loaded
    heimdall_keepalive: 5m
    vision_keepalive: 2m
    preload_heimdall: true
    preload_vision: false
    
  # 32GB+ system (e.g., Mac Studio)
  performance:
    max_vram: 0                 # Unlimited
    max_ram: 0                  # Unlimited
    max_concurrent_models: 3
    embedding_keepalive: 0      # Always loaded
    heimdall_keepalive: 0       # Always loaded
    vision_keepalive: 0         # Always loaded
    preload_heimdall: true
    preload_vision: true

# Usage: NORNICDB_MEMORY_PROFILE=balanced
```

### Monitoring & Metrics

```go
// Prometheus metrics
var (
    modelLoadTime = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "nornicdb_model_load_seconds",
            Help:    "Time to load models",
            Buckets: []float64{0.5, 1, 2, 5, 10, 30},
        },
        []string{"model_type"},
    )
    
    modelMemoryUsage = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "nornicdb_model_memory_bytes",
            Help: "Memory usage per model",
        },
        []string{"model_type", "memory_type"}, // memory_type: vram, ram
    )
    
    modelEvictions = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "nornicdb_model_evictions_total",
            Help: "Number of model evictions",
        },
        []string{"model_type", "reason"}, // reason: idle, memory_pressure, manual
    )
    
    modelState = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "nornicdb_model_state",
            Help: "Current model state (0=unloaded, 1=loading, 2=hot, 3=warm, 4=cold)",
        },
        []string{"model_type"},
    )
)
```

### API for Model Management

```cypher
// Check model status
CALL db.models.status()
YIELD model, state, memoryMB, lastUsed, useCount

// Force load a model
CALL db.models.load('vision')
YIELD success, loadTimeMs

// Force evict a model
CALL db.models.evict('vision')
YIELD success, freedMemoryMB

// Get memory stats
CALL db.models.memory()
YIELD totalVRAM, usedVRAM, totalRAM, usedRAM, loadedModels
```

```bash
# HTTP API
GET /api/models/status
{
  "models": {
    "embedding": {"state": "hot", "memory_mb": 1024, "last_used": "2024-12-03T10:00:00Z"},
    "heimdall": {"state": "warm", "memory_mb": 1536, "last_used": "2024-12-03T09:55:00Z"},
    "vision": {"state": "unloaded", "memory_mb": 0, "last_used": null}
  },
  "memory": {
    "vram_used_mb": 2560,
    "vram_total_mb": 8192,
    "ram_used_mb": 3072,
    "ram_total_mb": 16384
  }
}

POST /api/models/evict
{"model": "vision"}

POST /api/models/load
{"model": "heimdall"}
```

### Startup Sequence

```
┌─────────────────────────────────────────────────────────────────┐
│  NornicDB Startup                                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  1. Initialize ModelManager                                     │
│     └─ Detect available VRAM/RAM                                │
│     └─ Apply memory profile (constrained/balanced/performance)  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. Preload Embedding Model (always)                            │
│     └─ Load BGE-M3                                              │
│     └─ State: Hot                                               │
│     └─ Log: "✅ Embedding model ready (1024 MB)"                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. Conditionally Preload Heimdall                              │
│     └─ If NORNICDB_PRELOAD_HEIMDALL=true                        │
│     └─ Load Qwen2.5-0.5B                                        │
│     └─ Log: "✅ Heimdall AI Assistant ready (512 MB)"           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. Conditionally Preload Vision                                │
│     └─ If NORNICDB_PRELOAD_VISION=true                          │
│     └─ Check memory budget first                                │
│     └─ Load Qwen2.5-VL-2B                                       │
│     └─ Log: "✅ Vision pipeline ready (2048 MB)"                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  5. Ready for requests                                          │
│     └─ Log: "🚀 NornicDB ready (models: embedding, heimdall)"   │
│     └─ Log: "   Vision: on-demand (2min keep-alive)"            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 15. Implementation Roadmap

### Phase 1: Model Lifecycle Manager (Week 1-2)
- [ ] Create `pkg/models` package
- [ ] Implement ModelManager interface
- [ ] Add memory detection (VRAM/RAM)
- [ ] Implement acquire/release pattern
- [ ] Add keep-alive timers
- [ ] Implement priority-based LRU eviction
- [ ] Add memory profiles (constrained/balanced/performance)
- [ ] Prometheus metrics for model states
- [ ] Unit tests for eviction algorithm

### Phase 2: Vision Foundation (Week 3-4)
- [ ] Create `pkg/vision` package
- [ ] Implement types and interfaces
- [ ] Add configuration support
- [ ] Implement node detection logic
- [ ] Integrate with ModelManager

### Phase 3: Image Processing (Week 5-6)
- [ ] Implement image decoder
- [ ] Implement image scaler (bilinear + Lanczos options)
- [ ] Add MIME type detection from magic bytes
- [ ] Handle URL fetching with timeouts
- [ ] Add image validation and security checks

### Phase 4: VL Integration (Week 7-8)
- [ ] Extend llama.cpp bindings for vision (LLaVA architecture)
- [ ] Implement LlamaVisionGenerator
- [ ] Test with Qwen2.5-VL, MoonDream, LLaVA models
- [ ] Integration with ModelManager for lifecycle
- [ ] GPU memory tracking

### Phase 5: Embedding Pipeline Integration (Week 9-10)
- [ ] Modify Embedder to detect image nodes
- [ ] Implement vision pipeline routing
- [ ] Implement context combination
- [ ] Store `_vl_description` and `_vl_processed` properties
- [ ] Add automatic embedding on node creation
- [ ] Batch processing support

### Phase 6: API & Monitoring (Week 11-12)
- [ ] Add Cypher procedures (db.vision.*, db.models.*)
- [ ] Add HTTP API endpoints
- [ ] Prometheus metrics for vision pipeline
- [ ] Grafana dashboard templates
- [ ] Memory usage alerts

### Phase 7: Docker & Documentation (Week 13-14)
- [ ] Create Docker build target with VL model
- [ ] Download and test recommended models
- [ ] Write user documentation
- [ ] Add examples and tutorials
- [ ] Performance benchmarks
- [ ] Memory profile recommendations

---

## 15. Security Considerations

| Concern | Mitigation |
|---------|------------|
| Large image DoS | MaxPixels limit (3.2MP default) |
| Malformed images | Strict image validation before processing |
| URL fetching risks | Timeout limits, size limits, allowed hosts |
| Model prompt injection | Sanitize image metadata before combining |
| Resource exhaustion | Queue limits, concurrent processing limits |

---

## 16. Future Enhancements

### Short Term
- [ ] Support for image URLs with authentication
- [ ] Batch image processing API
- [ ] Custom prompts per node label

### Medium Term
- [ ] Multi-image nodes (galleries)
- [ ] Video frame extraction
- [ ] OCR-specific mode for documents
- [ ] Image similarity search (CLIP-style)

### Long Term
- [ ] Real-time image stream processing
- [ ] Image generation integration
- [ ] Visual question answering (VQA)
- [ ] Image-to-graph extraction (scene graphs)

---

## 17. API Reference

### Cypher Procedures (Proposed)

```cypher
// Process a single image node
CALL db.vision.process(nodeId) YIELD description, embedding

// Batch process all Image nodes
CALL db.vision.processAll() YIELD processed, errors

// Get vision pipeline status
CALL db.vision.status() YIELD enabled, model, processed_count
```

### HTTP Endpoints (Proposed)

```bash
# Process an image directly
POST /api/vision/describe
Content-Type: multipart/form-data
image: <binary>
prompt: "Describe this image"

# Response
{
  "description": "A sunset over mountains...",
  "processing_time_ms": 523
}
```

---

**Document Version:** 1.0.0  
**Last Updated:** December 2024  
**Status:** PROPOSAL - Ready for implementation
