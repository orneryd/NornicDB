# Model Requirements

Complete guide to AI models used in NornicDB for iOS.

## Model Overview

| Model Type | Required | Size | Purpose |
|------------|----------|------|---------|
| Embedding | ✅ Yes | 67-560 MB | Semantic search |
| LLM | ❌ Optional | 500MB-2.3GB | Summarization |
| Tokenizer | ✅ Yes | ~500 KB | Text processing |

## Embedding Models

### Recommended: all-MiniLM-L6-v2

**Best balance of quality and size**

| Property | Value |
|----------|-------|
| Size | ~90 MB (Core ML) |
| Dimensions | 384 |
| Max Sequence | 256 tokens |
| Speed | ~10ms on Neural Engine |
| Source | [HuggingFace](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) |

```swift
// Usage
let embedder = try MiniLMEmbedder()
let embedding = try embedder.embed("Hello world")  // [Float] with 384 dims
```

### Alternative: gte-small

**Smallest footprint**

| Property | Value |
|----------|-------|
| Size | ~67 MB (Core ML) |
| Dimensions | 384 |
| Max Sequence | 512 tokens |
| Speed | ~8ms on Neural Engine |
| Source | [HuggingFace](https://huggingface.co/thenlper/gte-small) |

### Full Featured: BGE-M3

**Same as macOS NornicDB**

| Property | Value |
|----------|-------|
| Size | ~560 MB (Core ML) |
| Dimensions | 1024 |
| Max Sequence | 8192 tokens |
| Speed | ~50ms on Neural Engine |
| Features | Multilingual, dense + sparse |
| Source | [HuggingFace](https://huggingface.co/BAAI/bge-m3) |

## LLM Models (Optional)

### Recommended: Qwen2.5-0.5B-Instruct

**Lightweight, fast responses**

| Property | Value |
|----------|-------|
| Size | ~500 MB (GGUF Q4) |
| Context | 4096 tokens |
| Speed | ~40 tok/s on A17 |
| RAM | ~600 MB active |
| Source | [HuggingFace](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF) |

### Full Featured: Phi-3-mini-4k-instruct

**Best quality responses**

| Property | Value |
|----------|-------|
| Size | ~2.3 GB (GGUF Q4) |
| Context | 4096 tokens |
| Speed | ~20 tok/s on A17 Pro |
| RAM | ~2.5 GB active |
| Min Device | iPhone 14 Pro |
| Source | [HuggingFace](https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf) |

### No LLM: Template-Based

**Zero additional storage**

```swift
// Template-based response generation
func generateResponse(memories: [Memory], query: String) -> String {
    let count = memories.count
    let top = memories.first?.content.prefix(100) ?? ""
    let topics = memories.flatMap { $0.tags }.unique().prefix(5)
    
    return """
    Found \(count) memories related to "\(query)".
    
    Most relevant: "\(top)..."
    
    Topics: \(topics.joined(separator: ", "))
    
    Related connections: \(memories.map { $0.connections }.reduce(0, +))
    """
}
```

## Model Configuration

### Presets

```swift
enum ModelConfiguration {
    case minimal    // gte-small, no LLM (70 MB)
    case standard   // MiniLM, no LLM (90 MB)
    case full       // MiniLM + Qwen (590 MB)
    case maximum    // BGE-M3 + Phi-3 (2.9 GB)
}
```

### Storage Locations

```
App Bundle (ships with app):
├── Models/
│   ├── embedder.mlmodelc/     # Core ML embedding model
│   └── tokenizer.json         # BPE tokenizer
│
Documents (user data):
├── models/
│   └── user_models/           # User-added models
│
Caches (re-downloadable):
├── models/
│   ├── qwen2.5-0.5b-q4.gguf  # Optional LLM
│   └── phi-3-mini-q4.gguf    # Optional LLM
```

## Model Download Manager

```swift
class ModelDownloader {
    static let shared = ModelDownloader()
    
    enum Model: String {
        case qwen = "qwen2.5-0.5b-instruct-q4.gguf"
        case phi3 = "phi-3-mini-4k-instruct-q4.gguf"
        
        var url: URL {
            switch self {
            case .qwen:
                return URL(string: "https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf")!
            case .phi3:
                return URL(string: "https://huggingface.co/microsoft/Phi-3-mini-4k-instruct-gguf/resolve/main/Phi-3-mini-4k-instruct-q4.gguf")!
            }
        }
        
        var size: Int64 {
            switch self {
            case .qwen: return 500_000_000
            case .phi3: return 2_300_000_000
            }
        }
    }
    
    func download(
        _ model: Model,
        progress: @escaping (Double) -> Void
    ) async throws -> URL {
        let cacheDir = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
        let modelDir = cacheDir.appendingPathComponent("models")
        try FileManager.default.createDirectory(at: modelDir, withIntermediateDirectories: true)
        
        let destination = modelDir.appendingPathComponent(model.rawValue)
        
        // Check if already downloaded
        if FileManager.default.fileExists(atPath: destination.path) {
            return destination
        }
        
        // Download with progress
        let (tempURL, response) = try await URLSession.shared.download(from: model.url) { bytesWritten, totalBytesWritten, totalBytesExpectedToWrite in
            let progress = Double(totalBytesWritten) / Double(totalBytesExpectedToWrite)
            DispatchQueue.main.async {
                progress(progress)
            }
        }
        
        try FileManager.default.moveItem(at: tempURL, to: destination)
        return destination
    }
    
    func isDownloaded(_ model: Model) -> Bool {
        let cacheDir = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
        let path = cacheDir.appendingPathComponent("models/\(model.rawValue)").path
        return FileManager.default.fileExists(atPath: path)
    }
    
    func deleteModel(_ model: Model) throws {
        let cacheDir = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
        let path = cacheDir.appendingPathComponent("models/\(model.rawValue)")
        try FileManager.default.removeItem(at: path)
    }
}
```

## Core ML Conversion

### From HuggingFace to Core ML

```bash
# Install dependencies
pip install coremltools transformers torch onnx

# Convert model
python convert_to_coreml.py
```

```python
# convert_to_coreml.py
import coremltools as ct
from transformers import AutoModel, AutoTokenizer
import torch
import numpy as np

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# Load model
model = AutoModel.from_pretrained(MODEL_NAME)
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

# Create wrapper for mean pooling
class MeanPoolingModel(torch.nn.Module):
    def __init__(self, base_model):
        super().__init__()
        self.model = base_model
    
    def forward(self, input_ids, attention_mask):
        output = self.model(input_ids, attention_mask=attention_mask)
        token_embeddings = output.last_hidden_state
        
        # Mean pooling
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
        sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
        
        return sum_embeddings / sum_mask

pooling_model = MeanPoolingModel(model)
pooling_model.eval()

# Export to ONNX
dummy_input = tokenizer(
    "test sentence",
    return_tensors="pt",
    padding="max_length",
    max_length=128,
    truncation=True
)

torch.onnx.export(
    pooling_model,
    (dummy_input["input_ids"], dummy_input["attention_mask"]),
    "embedder.onnx",
    input_names=["input_ids", "attention_mask"],
    output_names=["embeddings"],
    dynamic_axes={
        "input_ids": {0: "batch", 1: "sequence"},
        "attention_mask": {0: "batch", 1: "sequence"},
        "embeddings": {0: "batch"}
    },
    opset_version=14
)

# Convert to Core ML
mlmodel = ct.convert(
    "embedder.onnx",
    minimum_deployment_target=ct.target.iOS17,
    compute_units=ct.ComputeUnit.ALL,
    convert_to="mlprogram"
)

# Quantize to float16
mlmodel_fp16 = ct.models.neural_network.quantization_utils.quantize_weights(
    mlmodel,
    nbits=16
)

# Save
mlmodel_fp16.save("embedder.mlpackage")
print("✅ Model converted to Core ML")
```

## Tokenizer

### BPE Tokenizer Implementation

```swift
class BPETokenizer {
    private let vocab: [String: Int]
    private let merges: [(String, String)]
    private let specialTokens: [String: Int]
    
    init(vocabPath: URL, mergesPath: URL) throws {
        // Load vocabulary
        let vocabData = try Data(contentsOf: vocabPath)
        vocab = try JSONDecoder().decode([String: Int].self, from: vocabData)
        
        // Load merges
        let mergesText = try String(contentsOf: mergesPath)
        merges = mergesText.split(separator: "\n").compactMap { line in
            let parts = line.split(separator: " ")
            guard parts.count == 2 else { return nil }
            return (String(parts[0]), String(parts[1]))
        }
        
        // Special tokens
        specialTokens = [
            "[CLS]": 101,
            "[SEP]": 102,
            "[PAD]": 0,
            "[UNK]": 100
        ]
    }
    
    func encode(_ text: String, maxLength: Int = 128) -> (inputIds: [Int], attentionMask: [Int]) {
        // Normalize
        let normalized = text.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
        
        // Tokenize
        var tokens = tokenize(normalized)
        
        // Truncate
        if tokens.count > maxLength - 2 {
            tokens = Array(tokens.prefix(maxLength - 2))
        }
        
        // Add special tokens
        tokens = ["[CLS]"] + tokens + ["[SEP]"]
        
        // Convert to IDs
        var inputIds = tokens.map { vocab[$0] ?? specialTokens["[UNK]"]! }
        var attentionMask = [Int](repeating: 1, count: inputIds.count)
        
        // Pad
        while inputIds.count < maxLength {
            inputIds.append(specialTokens["[PAD]"]!)
            attentionMask.append(0)
        }
        
        return (inputIds, attentionMask)
    }
    
    private func tokenize(_ text: String) -> [String] {
        // Basic word tokenization + BPE
        let words = text.split(separator: " ").map { String($0) }
        var tokens: [String] = []
        
        for word in words {
            tokens.append(contentsOf: bpe(word))
        }
        
        return tokens
    }
    
    private func bpe(_ word: String) -> [String] {
        // Apply BPE merges
        var chars = word.map { String($0) }
        
        while chars.count > 1 {
            var minIdx = -1
            var minRank = Int.max
            
            for i in 0..<(chars.count - 1) {
                let pair = (chars[i], chars[i + 1])
                if let rank = merges.firstIndex(where: { $0 == pair }) {
                    if rank < minRank {
                        minRank = rank
                        minIdx = i
                    }
                }
            }
            
            if minIdx == -1 { break }
            
            chars[minIdx] = chars[minIdx] + chars[minIdx + 1]
            chars.remove(at: minIdx + 1)
        }
        
        return chars
    }
}
```

## Model Selection UI

```swift
struct ModelSettingsView: View {
    @StateObject private var settings = ModelSettings.shared
    
    var body: some View {
        Form {
            Section("Embedding Model") {
                Picker("Model", selection: $settings.embeddingModel) {
                    Text("MiniLM (90 MB)").tag(EmbeddingModel.miniLM)
                    Text("GTE Small (67 MB)").tag(EmbeddingModel.gteSmall)
                    Text("BGE-M3 (560 MB)").tag(EmbeddingModel.bgeM3)
                }
                
                Text("Current: \(settings.embeddingModel.displayName)")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Section("Language Model (Optional)") {
                Toggle("Enable LLM", isOn: $settings.llmEnabled)
                
                if settings.llmEnabled {
                    Picker("Model", selection: $settings.llmModel) {
                        Text("Qwen 0.5B (500 MB)").tag(LLMModel.qwen)
                        Text("Phi-3 Mini (2.3 GB)").tag(LLMModel.phi3)
                    }
                    
                    if !ModelDownloader.shared.isDownloaded(settings.llmModel.downloadable) {
                        Button("Download Model") {
                            downloadModel()
                        }
                    } else {
                        Label("Downloaded", systemImage: "checkmark.circle.fill")
                            .foregroundColor(.green)
                    }
                }
            }
            
            Section("Storage") {
                LabeledContent("Embedding Model", value: settings.embeddingModel.sizeString)
                if settings.llmEnabled {
                    LabeledContent("LLM", value: settings.llmModel.sizeString)
                }
                LabeledContent("Total", value: totalSizeString)
            }
        }
    }
}
```

## Performance Comparison

| Model | Quality | Speed | Size | Recommendation |
|-------|---------|-------|------|----------------|
| gte-small | ★★★☆☆ | ★★★★★ | 67 MB | Low storage devices |
| MiniLM | ★★★★☆ | ★★★★☆ | 90 MB | **Default choice** |
| BGE-M3 | ★★★★★ | ★★★☆☆ | 560 MB | Power users |
| Qwen 0.5B | ★★★☆☆ | ★★★★★ | 500 MB | Quick responses |
| Phi-3 Mini | ★★★★★ | ★★★☆☆ | 2.3 GB | Best quality |

## Next Steps

1. [API Reference →](API.md)
2. [Roadmap →](ROADMAP.md)
3. [Architecture →](ARCHITECTURE.md)

