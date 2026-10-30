# Apple ML Multimodal Embedding Server

**Version:** 1.0.0  
**Last Updated:** 2025-12-10  
**Status:** Design Document (Implementation Pending)

---

## Overview

The Apple ML Multimodal Embedding Server extends the current text-only embedding server to support **images, videos, PDFs, and documents**. The key insight is that all content types are converted to **text representations** before embedding, making the system truly multimodal while maintaining a single embedding space.

**Core Principle:** Binary content → Text extraction → Text embedding (512 dims)

---

## Current State vs Proposed Extension

### Current: Text-Only Embeddings

```
NornicDB Server                      Apple ML Server
─────────────────                    ──────────────────

1. Node: "Hello world"
   ↓
2. POST /v1/embeddings
   ──────────────────────────────────►
   {
     "input": ["Hello world"],
     "model": "apple-ml-embeddings"
   }
                                  3. NLEmbedding.generate("Hello world")
                                     ↓
4. Response                       [512 floats]
   ◄──────────────────────────────────
   {"data": [{"embedding": [512 values]}]}
```

### Proposed: Multimodal INPUT → Text Embedding

```
NornicDB Server                      Apple ML Server
─────────────────                    ──────────────────

SCENARIO 1: Image with Text
────────────────────────────
1. Node: Image file
   /photos/receipt.jpg
   ↓
2. POST /v1/embeddings
   ──────────────────────────────────►
   {
     "input": ["data:image/jpeg;base64,/9j/4AAQ..."],
     "model": "apple-ml-multimodal",
     "extract": ["ocr", "description"]
   }
                                  3. Decode image
                                     ↓
                                  4. Vision OCR
                                     VNRecognizeTextRequest
                                     → "Receipt from Acme Store
                                        Total: $42.99
                                        Date: 12/09/2025"
                                     ↓
                                  5. Scene Classification
                                     VNClassifyImageRequest
                                     → ["document", "receipt", "text"]
                                     ↓
                                  6. Combine extracted text
                                     "Receipt from Acme Store Total: $42.99
                                      [Scene: document, receipt, text]"
                                     ↓
                                  7. NLEmbedding.generate(combined_text)
                                     ↓
8. Response                       [512 floats]
   ◄──────────────────────────────────
   {
     "data": [{
       "embedding": [512 values],
       "extracted_text": "Receipt from Acme Store...",
       "ocr_confidence": 0.93,
       "scene_labels": ["document", "receipt", "text"]
     }]
   }

SCENARIO 2: PDF Document
────────────────────────
2. POST /v1/embeddings
   ──────────────────────────────────►
   {
     "input": ["data:application/pdf;base64,JVBERi0..."],
     "model": "apple-ml-multimodal",
     "extract": ["text", "metadata"]
   }
                                  3. PDFKit extraction
                                     PDFDocument(data: pdfData)
                                     .string
                                     ↓
                                     "Executive Summary
                                      Q4 results show 23% growth..."
                                     ↓
                                  4. Extract metadata
                                     .documentAttributes
                                     → Title, Author, Keywords
                                     ↓
                                  5. Combine
                                     "Title: Q4 Report
                                      Author: Finance Team
                                      Keywords: revenue, growth
                                      Content: Executive Summary..."
                                     ↓
                                  6. NLEmbedding.generate(enriched_text)
8. Response
   ◄──────────────────────────────────
   {
     "data": [{
       "embedding": [512 values],
       "extracted_text": "Executive Summary Q4...",
       "metadata": {
         "title": "Q4 Report",
         "author": "Finance Team",
         "pages": 42
       }
     }]
   }

SCENARIO 3: Plain Text (Unchanged)
───────────────────────────────────
2. POST /v1/embeddings
   ──────────────────────────────────►
   {
     "input": ["Hello world"],
     "model": "apple-ml-embeddings"
   }
                                  3. NLEmbedding.generate("Hello world")
5. Response
   ◄──────────────────────────────────
   (same as before - backward compatible)
```

---

## Apple Vision Framework Video Capabilities

### Available APIs (macOS/iOS)

| API | Purpose | Video Support |
|-----|---------|---------------|
| `VNVideoProcessor` | Process video frames efficiently | ✅ Native video support |
| `VNTrackObjectRequest` | Track objects across frames | ✅ Temporal tracking |
| `VNClassifyImageRequest` | Scene classification per frame | ✅ Per-frame analysis |
| `VNRecognizeTextRequest` | OCR on video frames | ✅ Extract on-screen text |
| `VNDetectHumanBodyPoseRequest` | Human pose/activity detection | ✅ Gesture recognition |
| `SFSpeechRecognizer` | Audio transcription | ✅ Extract speech |

### What Apple Does NOT Provide

- ❌ Direct "video description" API
- ❌ Video captioning ("a person is cooking pasta")
- ❌ Video summarization ("5-minute cooking tutorial")
- ❌ Action description ("chef adds salt to boiling water")

### What We Can Build

✅ **Aggregate frame analysis** → descriptive text → embedding
- Sample keyframes
- Classify each frame
- Extract OCR from frames
- Transcribe audio track
- Combine all text → single embedding

---

## Video Processing Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Video → Text → Embedding Pipeline                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Input: video.mp4                                                            │
│     ↓                                                                        │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │  1. Video Sampling Strategy                                  │           │
│  │     • Sample keyframes (every 1-2 seconds)                   │           │
│  │     • Or: Scene change detection                             │           │
│  │     • Or: Uniform sampling (10 frames total)                 │           │
│  └───────────────────────┬──────────────────────────────────────┘           │
│                          │                                                   │
│                          ▼                                                   │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │  2. Per-Frame Analysis (VNVideoProcessor)                    │           │
│  │                                                               │           │
│  │  For each sampled frame:                                     │           │
│  │    A. Scene Classification                                   │           │
│  │       VNClassifyImageRequest → ["cooking", "kitchen"]        │           │
│  │                                                               │           │
│  │    B. OCR (if text visible)                                  │           │
│  │       VNRecognizeTextRequest → "Recipe: Pasta Carbonara"     │           │
│  │                                                               │           │
│  │    C. Object Detection                                       │           │
│  │       VNDetectObjectRequest → ["pot", "stove", "ingredients"]│           │
│  └───────────────────────┬──────────────────────────────────────┘           │
│                          │                                                   │
│                          ▼                                                   │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │  3. Aggregate Frame Results                                  │           │
│  │     • Top 5 scene labels (by frequency)                      │           │
│  │     • All OCR text combined                                  │           │
│  │     • Common objects detected                                │           │
│  │                                                               │           │
│  │  Result: "cooking kitchen recipe pasta carbonara pot stove"  │           │
│  └───────────────────────┬──────────────────────────────────────┘           │
│                          │                                                   │
│                          ▼                                                   │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │  4. Audio Track Processing (SFSpeechRecognizer)              │           │
│  │     • Extract audio from video                               │           │
│  │     • Transcribe speech → text                               │           │
│  │     • Add to combined text                                   │           │
│  │                                                               │           │
│  │  Result: "In this video about cooking pasta, the chef        │           │
│  │           demonstrates how to make carbonara sauce..."        │           │
│  └───────────────────────┬──────────────────────────────────────┘           │
│                          │                                                   │
│                          ▼                                                   │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │  5. Final Text Combination                                   │           │
│  │     "cooking kitchen recipe pasta carbonara pot stove        │           │
│  │      Recipe: Pasta Carbonara                                 │           │
│  │      In this video about cooking pasta, the chef             │           │
│  │      demonstrates how to make carbonara sauce..."            │           │
│  └───────────────────────┬──────────────────────────────────────┘           │
│                          │                                                   │
│                          ▼                                                   │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │  6. NLEmbedding.generate(final_text)                         │           │
│  │     Returns: [512 floats]                                    │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Complete Multimodal Support Matrix

```
┌──────────────────┬─────────────────────┬────────────────────┬──────────────┐
│  Media Type      │  Apple Framework    │  What It Extracts  │  Complexity  │
├──────────────────┼─────────────────────┼────────────────────┼──────────────┤
│  📝 Text         │  (none needed)      │  Text itself       │  ✅ Current  │
├──────────────────┼─────────────────────┼────────────────────┼──────────────┤
│  🖼️  Image       │  Vision Framework   │  • OCR text        │  ⚡ Easy     │
│                  │  • VNRecognizeText  │  • Scene labels    │  (~200 LOC)  │
│                  │  • VNClassifyImage  │  • Objects         │              │
├──────────────────┼─────────────────────┼────────────────────┼──────────────┤
│  📄 PDF          │  PDFKit             │  • Full text       │  ⚡ Easy     │
│                  │                     │  • Metadata        │  (~100 LOC)  │
├──────────────────┼─────────────────────┼────────────────────┼──────────────┤
│  📝 RTF/DOCX     │  NSAttributedString │  • Plain text      │  ⚡ Easy     │
│                  │                     │  • Formatting      │  (~50 LOC)   │
├──────────────────┼─────────────────────┼────────────────────┼──────────────┤
│  🎬 Video        │  Vision Framework   │  • Frame labels    │  🔧 Moderate │
│                  │  • VNVideoProcessor │  • OCR per frame   │  (~400 LOC)  │
│                  │  • VNClassifyImage  │  • Objects         │              │
│                  │  + SFSpeechRecog.   │  • Audio→text      │              │
├──────────────────┼─────────────────────┼────────────────────┼──────────────┤
│  🎵 Audio        │  SFSpeechRecognizer │  • Transcription   │  ⚡ Easy     │
│                  │  (Speech Framework) │  • Language detect │  (~150 LOC)  │
└──────────────────┴─────────────────────┴────────────────────┴──────────────┘
```

---

## Enhanced API Specification

### Request Format

```json
POST /v1/embeddings
{
  "input": [
    "<text string>" OR
    "data:<mime-type>;base64,<encoded-data>"
  ],
  "model": "apple-ml-embeddings" | "apple-ml-multimodal",
  "extract": ["ocr", "description", "transcription", "metadata"],
  "video_sampling": {
    "strategy": "keyframes" | "uniform" | "scene_change",
    "frame_count": 10,
    "include_audio": true
  }
}
```

### Response Format (Enhanced)

```json
{
  "data": [{
    "embedding": [512 floats],
    "index": 0,
    "object": "embedding",
    
    // NEW: Extracted content (optional, only for multimodal)
    "extracted_text": "Combined text from all sources",
    "ocr_text": "Text extracted via OCR",
    "ocr_confidence": 0.93,
    "scene_labels": ["category1", "category2"],
    "transcription": "Audio transcription",
    "metadata": {
      "title": "Document title",
      "author": "Author name",
      "pages": 42,
      "duration_seconds": 45.2
    }
  }],
  "usage": {
    "prompt_tokens": 1,
    "total_tokens": 1
  }
}
```

---

## Architecture: Content-to-Text-to-Embedding Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              Apple ML Multimodal Embedding Server (Enhanced)                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  1. Request Handler                                             │        │
│  │     • Parse input                                                │        │
│  │     • Detect content type                                        │        │
│  └──────────────────────────┬──────────────────────────────────────┘        │
│                             │                                                │
│              ┌──────────────┼──────────────┐                                │
│              │              │              │                                │
│              ▼              ▼              ▼                                │
│  ┌─────────────────┐ ┌─────────────┐ ┌──────────────────┐                 │
│  │ Plain Text      │ │ Image Data  │ │ Document Data    │                 │
│  │ "hello..."      │ │ Base64/PNG  │ │ Base64/PDF/DOC   │                 │
│  └────────┬────────┘ └──────┬──────┘ └────────┬─────────┘                 │
│           │                 │                 │                             │
│           │                 ▼                 ▼                             │
│           │   ┌───────────────────────────────────────────┐                │
│           │   │  2. Content Extraction Pipeline           │                │
│           │   ├───────────────────────────────────────────┤                │
│           │   │                                           │                │
│           │   │  ┌─────────────────────────────────────┐ │                │
│           │   │  │ Image Processing                    │ │                │
│           │   │  │ • Decode base64 → NSImage           │ │                │
│           │   │  │ • Convert to CIImage                │ │                │
│           │   │  │                                     │ │                │
│           │   │  │ A. OCR (VNRecognizeTextRequest)    │ │                │
│           │   │  │    → Extract visible text          │ │                │
│           │   │  │    → Confidence per word           │ │                │
│           │   │  │                                     │ │                │
│           │   │  │ B. Scene Classification            │ │                │
│           │   │  │    (VNClassifyImageRequest)        │ │                │
│           │   │  │    → Categories: "animal", "food"  │ │                │
│           │   │  │    → Confidence per label          │ │                │
│           │   │  │                                     │ │                │
│           │   │  │ C. Object Detection (optional)     │ │                │
│           │   │  │    → "dog", "tree", "car"          │ │                │
│           │   │  └─────────────────────────────────────┘ │                │
│           │   │                                           │                │
│           │   │  ┌─────────────────────────────────────┐ │                │
│           │   │  │ Document Processing                 │ │                │
│           │   │  │ • PDFKit: Extract text from PDF     │ │                │
│           │   │  │ • NSAttributedString: RTF parsing   │ │                │
│           │   │  │ • UniformTypeIdentifiers: Detect    │ │                │
│           │   │  └─────────────────────────────────────┘ │                │
│           │   │                                           │                │
│           │   │  ┌─────────────────────────────────────┐ │                │
│           │   │  │ Video Processing                    │ │                │
│           │   │  │ • AVAsset: Load video               │ │                │
│           │   │  │ • Sample 10 keyframes               │ │                │
│           │   │  │ • Per-frame: OCR + classification   │ │                │
│           │   │  │ • Extract audio → transcribe        │ │                │
│           │   │  └─────────────────────────────────────┘ │                │
│           │   │                                           │                │
│           │   └───────────────────┬───────────────────────┘                │
│           │                       │                                         │
│           │         ┌─────────────▼─────────────┐                           │
│           │         │  3. Text Combiner         │                           │
│           │         │  Merge: OCR + Description │                           │
│           │         │         + Extracted Text  │                           │
│           │         └─────────────┬─────────────┘                           │
│           │                       │                                         │
│           └───────────────────────┤                                         │
│                                   │                                         │
│                     ┌─────────────▼─────────────┐                           │
│                     │  4. NLEmbedding Generator │                           │
│                     │  (Apple Framework)        │                           │
│                     │  sentenceEmbedding(for:)  │                           │
│                     └─────────────┬─────────────┘                           │
│                                   │                                         │
│                     ┌─────────────▼─────────────┐                           │
│                     │  5. Return Enhanced       │                           │
│                     │  {                        │                           │
│                     │    embedding: [512],      │                           │
│                     │    extracted_text: "...", │                           │
│                     │    ocr_text: "...",       │                           │
│                     │    scene_labels: [...]    │                           │
│                     │  }                        │                           │
│                     └───────────────────────────┘                           │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Image Support (⚡ Easy - ~200 LOC)

**New Files:**
- `AppleVisionExtractor.swift` - OCR + scene classification

**Changes to `EmbeddingServer.swift`:**
```swift
// Add input type detection
private func detectInputType(_ input: String) -> InputType {
    if input.hasPrefix("data:image/") { return .image }
    if input.hasPrefix("data:application/pdf") { return .pdf }
    return .text
}

// Add image processor
private func processImage(_ data: Data) -> (text: String, labels: [String], confidence: Double) {
    // VNRecognizeTextRequest for OCR
    // VNClassifyImageRequest for scene labels
    // Combine results
}
```

**API Example:**
```bash
curl -X POST http://127.0.0.1:11435/v1/embeddings \
  -H "Authorization: Bearer <key>" \
  -H "Content-Type: application/json" \
  -d '{
    "input": ["data:image/jpeg;base64,/9j/4AAQ..."],
    "model": "apple-ml-multimodal"
  }'
```

---

### Phase 2: PDF/Document Support (⚡ Easy - ~100 LOC)

**New Files:**
- `DocumentExtractor.swift` - PDF/RTF text extraction

**Changes:**
```swift
private func processPDF(_ data: Data) -> (text: String, metadata: [String: Any]) {
    let pdfDoc = PDFDocument(data: data)
    let text = pdfDoc?.string ?? ""
    let metadata = pdfDoc?.documentAttributes ?? [:]
    return (text, metadata)
}

private func processRTF(_ data: Data) -> String {
    let attributed = NSAttributedString(rtf: data, documentAttributes: nil)
    return attributed?.string ?? ""
}
```

---

### Phase 3: Video Support (🔧 Moderate - ~400 LOC)

**New Files:**
- `VideoProcessor.swift` - Frame sampling + analysis
- `AudioTranscriber.swift` - Speech-to-text

**Changes:**
```swift
private func processVideo(_ data: Data) -> VideoAnalysis {
    let asset = AVAsset(data: data)
    
    // Sample 10 keyframes
    let frames = sampleKeyframes(asset, count: 10)
    
    // Analyze each frame
    var sceneLabels: [String: Int] = [:]
    var ocrTexts: [String] = []
    
    for frame in frames {
        let (ocr, labels) = analyzeFrame(frame)
        ocrTexts.append(ocr)
        for label in labels {
            sceneLabels[label, default: 0] += 1
        }
    }
    
    // Extract audio and transcribe
    let transcription = transcribeAudio(asset)
    
    // Combine all text
    let topLabels = sceneLabels.sorted { $0.value > $1.value }
                                .prefix(5)
                                .map { $0.key }
    
    let combined = "\(topLabels.joined(separator: " ")) " +
                   "\(ocrTexts.joined(separator: " ")) " +
                   "\(transcription)"
    
    return VideoAnalysis(
        text: combined,
        labels: topLabels,
        ocrTexts: ocrTexts,
        transcription: transcription
    )
}
```

---

### Phase 4: Audio Support (⚡ Easy - ~150 LOC)

**New Files:**
- `AudioTranscriber.swift` (reuse from video)

**Changes:**
```swift
private func processAudio(_ data: Data) -> (text: String, language: String) {
    let recognizer = SFSpeechRecognizer()
    let request = SFSpeechAudioBufferRecognitionRequest()
    
    // Transcribe audio
    let transcription = recognizer.recognitionTask(with: request) { result, error in
        // Handle result
    }
    
    return (transcription, recognizer.locale.identifier)
}
```

---

## Benefits of This Architecture

```
┌──────────────────────┬─────────────────────────┬────────────────────────────┐
│  Component           │  Responsibility         │  Benefit                   │
├──────────────────────┼─────────────────────────┼────────────────────────────┤
│  NornicDB Server     │  • Node storage         │  • Simpler code            │
│  (Go)                │  • Graph queries        │  • No OCR logic            │
│                      │  • Vector search        │  • No PDF parsing          │
│                      │  • Just stores nodes    │  • Framework-agnostic      │
├──────────────────────┼─────────────────────────┼────────────────────────────┤
│  Apple ML Server     │  • OCR extraction       │  • Centralized processing  │
│  (Swift)             │  • Scene analysis       │  • Reusable across clients │
│                      │  • Document parsing     │  • Apple APIs in Swift     │
│                      │  • Video frame analysis │  • Natural fit             │
│                      │  • Embedding generation │  • Single responsibility   │
├──────────────────────┼─────────────────────────┼────────────────────────────┤
│  Client (Indexer)    │  • Send file data       │  • Simple API              │
│                      │  • Store embedding      │  • No dependencies         │
│                      │  • No preprocessing     │  • Works from any language │
└──────────────────────┴─────────────────────────┴────────────────────────────┘

Key Advantages:
  ├─ Single embedding space (512 dims) for ALL content types
  ├─ Text-based search works across modalities
  ├─ NornicDB stays modality-agnostic (just stores vectors)
  ├─ Apple ML server handles all complexity
  └─ Backward compatible (plain text still works)
```

---

## Example Use Cases

### 1. Receipt Search

```
Store receipt.jpg → OCR: "Acme Store $42.99" → Embedding
Query: "where did I spend money on December 9th?"
Result: Finds receipt.jpg via semantic similarity
```

### 2. PDF Document Search

```
Store report.pdf → Extract: "Q4 Financial Report..." → Embedding
Query: "quarterly revenue analysis"
Result: Finds report.pdf
```

### 3. Video Tutorial Search

```
Store cooking.mp4 → Frames: "kitchen, cooking, pasta"
                  → Audio: "how to make carbonara sauce"
                  → Embedding
Query: "pasta recipe tutorial"
Result: Finds cooking.mp4
```

### 4. Mixed Content Search

```
Database contains:
  • Text note: "Need to buy groceries"
  • Receipt image: "Grocery Store $87.43"
  • Video: cooking tutorial (transcribed)

Query: "grocery shopping"
Result: All 3 items (unified text embedding space!)
```

---

## Data Flow: Multimodal Node to Searchable Embedding

```
1. User Creates Node with Binary Content
   ↓
   MATCH (n:Image {file_path: "/photos/receipt.jpg"})
   ↓
2. NornicDB detects node needs embedding
   ↓
3. Read file data (if not in node)
   ↓
4. POST to Apple ML Server
   ──────────────────────────────────►
   {
     "input": ["data:image/jpeg;base64,..."],
     "model": "apple-ml-multimodal",
     "extract": ["ocr", "description"]
   }
                                    5. Apple ML Server:
                                       • Decode image
                                       • Run OCR
                                       • Classify scene
                                       • Combine text
                                       • Generate embedding
                                       ↓
6. Response                         [512 floats] + extracted text
   ◄──────────────────────────────────
   ↓
7. Update Node
   node.Embedding = [512 floats]
   node.Properties["ocr_text"] = "Receipt from Acme..."
   node.Properties["scene_labels"] = ["document", "receipt"]
   node.Properties["embedding_model"] = "apple-ml-embeddings"
   node.Properties["source_modality"] = "image"
   ↓
8. Index in Vector Search
   vectorIndex.Add("node-123", embedding)
   ↓
9. Ready for Semantic Search
   Query: "where did I shop yesterday?"
   → Finds receipt via text similarity!
```

---

## Why This Approach Works

### Single Embedding Space

All content types → text → 512-dim embedding → same vector space

**This means:**
- Text query can find images (via OCR)
- Text query can find videos (via transcription)
- Text query can find PDFs (via extracted text)
- **No separate indexes needed** - one vector index for everything!

### Separation of Concerns

| Layer | Responsibility | Technology |
|-------|----------------|------------|
| **Storage** | Store nodes + vectors | Go + BadgerDB |
| **Search** | Vector similarity | Go + HNSW |
| **Extraction** | Binary → Text | Swift + Apple Frameworks |
| **Embedding** | Text → Vector | Swift + NLEmbedding |

### Backward Compatibility

- Plain text: Works exactly as before
- No changes to NornicDB server code
- Optional response fields (clients can ignore)
- Same 512-dim vector space

---

## Performance Considerations

```
┌──────────────────┬─────────────────┬──────────────────┬────────────────────┐
│  Content Type    │  Processing     │  Latency         │  Notes             │
├──────────────────┼─────────────────┼──────────────────┼────────────────────┤
│  Text            │  None           │  ~50ms           │  Direct embedding  │
├──────────────────┼─────────────────┼──────────────────┼────────────────────┤
│  Image (small)   │  OCR + classify │  ~200ms          │  Single image      │
├──────────────────┼─────────────────┼──────────────────┼────────────────────┤
│  PDF (10 pages)  │  Text extract   │  ~100ms          │  Fast (PDFKit)     │
├──────────────────┼─────────────────┼──────────────────┼────────────────────┤
│  Video (1 min)   │  10 frames +    │  ~2-3 seconds    │  Most expensive    │
│                  │  audio transc.  │                  │  (batch process)   │
└──────────────────┴─────────────────┴──────────────────┴────────────────────┘

Optimization Strategies:
  ├─ Cache extracted text (don't re-process same file)
  ├─ Async processing (return 202 Accepted for videos)
  ├─ Configurable frame sampling (fewer frames = faster)
  └─ Skip audio transcription if not needed
```

---

## Security Model (Unchanged)

```
🔒 Authentication Flow:
  1. Generate UUID API key → Store in Keychain
  2. Menu bar app: server.setAPIKey(key)
  3. NornicDB: Include in Authorization header
  4. Apple ML Server: Validate Bearer token
  5. ✅ Match → Process | ❌ Mismatch → 401

🏠 Network Binding:
  • 127.0.0.1:11435 only (not 0.0.0.0)
  • No external access
  • Same-machine communication only

🔐 Privacy:
  • All processing on-device
  • No data sent to cloud
  • Apple frameworks are local-only
```

---

## Implementation Roadmap

```
Phase 1: Image Support (⚡ Easy - 1-2 days)
  ├─ Create AppleVisionExtractor.swift
  ├─ Add OCR + scene classification
  ├─ Update handleEmbeddingsRequest()
  └─ Test with receipt images

Phase 2: PDF Support (⚡ Easy - 1 day)
  ├─ Create DocumentExtractor.swift
  ├─ PDFKit text extraction
  ├─ Metadata enrichment
  └─ Test with research papers

Phase 3: Video Support (🔧 Moderate - 3-4 days)
  ├─ Create VideoProcessor.swift
  ├─ Frame sampling logic
  ├─ Per-frame Vision analysis
  ├─ Audio transcription
  └─ Test with tutorial videos

Phase 4: Audio Support (⚡ Easy - 1 day)
  ├─ Create AudioTranscriber.swift
  ├─ SFSpeechRecognizer integration
  └─ Test with voice notes
```

---

## Future: True Multimodal Models

While Apple's Vision framework doesn't provide video descriptions, **Apple's research models** (4M-21, MM1) do support true multimodal understanding. If/when Apple releases these as developer APIs:

```
Current: Binary → Text → Embedding (512d text space)
Future:  Binary → Direct Multimodal Embedding (shared vision-language space)

Example:
  • Image embedding: [512 floats in vision-language space]
  • Text embedding: [512 floats in SAME space]
  • Query "red car" finds images of red cars (no OCR needed!)
```

But for now, the **text extraction approach** gives us 80% of the benefit with 100% available APIs.

---

**Ready to implement?** Start with Phase 1 (images) since it's the most impactful and easiest to add.
