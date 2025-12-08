# NornicDB for iOS

<p align="center">
  <img src="../docs/assets/logos/nornicdb-logo.svg" alt="NornicDB Logo" width="120"/>
</p>

<h2 align="center">Personal Graph Memory for iPhone</h2>

<p align="center">
  <strong>Graph-RAG • Siri Integration • On-Device AI • Zero Cloud Dependency</strong>
</p>

---

## 🎯 Vision

Transform your iPhone into a personal knowledge engine. NornicDB for iOS stores memories, discovers relationships, and provides intelligent retrieval through Siri—all completely offline and private.

**"Hey Siri, what did I learn about Swift concurrency last month?"**

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | System design & component overview |
| [Apple Intelligence](docs/APPLE_INTELLIGENCE.md) | Siri, App Intents, Shortcuts integration |
| [Metal ML](docs/METAL_ML.md) | GPU acceleration & on-device inference |
| [Background Processing](docs/BACKGROUND_PROCESSING.md) | Deferred tasks & charging optimization |
| [Models](docs/MODELS.md) | Embedding & LLM model requirements |
| [Data Flow](docs/DATA_FLOW.md) | How data moves through the system |
| [API Reference](docs/API.md) | Swift/Go binding interface |
| [Privacy](docs/PRIVACY.md) | Data handling & security |
| [Roadmap](docs/ROADMAP.md) | Implementation phases & timeline |

## 🚀 Quick Overview

### What It Does

```
┌─────────────────────────────────────────────────────────────────┐
│                         iPhone                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   "Hey Siri, remind me about the project deadline"              │
│                          ↓                                       │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│   │   Siri &    │───▶│  NornicDB   │───▶│   Graph     │        │
│   │ App Intents │    │   Engine    │    │    RAG      │        │
│   └─────────────┘    └─────────────┘    └─────────────┘        │
│                          ↓                     ↓                 │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│   │   Metal     │◀───│  BadgerDB   │    │  Response   │        │
│   │    GPU      │    │  Storage    │    │ Generation  │        │
│   └─────────────┘    └─────────────┘    └─────────────┘        │
│                                                                  │
│   "You have a project deadline on Friday. Related notes..."     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Features

- **🧠 Personal Knowledge Graph** — Store thoughts, notes, conversations as connected nodes
- **🔍 Semantic Search** — Find memories by meaning, not keywords
- **🗣️ Siri Integration** — Natural language queries via voice
- **⚡ Metal GPU** — Fast on-device embeddings and inference
- **🔒 Privacy First** — Everything stays on your device
- **🔋 Battery Smart** — Heavy tasks deferred to charging

## 🏗️ Architecture Summary

```
┌────────────────────────────────────────────────────────────────────┐
│                        NornicDB iOS Stack                          │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    Swift UI Layer                            │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │  │
│  │  │  Views   │ │ Widgets  │ │Shortcuts │ │  Intents │       │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                              ↓                                      │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                   NornicDB.framework                         │  │
│  │  ┌──────────────────────────────────────────────────────┐   │  │
│  │  │              Go Core (via gomobile)                   │   │  │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐    │   │  │
│  │  │  │ Cypher  │ │ Storage │ │ Search  │ │  Decay  │    │   │  │
│  │  │  │ Engine  │ │BadgerDB │ │ Vector  │ │ Memory  │    │   │  │
│  │  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘    │   │  │
│  │  └──────────────────────────────────────────────────────┘   │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                              ↓                                      │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                   Apple Frameworks                           │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │  │
│  │  │  Metal   │ │ Core ML  │ │ BGTasks  │ │ CloudKit │       │  │
│  │  │   GPU    │ │  Models  │ │Background│ │  (opt.)  │       │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

## 📱 Requirements

- **iOS 17.0+** (for App Intents v2 and improved Siri)
- **iPhone 12+** (A14 Bionic or later for Neural Engine)
- **~500MB storage** for models + database
- **Xcode 15+** for development

## 🎯 Target Use Cases

### 1. Personal Memory Assistant
```
User: "Hey Siri, what was that restaurant Sarah recommended?"
Siri: "Sarah recommended Pizzeria Delfina on March 15th. 
       She mentioned their margherita pizza is excellent."
```

### 2. Learning & Research
```
User: "Show me what I've learned about machine learning"
App:  [Graph visualization of ML concepts with connections]
      - Stored 47 notes on ML
      - Connected to: Python, Neural Networks, Your Job
      - Last updated: 2 days ago
```

### 3. Work Context
```
User: "Summarize my notes from the design meeting"
Siri: "From the design meeting on Tuesday:
       - New color palette approved
       - Launch date moved to Q2
       - Related: Marketing brief, Brand guidelines"
```

### 4. Life Journaling
```
User: "What made me happy this week?"
App:  [Shows memories tagged with positive sentiment]
      - Coffee with Alex (Thursday)
      - Finished the book (Wednesday)
      - Dog park visit (Monday)
```

## 🛠️ Development Setup

```bash
# Clone repository
git clone https://github.com/orneryd/NornicDB.git
cd NornicDB/ios

# Install gomobile
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init

# Build iOS framework
make framework

# Open Xcode project
open NornicDB.xcodeproj
```

## 📊 Resource Budget

| Resource | Budget | Notes |
|----------|--------|-------|
| Storage | 500MB base | Grows with usage |
| RAM | 150MB active | Freed when backgrounded |
| Battery | <5% daily | Heavy ops on charger |
| Network | 0 | Fully offline |

## 🔐 Privacy Guarantees

- ✅ **All data on-device** — Never leaves your iPhone
- ✅ **No telemetry** — Zero analytics or tracking
- ✅ **No cloud dependency** — Works in airplane mode
- ✅ **User-controlled export** — Your data, your choice
- ✅ **Encrypted storage** — BadgerDB encryption at rest

## 📅 Roadmap Overview

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| 1. Core Framework | 4 weeks | Go bindings + basic queries |
| 2. Siri Integration | 3 weeks | App Intents + voice queries |
| 3. Metal ML | 3 weeks | On-device embeddings |
| 4. Background Processing | 2 weeks | Deferred tasks |
| 5. UI & Polish | 4 weeks | SwiftUI app + widgets |
| 6. TestFlight | 2 weeks | Beta testing |

**Total: ~18 weeks to MVP**

## 📄 License

MIT License — Same as NornicDB core.

---

<p align="center">
  <em>Your memories, your device, your control.</em>
</p>
