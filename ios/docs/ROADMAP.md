# Implementation Roadmap

Detailed timeline for NornicDB iOS development.

## Overview

**Total Duration:** ~18 weeks to MVP  
**Team Size:** 1-2 developers  
**Target:** iOS 17.0+, iPhone 12+

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DEVELOPMENT TIMELINE                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Phase 1: Core Framework                    ████████░░░░░░░░░░  4 weeks │
│  Phase 2: Siri Integration                  ░░░░░░░░██████░░░░  3 weeks │
│  Phase 3: Metal ML                          ░░░░░░░░░░░░██████  3 weeks │
│  Phase 4: Background Processing             ░░░░░░░░░░░░░░░░██  2 weeks │
│  Phase 5: UI & Polish                       ░░░░░░░░░░░░░░░░██████████  │
│                                                               4 weeks    │
│  Phase 6: Testing & Release                 ░░░░░░░░░░░░░░░░░░░░░░░░██  │
│                                                                 2 weeks  │
│                                                                          │
│  Week: 1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Core Framework (Weeks 1-4)

### Goals
- Working NornicDB embedded in iOS app
- Basic Cypher queries functional
- Data persists between launches

### Week 1: Project Setup

- [ ] Create Xcode project with SwiftUI
- [ ] Set up gomobile toolchain
- [ ] Create `pkg/mobile/mobile.go` interface
- [ ] Initial iOS framework build
- [ ] Basic integration test

**Deliverable:** App launches with embedded NornicDB

### Week 2: Storage Layer

- [ ] BadgerDB integration for iOS
- [ ] File paths in iOS sandbox
- [ ] Database encryption with iOS keychain
- [ ] Basic CRUD operations working
- [ ] Memory management testing

**Deliverable:** Data persists between app launches

### Week 3: Query Engine

- [ ] Cypher parser integration
- [ ] Query execution from Swift
- [ ] JSON result marshaling
- [ ] Error handling Swift ↔ Go
- [ ] Query timeout handling

**Deliverable:** Cypher queries work from iOS

### Week 4: Core Polishing

- [ ] Framework size optimization
- [ ] Memory leak testing
- [ ] Crash handling
- [ ] Logging integration
- [ ] Unit tests for Go bridge

**Deliverable:** Stable 1.0 framework

---

## Phase 2: Siri Integration (Weeks 5-7)

### Goals
- Voice queries working
- Shortcuts integration
- Spotlight indexing

### Week 5: App Intents

- [ ] Define `QueryMemoryIntent`
- [ ] Define `StoreMemoryIntent`
- [ ] Define `SummarizeIntent`
- [ ] Entity types (`MemoryEntity`)
- [ ] Basic phrase matching

**Deliverable:** "Hey Siri, search my memories for..."

### Week 6: Shortcuts & Widgets

- [ ] `AppShortcutsProvider` setup
- [ ] Custom Siri phrases
- [ ] WidgetKit integration
- [ ] Memory of the Day widget
- [ ] Quick Stats widget

**Deliverable:** Shortcuts app integration

### Week 7: Spotlight & Polish

- [ ] CoreSpotlight indexing
- [ ] NSUserActivity for Handoff
- [ ] Focus mode filtering
- [ ] Siri dialog improvements
- [ ] Error handling for voice

**Deliverable:** System-wide memory search

---

## Phase 3: Metal ML (Weeks 8-10)

### Goals
- On-device embeddings
- GPU-accelerated search
- K-means clustering

### Week 8: Core ML Integration

- [ ] Convert embedding model to Core ML
- [ ] Tokenizer implementation
- [ ] Model loading & caching
- [ ] Basic embedding generation
- [ ] Neural Engine optimization

**Deliverable:** Text → embedding on device

### Week 9: Metal Compute

- [ ] Port existing Metal shaders
- [ ] K-means kernel for iOS
- [ ] Cosine similarity batch
- [ ] Vector index structure
- [ ] GPU memory management

**Deliverable:** GPU-accelerated vector search

### Week 10: Optimization

- [ ] Batch processing pipeline
- [ ] Thermal management
- [ ] Memory-efficient inference
- [ ] Model quantization testing
- [ ] Benchmark suite

**Deliverable:** <100ms vector search

---

## Phase 4: Background Processing (Weeks 11-12)

### Goals
- Heavy tasks run while charging
- Battery-efficient operation
- Automatic maintenance

### Week 11: BGTasks Setup

- [ ] Task registration
- [ ] Embedding refresh task
- [ ] Decay update task
- [ ] Compaction task
- [ ] Charging detection

**Deliverable:** Background task scheduler

### Week 12: Deferred Work

- [ ] Work queue system
- [ ] Priority scheduling
- [ ] Thermal-aware throttling
- [ ] Task persistence
- [ ] Error recovery

**Deliverable:** Battery-efficient background ops

---

## Phase 5: UI & Polish (Weeks 13-16)

### Goals
- Beautiful SwiftUI app
- Graph visualization
- Smooth user experience

### Week 13: Main Interface

- [ ] Memory list view
- [ ] Memory detail view
- [ ] Search interface
- [ ] Quick capture sheet
- [ ] Settings screen

**Deliverable:** Functional main app

### Week 14: Graph Visualization

- [ ] Force-directed graph view
- [ ] Touch interaction
- [ ] Zoom and pan
- [ ] Connection highlighting
- [ ] Animation polish

**Deliverable:** Interactive graph view

### Week 15: Advanced Features

- [ ] Import/export
- [ ] iCloud sync (optional)
- [ ] Share sheet integration
- [ ] Tags management
- [ ] Batch operations

**Deliverable:** Full feature set

### Week 16: Polish

- [ ] Haptic feedback
- [ ] Accessibility
- [ ] Localization prep
- [ ] Onboarding flow
- [ ] App icons & assets

**Deliverable:** App Store ready UI

---

## Phase 6: Testing & Release (Weeks 17-18)

### Goals
- Stable release candidate
- TestFlight beta
- App Store submission

### Week 17: Testing

- [ ] Full integration tests
- [ ] Performance profiling
- [ ] Memory testing
- [ ] Device compatibility
- [ ] Beta user feedback

**Deliverable:** Release candidate

### Week 18: Release

- [ ] TestFlight distribution
- [ ] Bug fixes from beta
- [ ] App Store screenshots
- [ ] App Store description
- [ ] Submit for review

**Deliverable:** App Store submission

---

## Milestones

| Milestone | Date | Criteria |
|-----------|------|----------|
| M1: Framework Alpha | Week 4 | Cypher queries work |
| M2: Siri Beta | Week 7 | Voice search works |
| M3: ML Complete | Week 10 | On-device embeddings |
| M4: Feature Complete | Week 16 | All features working |
| M5: App Store | Week 18 | Published |

---

## Risk Mitigation

### Risk: Framework Size Too Large
- **Mitigation:** Strip debug symbols, optimize Go build
- **Fallback:** Ship models separately, download on demand

### Risk: Battery Drain
- **Mitigation:** Aggressive background throttling
- **Fallback:** Disable background tasks, manual sync only

### Risk: Memory Pressure
- **Mitigation:** Streaming inference, batch limits
- **Fallback:** Smaller models, reduce feature set

### Risk: Siri Reliability
- **Mitigation:** Extensive phrase testing
- **Fallback:** Manual search fallback, clear error messages

---

## Post-Launch Roadmap

### Version 1.1 (Month 2)
- [ ] Apple Watch companion
- [ ] CarPlay integration
- [ ] Improved graph visualization

### Version 1.2 (Month 3)
- [ ] iPad optimization
- [ ] macOS Catalyst
- [ ] Cross-device sync

### Version 2.0 (Month 6)
- [ ] Vision Pro support
- [ ] Advanced ML features
- [ ] Plugin system

---

## Success Metrics

| Metric | Target |
|--------|--------|
| App launch time | <1s |
| Query latency | <100ms |
| Battery impact | <1%/day |
| Crash-free rate | >99.5% |
| App Store rating | ≥4.5★ |
| TestFlight users | 100+ |

---

## Dependencies

### Required
- Xcode 15+
- iOS 17 SDK
- gomobile latest
- Core ML tools

### Optional
- TestFlight account
- App Store Developer account ($99/year)
- Physical devices for testing

---

## Getting Started

```bash
# Week 1, Day 1

# 1. Clone repo
git clone https://github.com/orneryd/NornicDB.git
cd NornicDB/ios

# 2. Install gomobile
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init

# 3. Build framework
make framework

# 4. Open Xcode
open NornicDB.xcodeproj

# 5. Run on simulator
# Cmd+R
```

**Let's build something amazing! 🚀**
