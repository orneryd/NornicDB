# NornicDB macOS Menu Bar App - Changelog

## [Unreleased]

### Added
- **Progress Indicator** during save/restart operations
  - Shows "Saving configuration..." step
  - Shows "Configuration saved. Restarting server..." step
  - Shows "Server restarted successfully!" confirmation
  - Progress bar and status text at bottom of Settings window
  - Window automatically closes after successful restart

- **Change Detection** in Settings
  - "Save & Restart" button now disabled when no changes
  - "Unsaved changes" indicator appears when modifications are made
  - Tracks changes across all tabs (Features, Server, Models, Startup)
  - Visual feedback for pending changes

- **Model Configuration Persistence**
  - Embedding model selection now saved to config.yaml
  - Heimdall model selection now saved to config.yaml
  - Model selections loaded on app startup
  - Models displayed in Settings → Models tab

- **Enhanced Logging**
  - Config loading now logs to console
  - Feature states logged on load
  - Model selections logged on load
  - Helpful for debugging configuration issues

### Changed
- Settings window height increases to 580px when showing progress
- Cancel button disabled during save/restart operations
- Save & Restart button shows disabled state when no changes

### Fixed
- Model selections were not being persisted to config.yaml
- Config loading didn't parse model selections
- No visual feedback during server restart
- Users could make multiple save requests simultaneously

## [1.0.0] - 2024-12-07

### Added
- Initial macOS menu bar app release
- Status icon with color-coded health (green/red/gray)
- Quick menu for server control
- 4-tab Settings window (Features, Server, Models, Startup)
- Model download functionality from HuggingFace
- Model folder browser
- Model selection dropdowns
- First-run setup wizard
- Auto-start configuration for both server and app
- LaunchAgent integration
- Health monitoring every 10 seconds

### Features
- Toggle AI features (Embeddings, K-Means, Auto-TLP, Heimdall)
- Configure server port and host
- Select AI models from available .gguf files
- Control auto-start behavior
- Download default models (BGE-M3, Qwen2.5-0.5B)
- Open models folder for manual model management

### Installation
- Distributable .pkg installer
- Graceful reinstallation support
- Preserves user data and settings
- Auto-configures LaunchAgents

---

## UI/UX Improvements Summary

### Before:
- ❌ Alert dialog after save (modal, blocking)
- ❌ No feedback during restart process
- ❌ Could save even with no changes
- ❌ Model selections not persisted

### After:
- ✅ Inline progress indicator (non-blocking)
- ✅ Step-by-step progress updates
- ✅ Save button disabled when no changes
- ✅ "Unsaved changes" warning
- ✅ All settings including models saved
- ✅ Auto-close after successful restart
