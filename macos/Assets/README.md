# NornicDB macOS Icon Assets

## Overview

This directory contains icon assets for the NornicDB menu bar application.

## Icon Generation

### Option 1: Use Pre-generated Icon (Recommended)

If you have `NornicDB.icns` in this directory, it will be automatically used during build.

### Option 2: Generate from SVG

To generate icons from the SVG logo:

```bash
# Install librsvg (one-time setup)
brew install librsvg

# Generate icons
./create-icons.sh
```

This creates `NornicDB.icns` which will be used in future builds.

### Option 3: Use SF Symbols (Default)

If no custom icon is available, the app uses macOS SF Symbols:
- `database.fill` - Green when server running
- `database` - Red/gray when stopped

This provides a clean, native look without requiring custom assets.

## Icon Specifications

macOS app icons require multiple sizes:
- 16x16, 32x32 (menu bar, small icons)
- 64x64, 128x128 (Finder, dock)
- 256x256, 512x512 (Retina displays)
- 1024x1024 (App Store, high-res displays)

Each size should have a @2x version for Retina displays.

## Current Status

The menu bar app currently uses SF Symbols for the status icon. This is intentional as it:
- Provides clear visual status (green/red/gray)
- Matches macOS design language
- Works without custom assets
- Adapts to light/dark mode automatically

The app bundle icon (shown in Applications folder) can use the custom NornicDB logo if generated.

## Build Integration

The Makefile automatically:
1. Checks for `NornicDB.icns` in this directory
2. Falls back to SF Symbol icons if not found
3. Includes icon in app bundle if available

No manual steps required during normal builds.
