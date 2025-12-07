╔══════════════════════════════════════════════════════════════╗
║                    NornicDB for macOS                        ║
║              High-Performance Graph Database                 ║
╚══════════════════════════════════════════════════════════════╝

Thank you for downloading NornicDB!

═══════════════════════════════════════════════════════════════

QUICK START
───────────────────────────────────────────────────────────────

1. Double-click the .pkg file to install
2. Follow the setup wizard (takes 1 minute)
3. Look for the database icon in your menu bar
4. Click it and select "Open Web UI"

That's it! 🎉

═══════════════════════════════════════════════════════════════

WHAT GETS INSTALLED
───────────────────────────────────────────────────────────────

✓ NornicDB server (starts automatically)
✓ Menu bar app for easy control
✓ Configuration in ~/Library/Application Support/NornicDB
✓ Data storage in /usr/local/var/nornicdb

═══════════════════════════════════════════════════════════════

USING THE MENU BAR APP
───────────────────────────────────────────────────────────────

The database icon in your menu bar shows status:
  🟢 Green = Running
  🔴 Red = Stopped

Click the icon for quick actions:
  • Open Web UI - Access the database
  • Settings - Configure features
  • Start/Stop/Restart - Control the server
  • Show Logs - View activity logs

═══════════════════════════════════════════════════════════════

CONFIGURING FEATURES
───────────────────────────────────────────────────────────────

Click menu bar icon → Settings (⌘,) to configure:

Features Tab:
  • Embeddings - Smart semantic search
  • K-Means - Automatic clustering
  • Auto-TLP - Predict connections
  • Heimdall - AI monitoring

Server Tab:
  • Port Number - Default: 7687
  • Host Address - Default: localhost

Startup Tab:
  • Start at Login - Auto-start on boot

Click "Save & Restart" to apply changes.

═══════════════════════════════════════════════════════════════

SYSTEM REQUIREMENTS
───────────────────────────────────────────────────────────────

• macOS 12.0 (Monterey) or later
• Apple Silicon (M1/M2/M3/M4) or Intel
• 200 MB disk space (more for data)
• Internet connection (optional, for updates)

═══════════════════════════════════════════════════════════════

ACCESSING YOUR DATABASE
───────────────────────────────────────────────────────────────

Web UI:  http://localhost:7687
Bolt:    bolt://localhost:7687

Compatible with all Neo4j drivers and tools!

═══════════════════════════════════════════════════════════════

GETTING HELP
───────────────────────────────────────────────────────────────

User Guide:      macos/USER_GUIDE.md (included)
Documentation:   https://github.com/orneryd/nornicdb/docs
Community:       https://github.com/orneryd/nornicdb/discussions
Report Issues:   https://github.com/orneryd/nornicdb/issues

═══════════════════════════════════════════════════════════════

UNINSTALLING
───────────────────────────────────────────────────────────────

Menu bar icon → Quit
Then drag NornicDB.app to Trash

Or use the uninstall script:
  https://github.com/orneryd/nornicdb/blob/main/macos/scripts/uninstall.sh

Your data is preserved unless you explicitly delete it.

═══════════════════════════════════════════════════════════════

TROUBLESHOOTING
───────────────────────────────────────────────────────────────

❌ Menu bar icon doesn't appear
   → Open Applications/NornicDB.app manually

❌ Icon is red (server stopped)
   → Click icon → Start Server

❌ "Connection refused" error
   → Wait 30 seconds for server to fully start

❌ Port already in use
   → Change port in Settings → Server tab

═══════════════════════════════════════════════════════════════

LICENSE & CREDITS
───────────────────────────────────────────────────────────────

See LICENSE file for license information.

NornicDB is built with:
  • Neo4j Bolt protocol
  • llama.cpp for AI features
  • Swift/SwiftUI for menu bar app

═══════════════════════════════════════════════════════════════

Thank you for using NornicDB! ❤️

Please star us on GitHub if you find this useful:
https://github.com/orneryd/nornicdb

═══════════════════════════════════════════════════════════════

