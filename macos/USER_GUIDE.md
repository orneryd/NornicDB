# NornicDB - User Guide for Mac

**Welcome!** This guide will help you install and use NornicDB on your Mac - no technical knowledge required.

## What is NornicDB?

NornicDB is a **graph database** - think of it like a smart spreadsheet that understands relationships between your data. It's perfect for:

- 🔗 Connecting and exploring related information
- 🤖 AI-powered data analysis
- 📊 Building knowledge graphs
- 🚀 Fast data queries (3-52x faster than alternatives)

## Installation (3 Easy Steps)

### Step 1: Download the Installer

1. Go to the [NornicDB Releases page](https://github.com/orneryd/nornicdb/releases)
2. Download **NornicDB-[version]-arm64.dmg** (for Apple Silicon Mac)
   - If you have an Intel Mac, download the **x86_64** version instead
3. Open the downloaded .dmg file

### Step 2: Run the Installer

1. Double-click **NornicDB-[version]-arm64.pkg**
2. Click **Continue** through the installer
3. Enter your Mac password when prompted
4. Wait for installation to complete (takes about 30 seconds)

### Step 3: Complete Setup

A welcome wizard will appear automatically:

1. **Welcome Screen** - Click **Next**
2. **Choose Your Setup**:
   - **Basic** - Just the essentials (good for beginners)
   - **Standard** ✅ - Recommended for most people
   - **Advanced** - All AI features enabled
3. Click **Next**
4. Review your choices
5. Click **Start NornicDB**

**That's it!** NornicDB is now running on your Mac. 🎉

## Using NornicDB

### Finding the App

Look in your menu bar (top-right of your screen) for the database icon:
- 🟢 Green = Running and healthy
- 🔴 Red = Stopped
- ⚪️ Gray = Unknown status

### Opening the Menu

Click the database icon to see available actions:

```
NornicDB - 🟢 Running
────────────────────────────
Open Web UI
Stop Server
Restart Server
────────────────────────────
Settings...
Open Config File
Show Logs
────────────────────────────
About NornicDB
Check for Updates
────────────────────────────
Quit
```

### Common Actions

#### Opening the Database

1. Click the menu bar icon
2. Select **Open Web UI**
3. Your browser opens to http://localhost:7687

Here you can:
- Create and explore graphs
- Run queries
- Visualize data
- Import/export data

#### Starting/Stopping the Database

**To Stop:**
1. Click menu bar icon
2. Select **Stop Server**
3. Icon turns red

**To Start:**
1. Click menu bar icon  
2. Select **Start Server**
3. Icon turns green after a few seconds

**To Restart:**
1. Click menu bar icon
2. Select **Restart Server**

#### Changing Settings

1. Click menu bar icon
2. Select **Settings...** (or press ⌘,)
3. A window opens with three tabs:

**Features Tab** - Turn AI features on/off:
- **Embeddings** - Smart search based on meaning
- **K-Means** - Automatic grouping of similar data
- **Auto-TLP** - Predict future connections
- **Heimdall** - AI assistant for monitoring

**Server Tab** - Change network settings:
- **Port Number** - Which port to use (default: 7687)
- **Host Address** - Where to listen (default: localhost)

**Startup Tab** - Control when NornicDB starts:
- **Start at Login** - Turn on/off auto-start

4. Click **Save & Restart** when done

### Viewing Logs

If something isn't working:

1. Click menu bar icon
2. Select **Show Logs**
3. A folder opens with log files
4. Open `stdout.log` or `stderr.log` to see what's happening

## Common Questions

### Q: Does NornicDB start automatically?

**A:** Yes! After installation, NornicDB starts automatically when you log in to your Mac.

To change this:
1. Menu bar icon → **Settings...**
2. Go to **Startup** tab
3. Toggle **Start at Login**
4. Click **Save & Restart**

### Q: How do I know if it's running?

**A:** Look at the menu bar icon color:
- 🟢 Green = Running
- 🔴 Red = Stopped

### Q: Can I use it without the AI features?

**A:** Yes! 

1. Menu bar icon → **Settings...**
2. Go to **Features** tab
3. Turn off the features you don't need
4. Click **Save & Restart**

This saves memory and makes NornicDB faster.

### Q: Where is my data stored?

**A:** Your graph database is stored at:
```
/usr/local/var/nornicdb/data
```

This data persists even if you restart your Mac or update NornicDB.

### Q: How do I back up my data?

**Option 1: Quick Backup**
1. Click menu bar icon → **Stop Server**
2. Copy folder: `/usr/local/var/nornicdb/data`
3. Paste somewhere safe (external drive, cloud storage)
4. Click menu bar icon → **Start Server**

**Option 2: Export as JSON**
1. Open Web UI
2. Use Export feature
3. Save JSON file

### Q: Can other computers access my database?

**A:** By default, no. NornicDB only accepts connections from your Mac.

To allow network access:
1. Menu bar icon → **Settings...**
2. Go to **Server** tab
3. Change **Host Address** to `0.0.0.0`
4. Click **Save & Restart**

⚠️ **Security Note**: Only do this if you trust your network!

### Q: How much memory does it use?

**A:** Typically 100-500 MB depending on your data size and enabled features.

To reduce memory:
- Turn off AI features you don't need
- Use Basic or Standard preset instead of Advanced

### Q: Can I have multiple databases?

**A:** Currently, NornicDB runs one database per installation. For multiple databases, you'll need multiple installations with different ports.

## Troubleshooting

### Problem: Menu bar icon doesn't appear

**Solution:**
1. Open **Applications** folder
2. Find **NornicDB** app
3. Double-click it to launch

### Problem: Icon is red (server stopped)

**Solution:**
1. Click the red icon
2. Select **Start Server**
3. Wait 10 seconds for it to turn green

If it stays red:
1. Click icon → **Show Logs**
2. Open `stderr.log` to see error messages

### Problem: "Connection refused" error

**Solution:**
Server isn't running yet. Wait 30 seconds after starting, then try again.

### Problem: Forgot my port number

**Solution:**
1. Click menu bar icon → **Settings...**
2. Go to **Server** tab
3. Check **Port Number** field

Default is **7687**.

### Problem: Want to reset everything

**Solution:**
1. Click menu bar icon → **Stop Server**
2. Go to Applications
3. Right-click **NornicDB** → Move to Trash
4. Delete folder: `~/Library/Application Support/NornicDB`
5. Download and reinstall

## Uninstalling

Don't want NornicDB anymore? No problem!

### Easy Uninstall

1. Download the [uninstall script](https://github.com/orneryd/nornicdb/blob/main/macos/scripts/uninstall.sh)
2. Double-click to run it
3. Choose whether to keep your data
4. Done!

### Manual Uninstall

1. Click menu bar icon → **Quit**
2. Open **Terminal** app
3. Copy and paste this command:
```bash
launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist
rm ~/Library/LaunchAgents/com.nornicdb.server.plist
sudo rm /usr/local/bin/nornicdb
rm -rf ~/Applications/NornicDB.app
```
4. Enter your password when asked

Your data stays safe at `/usr/local/var/nornicdb/data` unless you manually delete it.

## Getting Help

### In-App Help

1. Click menu bar icon
2. Select **About NornicDB**
3. Click **Visit Website** for documentation

### Online Resources

- **Documentation**: https://github.com/orneryd/nornicdb/docs
- **Community**: https://github.com/orneryd/nornicdb/discussions
- **Report Issues**: https://github.com/orneryd/nornicdb/issues

### Quick Tips

- Press **⌘,** anytime to open Settings
- Press **⌘L** to view logs
- Press **⌘O** to open Web UI

## Next Steps

Now that NornicDB is installed:

1. **Learn the Basics**: Visit the Web UI and try the tutorial
2. **Import Data**: Use the import feature to load your data
3. **Run Queries**: Try Cypher queries (similar to SQL)
4. **Explore AI Features**: Enable embeddings for semantic search

## Cheat Sheet

| Action | How To |
|--------|--------|
| Open database | Menu bar → Open Web UI |
| Start server | Menu bar → Start Server |
| Stop server | Menu bar → Stop Server |
| Change settings | Menu bar → Settings (⌘,) |
| View logs | Menu bar → Show Logs |
| Check status | Look at menu bar icon color |
| Get help | Menu bar → About → Visit Website |

---

**Enjoy using NornicDB!** 🚀

If you found this helpful, please star us on [GitHub](https://github.com/orneryd/nornicdb)!

