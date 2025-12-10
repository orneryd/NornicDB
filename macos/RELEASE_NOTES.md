# NornicDB macOS Native Installation - Release Notes

## 🎉 What's New

NornicDB now includes a **native macOS installer** with a beautiful menu bar app for easy management!

### ✨ Key Features

#### 📦 One-Click Installation
- **Double-click installer** - No terminal commands required
- **Automatic setup** - Service, menu bar app, and directories created automatically
- **Auto-start on boot** - NornicDB starts automatically when your Mac boots
- **Clean uninstallation** - Simple removal process

#### 🎛️ Menu Bar Application
- **System tray integration** - Quick access from your menu bar
- **Real-time status** - See server health at a glance (🟢 Running / 🔴 Stopped / 🟡 Starting)
- **One-click controls** - Start, stop, restart server
- **Quick access** - Open Web UI directly from menu bar
- **Settings management** - Configure everything from a native macOS UI

#### 🧙‍♂️ First-Run Setup Wizard
- **Guided configuration** - 4-step wizard for initial setup
- **Configuration presets**:
  - **Basic** - Essential features only (Neo4j compatibility, fast queries, low resource usage)
  - **Standard** ⭐ (Recommended) - All basic features + vector embeddings + k-means clustering
  - **Advanced** - Full AI capabilities (Heimdall AI guardian, auto-predictions, maximum performance)
- **Security setup** - Configure admin credentials, JWT secret, and database encryption
- **Model management** - Download AI models directly from the wizard
- **Review & start** - See all settings before launching

#### ⚙️ Native Settings Panel
Five dedicated configuration tabs:
1. **Features** - Toggle major features (embeddings, k-means, auto-TLP, Heimdall)
2. **Server** - Configure ports, host, and network settings
3. **Models** - Select and manage AI models
4. **Security** - Admin credentials, JWT secret, database encryption
5. **Startup** - Auto-start configuration

#### 🔐 Enhanced Security
- **HTTP-only cookies** - Secure JWT storage in browser sessions
- **Stateless authentication** - No user state persisted on server
- **API token generation** - Create tokens for MCP servers via `/security` page
- **Database encryption** - Optional encryption at rest
- **Configurable credentials** - Set username, password, and JWT secret

#### 🤖 AI Model Management
- **Visual model browser** - See available models in `models/` directory
- **One-click downloads** - Download BGE-M3 and Qwen models from HuggingFace
- **Model status indicators** - Know which models are installed
- **Automatic detection** - Scans and lists all `.gguf` files

#### 📝 Configuration Management
- **YAML-based config** - All settings in `~/.nornicdb/config.yaml`
- **Clear precedence** - CLI flags > Env vars > Config file > Defaults
- **Live updates** - Changes applied on server restart
- **NORNICDB_ prefixes** - Consistent environment variable naming (migrated from NEO4J_)

---

## 🚀 Installation

### Prerequisites
- macOS 12.0 (Monterey) or later
- Apple Silicon (arm64) or Intel (amd64)
- ~500 MB disk space (1+ GB with AI models)

### Quick Install

1. **Download** `NornicDB-1.0.0-arm64.pkg` (or `.dmg`)
2. **Double-click** the installer
3. **Follow prompts** - The installer handles everything
4. **Look for the menu bar icon** - 🔵 NornicDB icon appears in your menu bar

That's it! 🎉

---

## 🧭 Getting Started

### First-Run Wizard

After installation, the **First-Run Wizard** appears automatically:

#### Step 1: Welcome
- Introduction to NornicDB
- Overview of what will be configured

#### Step 2: Features (Choose Your Setup)
Select a configuration preset:
- **Basic** - Lightweight, Neo4j-compatible graph database
- **Standard** ⭐ - Recommended for most users (includes embeddings & clustering)
- **Advanced** - Full AI capabilities with Heimdall guardian

#### Step 3: Security
Configure authentication and encryption:
- **Admin Username** - Default: `admin`
- **Admin Password** - Minimum 8 characters (default: `password`)
- **JWT Secret** - Auto-generated or custom (for token persistence)
- **Database Encryption** (Optional) - Enable encryption at rest with a strong key

#### Step 4: Review & Start
- Review all selected features
- See authentication summary
- Download AI models if needed (BGE-M3, Qwen2.5)
- **Save & Start Server** - Applies config and launches NornicDB

---

## 🎛️ Menu Bar Controls

Click the NornicDB icon in your menu bar to access:

### Status Indicator
- **🟢 Running** - Server is healthy and accessible
- **🔴 Stopped** - Server is not running
- **🟡 Starting...** - Server is launching
- **⚪️ Unknown** - Status cannot be determined

### Quick Actions
- **Open Web UI** - Launch NornicDB browser at `http://localhost:7474`
- **Start Server** - Start the NornicDB service
- **Stop Server** - Stop the NornicDB service  
- **Restart Server** - Restart with current config
- **Settings...** - Open settings panel
- **Download Models** - Get AI models from HuggingFace
- **Open Models Folder** - Browse installed models
- **Quit** - Exit menu bar app (server continues running)

---

## ⚙️ Settings Panel

Press **⌘,** or select **Settings...** from the menu to configure NornicDB.

### Features Tab
Toggle major features on/off:
- ✅ **Enable Embeddings** - Vector similarity search
- ✅ **Enable K-Means Clustering** - Automatic grouping
- ✅ **Enable Auto-TLP** - Topological Link prediction
- ✅ **Enable Heimdall** - AI guardian for security

### Server Tab
- **Bolt Port** - Neo4j protocol port (default: 7687)
- **HTTP Port** - Web UI and API port (default: 7474)
- **Host Address** - Bind address (default: 0.0.0.0)

### Models Tab
Select AI models for different features:
- **Embedding Model** - Choose from installed `.gguf` models
- **Heimdall Model** - Select Heimdall guardian model
- **Refresh List** - Rescan models directory

### Security Tab
- **Admin Credentials** - Change username and password
- **JWT Secret** - Update token signing key
- **Database Encryption** - Enable/disable and set encryption password

### Startup Tab
- ☑️ **Start at Login** - Launch menu bar app on boot

**All changes require a server restart** - Click **Save & Restart Server** to apply.

---

## 🔧 Configuration Files

### Primary Config
**Location**: `~/.nornicdb/config.yaml`

This is the main configuration file. The menu bar app reads and writes to this file.

### Config Search Order
1. `~/.nornicdb/config.yaml` (highest priority)
2. `./nornicdb.yaml` (binary directory)
3. `./config.yaml` (current directory)
4. OS-specific paths

### Example Config
```yaml
server:
  bolt_port: 7687
  port: 7474
  host: "0.0.0.0"

auth:
  username: "admin"
  password: "password"
  jwt_secret: "your-secret-here"

embedding:
  enabled: true
  provider: "local"
  model: "bge-m3"

kmeans:
  enabled: true

auto_tlp:
  enabled: false

heimdall:
  enabled: true
  model: "qwen2.5-0.5b-instruct"
  models_dir: "/usr/local/var/nornicdb/models"

database:
  encryption_password: ""  # Set to enable encryption
```

---

## 🛠️ Advanced Usage

### Command Line Access
NornicDB binary is installed at `/usr/local/bin/nornicdb`

```bash
# Start server manually
nornicdb serve

# With custom config
nornicdb serve --config ~/.nornicdb/config.yaml

# Specify ports
nornicdb serve --bolt-port 7687 --http-port 7474

# Check version
nornicdb version

# View help
nornicdb --help
```

### Service Management (Advanced Users)
```bash
# Check service status
launchctl list | grep nornicdb

# Manually start service
launchctl start com.nornicdb.server

# Manually stop service
launchctl stop com.nornicdb.server

# Restart service
launchctl kickstart -k gui/$(id -u)/com.nornicdb.server

# View logs
tail -f /usr/local/var/log/nornicdb/stdout.log
tail -f /usr/local/var/log/nornicdb/stderr.log
```

### Directory Structure
```
/usr/local/bin/nornicdb              # Binary
/usr/local/var/nornicdb/             # Data directory
  ├── data/                          # Database files
  └── models/                        # AI models (.gguf files)
/usr/local/var/log/nornicdb/         # Log files
  ├── stdout.log
  └── stderr.log
/Applications/NornicDB.app           # Menu bar app
~/.nornicdb/                         # User config
  ├── config.yaml                    # Configuration
  └── .first_run                     # First-run flag
~/Library/LaunchAgents/              # Auto-start config
  ├── com.nornicdb.server.plist      # Server service
  └── com.nornicdb.menubar.plist     # Menu bar app
```

---

## 🔐 Security Best Practices

### Change Default Credentials
The default credentials (`admin` / `password`) should be changed immediately:

1. Open **Settings** (⌘,)
2. Go to **Security** tab
3. Update **Username** and **Password**
4. Generate a new **JWT Secret**
5. Click **Save & Restart Server**

### Enable Encryption
For sensitive data:

1. Open **Settings** → **Security** tab
2. Enable **Encryption at Rest**
3. Generate a **strong encryption key** (16+ characters)
4. **Save this key securely** - data cannot be recovered without it
5. Click **Save & Restart Server**

### API Token Generation
For external services (MCP servers, automation):

1. Navigate to `http://localhost:7474/security` (requires admin login)
2. Set **Token Subject** (e.g., "MCP Server")
3. Choose **Expiry** (1 hour, 1 day, 1 week, 30 days, never)
4. Click **Generate Token**
5. **Copy token immediately** - it won't be shown again
6. Use in your application as `Authorization: Bearer <token>`

---

## 🗑️ Uninstallation

To completely remove NornicDB:

```bash
# Run uninstall script
/usr/local/bin/nornicdb-uninstall

# Or manually:
launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist
launchctl unload ~/Library/LaunchAgents/com.nornicdb.menubar.plist
rm ~/Library/LaunchAgents/com.nornicdb.*.plist
rm -rf /usr/local/var/nornicdb
rm -rf /usr/local/var/log/nornicdb
rm /usr/local/bin/nornicdb
rm -rf /Applications/NornicDB.app
rm -rf ~/.nornicdb
```

---

## 📊 Troubleshooting

### Server Won't Start
**Check logs**:
```bash
tail -n 50 /usr/local/var/log/nornicdb/stderr.log
```

**Common causes**:
- Port already in use (7687 or 7474)
- Permission issues with data directory
- Corrupt database (restore from backup)

**Solution**:
```bash
# Check port usage
lsof -i :7687
lsof -i :7474

# Fix permissions
sudo chown -R $(whoami) /usr/local/var/nornicdb

# Reset database (⚠️ destroys data)
rm -rf /usr/local/var/nornicdb/data
```

### Menu Bar Icon Missing
**Check if app is running**:
```bash
ps aux | grep NornicDB
```

**Restart menu bar app**:
```bash
killall NornicDB
open /Applications/NornicDB.app
```

### Can't Login to Web UI
**Verify credentials**:
```bash
cat ~/.nornicdb/config.yaml | grep -A3 "^auth:"
```

**Default credentials**:
- Username: `admin`
- Password: `password`

**Reset to defaults** (edit `~/.nornicdb/config.yaml`):
```yaml
auth:
  username: "admin"
  password: "password"
```

Then restart server from menu bar.

### Embeddings Not Working (Selected "Basic")
**Check feature flags**:
```bash
cat ~/.nornicdb/config.yaml | grep "enabled:"
```

**Enable embeddings**:
1. Open **Settings** → **Features** tab
2. Check ✅ **Enable Embeddings**
3. Click **Save & Restart Server**

### AI Models Not Found
**Download models**:
1. Click menu bar icon → **Download Models**
2. Or use the wizard: Delete `~/.nornicdb/.first_run` and reopen app

**Manual download**:
```bash
# BGE-M3 embedding model
curl -L "https://huggingface.co/nornicAI/bge-m3-GGUF/resolve/main/bge-m3-Q8_0.gguf" \
  -o /usr/local/var/nornicdb/models/bge-m3.gguf

# Qwen2.5 LLM
curl -L "https://huggingface.co/nornicAI/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q8_0.gguf" \
  -o /usr/local/var/nornicdb/models/qwen2.5-0.5b-instruct.gguf
```

---

## 🆘 Getting Help

- **Documentation**: [https://nornicdb.com/docs](https://nornicdb.com/docs)
- **GitHub Issues**: [https://github.com/orneryd/nornicdb/issues](https://github.com/orneryd/nornicdb/issues)
- **Discussions**: [https://github.com/orneryd/NornicDB/issues](https://github.com/orneryd/NornicDB/issues)

When reporting issues, include:
1. macOS version: `sw_vers`
2. NornicDB version: `nornicdb version`
3. Server logs: Last 50 lines of `/usr/local/var/log/nornicdb/stderr.log`
4. Config: `cat ~/.nornicdb/config.yaml` (redact passwords!)

---

## 🎯 What's Next?

### Recommended First Steps
1. ✅ Change default credentials (Settings → Security)
2. ✅ Choose your feature preset (Basic / Standard / Advanced)
3. ✅ Download AI models if using embeddings or Heimdall
4. ✅ Connect with Neo4j driver: `bolt://localhost:7687`
5. ✅ Explore the Web UI: `http://localhost:7474`

### Learning Resources
- **Quick Start Guide**: Learn Cypher queries and graph modeling
- **Performance Tuning**: Optimize for your workload
- **Integration Examples**: Connect from Python, JavaScript, Java, Go
- **AI Features Deep Dive**: Embeddings, clustering, and Heimdall

---

**Thank you for choosing NornicDB!** 🚀

*The next-generation graph database that's faster than Neo4j, easier to use, and AI-powered.*
