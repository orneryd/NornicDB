# Linux Deployment Plan

## Overview

NornicDB on Linux supports multiple distribution methods:
1. **systemd Service** - Direct binary installation with systemd
2. **.deb Package** - Debian/Ubuntu native package
3. **.rpm Package** - RHEL/CentOS/Fedora native package
4. **Snap** - Universal Linux package
5. **Docker** - Container deployment (already available)

All methods install NornicDB as a systemd service that runs in the background.

## Target Architectures

| Architecture | Hardware | Binary | Use Case |
|--------------|----------|--------|----------|
| amd64 | Intel/AMD 64-bit | `nornicdb-linux-amd64` | Servers, VPS, Desktop |
| arm64 | ARM 64-bit | `nornicdb-linux-arm64` | AWS Graviton, Ampere, Jetson |

## Method 1: systemd Service (Quick Install)

### User Experience
```bash
# Download and install
curl -Lo /usr/local/bin/nornicdb \
  https://github.com/timothyswt/nornicdb/releases/latest/download/nornicdb-linux-amd64
chmod +x /usr/local/bin/nornicdb

# Install systemd service
sudo nornicdb install

# Start
sudo systemctl start nornicdb
sudo systemctl enable nornicdb

# Check status
sudo systemctl status nornicdb
journalctl -u nornicdb -f
```

### Implementation

#### 1. systemd Unit File (`nornicdb.service`)
```ini
[Unit]
Description=NornicDB Graph Database
Documentation=https://github.com/timothyswt/nornicdb
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=nornicdb
Group=nornicdb
ExecStart=/usr/local/bin/nornicdb serve --data-dir /var/lib/nornicdb
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5
TimeoutStopSec=30

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
ReadWritePaths=/var/lib/nornicdb /var/log/nornicdb

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Environment
Environment=NORNICDB_LOG_LEVEL=info

[Install]
WantedBy=multi-user.target
```

#### 2. Install Command Implementation
Add to `cmd/nornicdb/main.go`:
```go
func installService() error {
    // Create user
    exec.Command("useradd", "-r", "-s", "/bin/false", "nornicdb").Run()
    
    // Create directories
    os.MkdirAll("/var/lib/nornicdb", 0755)
    os.MkdirAll("/var/log/nornicdb", 0755)
    exec.Command("chown", "-R", "nornicdb:nornicdb", "/var/lib/nornicdb").Run()
    exec.Command("chown", "-R", "nornicdb:nornicdb", "/var/log/nornicdb").Run()
    
    // Install systemd unit
    serviceFile := `[Unit]
Description=NornicDB Graph Database
...`
    ioutil.WriteFile("/etc/systemd/system/nornicdb.service", []byte(serviceFile), 0644)
    
    // Reload systemd
    exec.Command("systemctl", "daemon-reload").Run()
    
    fmt.Println("✓ NornicDB service installed")
    fmt.Println("  Start with: sudo systemctl start nornicdb")
    fmt.Println("  Enable on boot: sudo systemctl enable nornicdb")
    return nil
}
```

---

## Method 2: .deb Package (Debian/Ubuntu)

### User Experience
```bash
# Download and install
wget https://github.com/timothyswt/nornicdb/releases/latest/download/nornicdb_1.0.0_amd64.deb
sudo dpkg -i nornicdb_1.0.0_amd64.deb

# Or via APT repository
echo "deb https://apt.nornicdb.io stable main" | sudo tee /etc/apt/sources.list.d/nornicdb.list
curl -fsSL https://apt.nornicdb.io/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/nornicdb.gpg
sudo apt update
sudo apt install nornicdb

# Service is automatically enabled
sudo systemctl status nornicdb
```

### Implementation

#### 1. Directory Structure
```
packaging/deb/
├── DEBIAN/
│   ├── control           # Package metadata
│   ├── conffiles         # Config files to preserve
│   ├── preinst           # Pre-install script
│   ├── postinst          # Post-install script
│   ├── prerm             # Pre-remove script
│   └── postrm            # Post-remove script
├── usr/
│   └── bin/
│       └── nornicdb      # Binary
├── etc/
│   └── nornicdb/
│       └── config.yaml   # Default config
└── lib/
    └── systemd/
        └── system/
            └── nornicdb.service
```

#### 2. Control File (`DEBIAN/control`)
```
Package: nornicdb
Version: 1.0.0
Section: database
Priority: optional
Architecture: amd64
Maintainer: NornicDB Team <support@nornicdb.io>
Description: Lightweight graph database with vector search
 NornicDB is a Neo4j-compatible graph database optimized
 for AI/LLM memory and knowledge graphs. Features include
 vector similarity search, hybrid BM25+vector search,
 and MCP tool integration for AI agents.
Homepage: https://github.com/timothyswt/nornicdb
Depends: libc6 (>= 2.17)
```

#### 3. Post-Install Script (`DEBIAN/postinst`)
```bash
#!/bin/bash
set -e

case "$1" in
    configure)
        # Create system user
        if ! getent passwd nornicdb > /dev/null; then
            useradd -r -s /bin/false -d /var/lib/nornicdb nornicdb
        fi
        
        # Create directories
        mkdir -p /var/lib/nornicdb
        mkdir -p /var/log/nornicdb
        chown -R nornicdb:nornicdb /var/lib/nornicdb
        chown -R nornicdb:nornicdb /var/log/nornicdb
        
        # Enable and start service
        systemctl daemon-reload
        systemctl enable nornicdb
        systemctl start nornicdb || true
        
        echo ""
        echo "NornicDB installed successfully!"
        echo "Access at: http://localhost:7474"
        echo ""
        echo "Manage with:"
        echo "  sudo systemctl status nornicdb"
        echo "  sudo systemctl restart nornicdb"
        echo "  sudo journalctl -u nornicdb -f"
        ;;
esac

exit 0
```

#### 4. Pre-Remove Script (`DEBIAN/prerm`)
```bash
#!/bin/bash
set -e

case "$1" in
    remove|upgrade)
        systemctl stop nornicdb || true
        systemctl disable nornicdb || true
        ;;
esac

exit 0
```

#### 5. Build Script (`build-deb.sh`)
```bash
#!/bin/bash
set -e

VERSION=${1:-"1.0.0"}
ARCH=${2:-"amd64"}

echo "Building nornicdb_${VERSION}_${ARCH}.deb"

# Create package structure
PKG_DIR="build/deb/nornicdb_${VERSION}_${ARCH}"
rm -rf "$PKG_DIR"
mkdir -p "$PKG_DIR/DEBIAN"
mkdir -p "$PKG_DIR/usr/bin"
mkdir -p "$PKG_DIR/etc/nornicdb"
mkdir -p "$PKG_DIR/lib/systemd/system"

# Copy files
cp "bin/nornicdb-linux-${ARCH}" "$PKG_DIR/usr/bin/nornicdb"
cp packaging/deb/DEBIAN/* "$PKG_DIR/DEBIAN/"
cp packaging/deb/nornicdb.service "$PKG_DIR/lib/systemd/system/"
cp packaging/deb/config.yaml "$PKG_DIR/etc/nornicdb/"

# Update version in control file
sed -i "s/Version:.*/Version: ${VERSION}/" "$PKG_DIR/DEBIAN/control"
sed -i "s/Architecture:.*/Architecture: ${ARCH}/" "$PKG_DIR/DEBIAN/control"

# Set permissions
chmod 755 "$PKG_DIR/usr/bin/nornicdb"
chmod 755 "$PKG_DIR/DEBIAN/postinst"
chmod 755 "$PKG_DIR/DEBIAN/prerm"
chmod 755 "$PKG_DIR/DEBIAN/postrm"
chmod 644 "$PKG_DIR/lib/systemd/system/nornicdb.service"

# Build package
dpkg-deb --build "$PKG_DIR" "dist/nornicdb_${VERSION}_${ARCH}.deb"

echo "✓ Built: dist/nornicdb_${VERSION}_${ARCH}.deb"
```

---

## Method 3: .rpm Package (RHEL/CentOS/Fedora)

### User Experience
```bash
# Download and install
wget https://github.com/timothyswt/nornicdb/releases/latest/download/nornicdb-1.0.0.x86_64.rpm
sudo rpm -i nornicdb-1.0.0.x86_64.rpm

# Or via YUM/DNF repository
sudo tee /etc/yum.repos.d/nornicdb.repo << EOF
[nornicdb]
name=NornicDB Repository
baseurl=https://rpm.nornicdb.io/stable/
enabled=1
gpgcheck=1
gpgkey=https://rpm.nornicdb.io/gpg
EOF
sudo dnf install nornicdb

# Service is automatically enabled
sudo systemctl status nornicdb
```

### Implementation

#### 1. RPM Spec File (`nornicdb.spec`)
```spec
Name:           nornicdb
Version:        1.0.0
Release:        1%{?dist}
Summary:        Lightweight graph database with vector search

License:        MIT
URL:            https://github.com/timothyswt/nornicdb
Source0:        nornicdb-linux-amd64

Requires:       systemd

%description
NornicDB is a Neo4j-compatible graph database optimized
for AI/LLM memory and knowledge graphs.

%install
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/var/lib/nornicdb
mkdir -p %{buildroot}/var/log/nornicdb
mkdir -p %{buildroot}/etc/nornicdb
mkdir -p %{buildroot}/usr/lib/systemd/system

install -m 755 %{SOURCE0} %{buildroot}/usr/bin/nornicdb
install -m 644 nornicdb.service %{buildroot}/usr/lib/systemd/system/

%pre
getent passwd nornicdb >/dev/null || useradd -r -s /sbin/nologin nornicdb

%post
%systemd_post nornicdb.service

%preun
%systemd_preun nornicdb.service

%postun
%systemd_postun_with_restart nornicdb.service

%files
%attr(755, root, root) /usr/bin/nornicdb
%attr(644, root, root) /usr/lib/systemd/system/nornicdb.service
%dir %attr(755, nornicdb, nornicdb) /var/lib/nornicdb
%dir %attr(755, nornicdb, nornicdb) /var/log/nornicdb
%dir %attr(755, root, root) /etc/nornicdb

%changelog
* Mon Jan 01 2024 NornicDB Team <support@nornicdb.io> - 1.0.0-1
- Initial release
```

#### 2. Build Script (`build-rpm.sh`)
```bash
#!/bin/bash
set -e

VERSION=${1:-"1.0.0"}

echo "Building nornicdb-${VERSION}.x86_64.rpm"

# Setup rpmbuild structure
mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# Copy sources
cp bin/nornicdb-linux-amd64 ~/rpmbuild/SOURCES/
cp packaging/rpm/nornicdb.service ~/rpmbuild/SOURCES/
cp packaging/rpm/nornicdb.spec ~/rpmbuild/SPECS/

# Build RPM
rpmbuild -bb ~/rpmbuild/SPECS/nornicdb.spec

# Copy to dist
cp ~/rpmbuild/RPMS/x86_64/nornicdb-${VERSION}*.rpm dist/

echo "✓ Built: dist/nornicdb-${VERSION}.x86_64.rpm"
```

---

## Service Management

After installation, users can manage the service:

```bash
# Start/Stop/Restart
sudo systemctl start nornicdb
sudo systemctl stop nornicdb
sudo systemctl restart nornicdb

# Enable/Disable on boot
sudo systemctl enable nornicdb
sudo systemctl disable nornicdb

# Check status
sudo systemctl status nornicdb

# View logs
sudo journalctl -u nornicdb -f
sudo journalctl -u nornicdb --since "1 hour ago"

# Reload configuration
sudo systemctl reload nornicdb
```

## Data Locations

| Type | Path |
|------|------|
| Binary | `/usr/bin/nornicdb` or `/usr/local/bin/nornicdb` |
| Data | `/var/lib/nornicdb/` |
| Logs | `/var/log/nornicdb/` |
| Config | `/etc/nornicdb/config.yaml` |
| systemd | `/lib/systemd/system/nornicdb.service` |
| PID | `/run/nornicdb/nornicdb.pid` (optional) |

## Security Considerations

### Firewall (firewalld)
```bash
sudo firewall-cmd --permanent --add-port=7474/tcp
sudo firewall-cmd --permanent --add-port=7687/tcp
sudo firewall-cmd --reload
```

### Firewall (ufw)
```bash
sudo ufw allow 7474/tcp
sudo ufw allow 7687/tcp
```

### SELinux (RHEL/CentOS)
```bash
# Allow NornicDB to bind to ports
sudo semanage port -a -t http_port_t -p tcp 7474
sudo semanage port -a -t http_port_t -p tcp 7687

# Or set permissive for the service
sudo semanage permissive -a nornicdb_t
```

## Implementation Checklist

- [ ] Create systemd unit file
- [ ] Implement `nornicdb install` command
- [ ] Create .deb package structure
- [ ] Test on Debian 11/12
- [ ] Test on Ubuntu 20.04/22.04/24.04
- [ ] Create .rpm spec file
- [ ] Test on RHEL 8/9
- [ ] Test on CentOS Stream 8/9
- [ ] Test on Fedora 38/39
- [ ] Set up APT repository
- [ ] Set up YUM/DNF repository
- [ ] Add to GitHub Actions release workflow
- [ ] Document manual installation
- [ ] Document systemd hardening options
