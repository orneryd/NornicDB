# Windows Deployment Plan

## Overview

NornicDB on Windows supports multiple distribution methods:
1. **MSI Installer + Windows Service** - Enterprise/end-user, GUI installation
2. **Chocolatey** - Developer-focused, command-line installation
3. **winget** - Microsoft's official package manager
4. **Portable ZIP** - No installation required

All methods can optionally install NornicDB as a Windows Service that runs in the background.

## Target Architecture

| Architecture | Hardware | Binary |
|--------------|----------|--------|
| amd64 | Intel/AMD 64-bit | `nornicdb.exe` |

## Method 1: MSI Installer (Recommended)

### User Experience
1. Download `NornicDB-1.0.0.msi` from releases
2. Double-click to install
3. Follow installation wizard
4. Service starts automatically
5. Access at `http://localhost:7474`

### Implementation

#### 1. Directory Structure
```
packaging/windows/
├── wix/
│   ├── Product.wxs           # Main WiX definition
│   ├── Service.wxs           # Windows Service config
│   └── UI.wxs                # Installer UI
├── service/
│   ├── nornicdb-service.xml  # WinSW configuration
│   └── WinSW.exe             # Service wrapper
├── scripts/
│   ├── install.ps1           # Post-install script
│   └── uninstall.ps1         # Pre-uninstall script
└── build-msi.ps1             # Build script
```

#### 2. WiX Installer Definition (`Product.wxs`)
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">
    <Product Id="*" 
             Name="NornicDB" 
             Language="1033" 
             Version="1.0.0.0" 
             Manufacturer="NornicDB" 
             UpgradeCode="YOUR-GUID-HERE">
        
        <Package InstallerVersion="200" 
                 Compressed="yes" 
                 InstallScope="perMachine" />

        <MajorUpgrade DowngradeErrorMessage="A newer version is already installed." />
        <MediaTemplate EmbedCab="yes" />

        <Feature Id="ProductFeature" Title="NornicDB" Level="1">
            <ComponentGroupRef Id="ProductComponents" />
            <ComponentGroupRef Id="ServiceComponents" />
        </Feature>

        <!-- Properties -->
        <Property Id="WIXUI_INSTALLDIR" Value="INSTALLFOLDER" />
        <Property Id="INSTALL_SERVICE" Value="1" />
        
        <!-- Custom Actions -->
        <CustomAction Id="InstallService" 
                      Directory="INSTALLFOLDER"
                      ExeCommand="[INSTALLFOLDER]nornicdb-service.exe install"
                      Execute="deferred"
                      Impersonate="no"
                      Return="check" />
                      
        <CustomAction Id="StartService" 
                      Directory="INSTALLFOLDER"
                      ExeCommand="[INSTALLFOLDER]nornicdb-service.exe start"
                      Execute="deferred"
                      Impersonate="no"
                      Return="ignore" />
                      
        <CustomAction Id="StopService" 
                      Directory="INSTALLFOLDER"
                      ExeCommand="[INSTALLFOLDER]nornicdb-service.exe stop"
                      Execute="deferred"
                      Impersonate="no"
                      Return="ignore" />
                      
        <CustomAction Id="UninstallService" 
                      Directory="INSTALLFOLDER"
                      ExeCommand="[INSTALLFOLDER]nornicdb-service.exe uninstall"
                      Execute="deferred"
                      Impersonate="no"
                      Return="ignore" />

        <InstallExecuteSequence>
            <Custom Action="StopService" Before="InstallFiles">
                REMOVE="ALL"
            </Custom>
            <Custom Action="UninstallService" After="StopService">
                REMOVE="ALL"
            </Custom>
            <Custom Action="InstallService" After="InstallFiles">
                NOT REMOVE AND INSTALL_SERVICE
            </Custom>
            <Custom Action="StartService" After="InstallService">
                NOT REMOVE AND INSTALL_SERVICE
            </Custom>
        </InstallExecuteSequence>
    </Product>

    <Fragment>
        <Directory Id="TARGETDIR" Name="SourceDir">
            <Directory Id="ProgramFiles64Folder">
                <Directory Id="INSTALLFOLDER" Name="NornicDB">
                    <Directory Id="DataFolder" Name="data" />
                    <Directory Id="LogFolder" Name="logs" />
                </Directory>
            </Directory>
            <Directory Id="ProgramMenuFolder">
                <Directory Id="ApplicationProgramsFolder" Name="NornicDB" />
            </Directory>
        </Directory>
    </Fragment>

    <Fragment>
        <ComponentGroup Id="ProductComponents" Directory="INSTALLFOLDER">
            <Component Id="MainExecutable" Guid="YOUR-GUID-1">
                <File Id="NornicDBExe" 
                      Source="$(var.BuildDir)\nornicdb.exe" 
                      KeyPath="yes" />
                      
                <!-- Add to PATH -->
                <Environment Id="PATH" 
                             Name="PATH" 
                             Value="[INSTALLFOLDER]" 
                             Permanent="no" 
                             Part="last" 
                             Action="set" 
                             System="yes" />
            </Component>
            
            <Component Id="ServiceWrapper" Guid="YOUR-GUID-2">
                <File Id="ServiceExe" 
                      Source="$(var.BuildDir)\nornicdb-service.exe" />
                <File Id="ServiceConfig" 
                      Source="$(var.BuildDir)\nornicdb-service.xml" />
            </Component>
        </ComponentGroup>
        
        <ComponentGroup Id="ServiceComponents" Directory="INSTALLFOLDER">
            <!-- Data directory -->
            <Component Id="DataDir" Guid="YOUR-GUID-3">
                <CreateFolder Directory="DataFolder">
                    <Permission User="SYSTEM" GenericAll="yes" />
                    <Permission User="Administrators" GenericAll="yes" />
                </CreateFolder>
            </Component>
            
            <!-- Log directory -->
            <Component Id="LogDir" Guid="YOUR-GUID-4">
                <CreateFolder Directory="LogFolder">
                    <Permission User="SYSTEM" GenericAll="yes" />
                    <Permission User="Administrators" GenericAll="yes" />
                </CreateFolder>
            </Component>
        </ComponentGroup>
    </Fragment>
</Wix>
```

#### 3. WinSW Service Configuration (`nornicdb-service.xml`)
```xml
<service>
    <id>NornicDB</id>
    <name>NornicDB Graph Database</name>
    <description>Lightweight graph database with vector search and AI memory</description>
    <executable>%BASE%\nornicdb.exe</executable>
    <arguments>serve --data-dir "%BASE%\data"</arguments>
    
    <!-- Logging -->
    <log mode="roll-by-size">
        <sizeThreshold>10240</sizeThreshold>
        <keepFiles>8</keepFiles>
    </log>
    <logpath>%BASE%\logs</logpath>
    
    <!-- Service behavior -->
    <startmode>Automatic</startmode>
    <delayedAutoStart>true</delayedAutoStart>
    
    <!-- Recovery -->
    <onfailure action="restart" delay="10 sec"/>
    <onfailure action="restart" delay="20 sec"/>
    <onfailure action="restart" delay="30 sec"/>
    <resetfailure>1 hour</resetfailure>
    
    <!-- Environment -->
    <env name="NORNICDB_LOG_LEVEL" value="info"/>
    
    <!-- Run as LocalService for security -->
    <serviceaccount>
        <username>LocalService</username>
    </serviceaccount>
    
    <!-- Dependencies -->
    <depend>Tcpip</depend>
</service>
```

#### 4. Build Script (`build-msi.ps1`)
```powershell
param(
    [string]$Version = "1.0.0"
)

$ErrorActionPreference = "Stop"

Write-Host "Building NornicDB MSI v$Version" -ForegroundColor Cyan

# Paths
$BuildDir = "build\windows"
$DistDir = "dist"
$WixDir = "packaging\windows\wix"

# Create build directory
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
New-Item -ItemType Directory -Force -Path $DistDir | Out-Null

# Build Windows binary (cross-compile from Mac/Linux or native)
Write-Host "Building nornicdb.exe..."
$env:GOOS = "windows"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"
go build -o "$BuildDir\nornicdb.exe" .\cmd\nornicdb

# Download WinSW
$WinSWUrl = "https://github.com/winsw/winsw/releases/download/v2.12.0/WinSW-x64.exe"
if (-not (Test-Path "$BuildDir\nornicdb-service.exe")) {
    Write-Host "Downloading WinSW..."
    Invoke-WebRequest -Uri $WinSWUrl -OutFile "$BuildDir\nornicdb-service.exe"
}

# Copy service config
Copy-Item "packaging\windows\service\nornicdb-service.xml" $BuildDir

# Build MSI with WiX
Write-Host "Building MSI installer..."
$WixBin = "C:\Program Files (x86)\WiX Toolset v3.11\bin"

& "$WixBin\candle.exe" `
    -dBuildDir="$BuildDir" `
    -dVersion="$Version" `
    -out "$BuildDir\Product.wixobj" `
    "$WixDir\Product.wxs"

& "$WixBin\light.exe" `
    -ext WixUIExtension `
    -cultures:en-us `
    -out "$DistDir\NornicDB-$Version.msi" `
    "$BuildDir\Product.wixobj"

Write-Host "✓ Built: $DistDir\NornicDB-$Version.msi" -ForegroundColor Green
```

---

## Method 2: Chocolatey

### User Experience
```powershell
# Install
choco install nornicdb

# Start service
Start-Service NornicDB

# Check status
Get-Service NornicDB

# Uninstall
choco uninstall nornicdb
```

### Implementation

#### Package Structure
```
chocolatey/
├── nornicdb.nuspec
├── tools/
│   ├── chocolateyinstall.ps1
│   ├── chocolateyuninstall.ps1
│   └── LICENSE.txt
└── legal/
    └── VERIFICATION.txt
```

#### `nornicdb.nuspec`
```xml
<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://schemas.microsoft.com/packaging/2015/06/nuspec.xsd">
  <metadata>
    <id>nornicdb</id>
    <version>1.0.0</version>
    <title>NornicDB</title>
    <authors>NornicDB Team</authors>
    <owners>timothyswt</owners>
    <licenseUrl>https://github.com/timothyswt/nornicdb/blob/main/LICENSE</licenseUrl>
    <projectUrl>https://github.com/timothyswt/nornicdb</projectUrl>
    <iconUrl>https://raw.githubusercontent.com/timothyswt/nornicdb/main/icon.png</iconUrl>
    <requireLicenseAcceptance>false</requireLicenseAcceptance>
    <description>Lightweight graph database with vector search and AI memory</description>
    <summary>A Neo4j-compatible graph database optimized for AI/LLM memory</summary>
    <tags>database graph neo4j vector-search ai llm</tags>
    <releaseNotes>Initial release</releaseNotes>
  </metadata>
  <files>
    <file src="tools\**" target="tools" />
  </files>
</package>
```

#### `chocolateyinstall.ps1`
```powershell
$ErrorActionPreference = 'Stop'

$packageName = 'nornicdb'
$toolsDir = "$(Split-Path -Parent $MyInvocation.MyCommand.Definition)"
$url64 = 'https://github.com/timothyswt/nornicdb/releases/download/v1.0.0/nornicdb-windows-amd64.zip'
$checksum64 = 'PLACEHOLDER_SHA256'

# Install to Program Files
$installDir = Join-Path $env:ProgramFiles 'NornicDB'

Install-ChocolateyZipPackage `
    -PackageName $packageName `
    -Url64bit $url64 `
    -Checksum64 $checksum64 `
    -ChecksumType64 'sha256' `
    -UnzipLocation $installDir

# Add to PATH
Install-ChocolateyPath -PathToInstall $installDir -PathType 'Machine'

# Install as Windows Service
$serviceName = 'NornicDB'
$serviceExe = Join-Path $installDir 'nornicdb.exe'

# Download and configure WinSW
# ... (similar to MSI approach)

Write-Host "NornicDB installed successfully!" -ForegroundColor Green
Write-Host "Start the service with: Start-Service NornicDB"
```

---

## Method 3: winget

### User Experience
```powershell
winget install NornicDB.NornicDB
```

### Implementation

Submit a PR to [microsoft/winget-pkgs](https://github.com/microsoft/winget-pkgs):

```yaml
# manifests/n/NornicDB/NornicDB/1.0.0/NornicDB.NornicDB.yaml
PackageIdentifier: NornicDB.NornicDB
PackageVersion: 1.0.0
PackageName: NornicDB
Publisher: NornicDB
License: MIT
ShortDescription: Lightweight graph database with vector search
Installers:
  - Architecture: x64
    InstallerType: msi
    InstallerUrl: https://github.com/timothyswt/nornicdb/releases/download/v1.0.0/NornicDB-1.0.0.msi
    InstallerSha256: PLACEHOLDER
ManifestType: singleton
ManifestVersion: 1.0.0
```

---

## Service Management

After installation, users can manage the service:

```powershell
# PowerShell commands
Start-Service NornicDB
Stop-Service NornicDB
Restart-Service NornicDB
Get-Service NornicDB

# Command Prompt
net start NornicDB
net stop NornicDB

# Service Control (sc)
sc query NornicDB
sc start NornicDB
sc stop NornicDB
```

Or use the Services GUI: `services.msc`

## Data Locations

| Type | Path |
|------|------|
| Binary | `C:\Program Files\NornicDB\nornicdb.exe` |
| Data | `C:\Program Files\NornicDB\data\` |
| Logs | `C:\Program Files\NornicDB\logs\` |
| Config | `C:\ProgramData\NornicDB\config.yaml` (optional) |
| Service | Windows Service: "NornicDB" |

## Firewall Configuration

The installer should add a firewall rule:

```powershell
# Add during installation
New-NetFirewallRule `
    -DisplayName "NornicDB" `
    -Direction Inbound `
    -Protocol TCP `
    -LocalPort 7474,7687 `
    -Action Allow `
    -Program "C:\Program Files\NornicDB\nornicdb.exe"

# Remove during uninstallation
Remove-NetFirewallRule -DisplayName "NornicDB"
```

## Code Signing

For trusted installation without warnings:

1. Obtain a code signing certificate (DigiCert, Sectigo, etc.)
2. Sign the executable and MSI:

```powershell
# Sign executable
signtool sign /fd SHA256 /tr http://timestamp.digicert.com /td SHA256 `
    /f certificate.pfx /p PASSWORD nornicdb.exe

# Sign MSI
signtool sign /fd SHA256 /tr http://timestamp.digicert.com /td SHA256 `
    /f certificate.pfx /p PASSWORD NornicDB.msi
```

## Implementation Checklist

- [ ] Create WiX installer project
- [ ] Configure WinSW service wrapper
- [ ] Build MSI installer
- [ ] Test installation/uninstallation
- [ ] Create Chocolatey package
- [ ] Submit to Chocolatey community repository
- [ ] Create winget manifest
- [ ] Submit to winget-pkgs
- [ ] Set up code signing
- [ ] Add to GitHub Actions release workflow
- [ ] Test on Windows 10
- [ ] Test on Windows 11
- [ ] Test on Windows Server 2019/2022
- [ ] Document manual service installation
