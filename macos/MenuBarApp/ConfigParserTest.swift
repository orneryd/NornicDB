#!/usr/bin/env swift

import Foundation

// Helper function for separator lines
func separator() -> String {
    return String(repeating: "=", count: 60)
}

// Copy of parsing functions from NornicDBMenuBar.swift for testing
func extractYAMLSection(named sectionName: String, from content: String) -> String? {
    let lines = content.components(separatedBy: .newlines)
    var inSection = false
    var sectionLines: [String] = []
    
    for line in lines {
        // Check if this is the start of our target section (no leading whitespace)
        if line.hasPrefix("\(sectionName):") && !line.hasPrefix(" ") && !line.hasPrefix("\t") {
            inSection = true
            continue
        }
        
        // If we're in the section, check if we've hit another top-level key
        if inSection {
            // A line that starts with a non-whitespace character and contains ":" is a new section
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if !line.isEmpty && !line.hasPrefix(" ") && !line.hasPrefix("\t") && !line.hasPrefix("#") && trimmed.contains(":") {
                // We've hit a new top-level section, stop
                break
            }
            sectionLines.append(line)
        }
    }
    
    return sectionLines.isEmpty ? nil : sectionLines.joined(separator: "\n")
}

func getYAMLBool(key: String, from section: String, default defaultValue: Bool = false) -> Bool {
    for line in section.components(separatedBy: .newlines) {
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        if trimmed.hasPrefix("\(key):") {
            if trimmed.contains("true") { return true }
            if trimmed.contains("false") { return false }
        }
    }
    return defaultValue
}

func getYAMLString(key: String, from section: String) -> String? {
    for line in section.components(separatedBy: .newlines) {
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        if trimmed.hasPrefix("\(key):") {
            let value = trimmed.dropFirst("\(key):".count).trimmingCharacters(in: .whitespaces)
            // Remove quotes if present
            if value.hasPrefix("\"") && value.hasSuffix("\"") {
                return String(value.dropFirst().dropLast())
            }
            if value.hasPrefix("'") && value.hasSuffix("'") {
                return String(value.dropFirst().dropLast())
            }
            return value.isEmpty ? nil : value
        }
    }
    return nil
}

// Test with ACTUAL config file content
let testConfig = """
# NornicDB Configuration (Full Edition)
# Edit via Settings app (⌘,) or manually

server:
  port: 7687
  host: localhost
  bolt_port: 7687
  http_port: 7474

storage:
  path: "/usr/local/var/nornicdb/data"

database:
  encryption_enabled: false
  encryption_password: ""

embedding:
  enabled: true
  model: bge-m3
  provider: "local"

kmeans:
  enabled: true

heimdall:
  enabled: true
  model: qwen2.5-0.5b-instruct.gguf

auth:
  username: admin
  password: password
  jwt_secret: "[stored-in-keychain]"

auto_tlp:
  enabled: true
"""

print(separator())
print("YAML PARSING TEST")
print(separator())

// Test embedding section
print("\n📦 Testing EMBEDDING section:")
if let embeddingSection = extractYAMLSection(named: "embedding", from: testConfig) {
    print("  ✅ Section found:")
    print("     ---")
    for line in embeddingSection.components(separatedBy: .newlines) {
        print("     \(line)")
    }
    print("     ---")
    
    let enabled = getYAMLBool(key: "enabled", from: embeddingSection)
    print("  enabled: \(enabled) \(enabled == true ? "✅" : "❌ EXPECTED: true")")
    
    if let model = getYAMLString(key: "model", from: embeddingSection) {
        print("  model: '\(model)' \(model == "bge-m3" ? "✅" : "❌ EXPECTED: bge-m3")")
    } else {
        print("  model: nil ❌ EXPECTED: bge-m3")
    }
    
    if let provider = getYAMLString(key: "provider", from: embeddingSection) {
        print("  provider: '\(provider)' \(provider == "local" ? "✅" : "❌ EXPECTED: local")")
    } else {
        print("  provider: nil ❌ EXPECTED: local")
    }
} else {
    print("  ❌ FAILED: Could not extract embedding section!")
}

// Test heimdall section
print("\n🔮 Testing HEIMDALL section:")
if let heimdallSection = extractYAMLSection(named: "heimdall", from: testConfig) {
    print("  ✅ Section found:")
    print("     ---")
    for line in heimdallSection.components(separatedBy: .newlines) {
        print("     \(line)")
    }
    print("     ---")
    
    let enabled = getYAMLBool(key: "enabled", from: heimdallSection)
    print("  enabled: \(enabled) \(enabled == true ? "✅" : "❌ EXPECTED: true")")
    
    if let model = getYAMLString(key: "model", from: heimdallSection) {
        print("  model: '\(model)' \(model == "qwen2.5-0.5b-instruct.gguf" ? "✅" : "❌ EXPECTED: qwen2.5-0.5b-instruct.gguf")")
    } else {
        print("  model: nil ❌ EXPECTED: qwen2.5-0.5b-instruct.gguf")
    }
} else {
    print("  ❌ FAILED: Could not extract heimdall section!")
}

// Test server section
print("\n🖥️ Testing SERVER section:")
if let serverSection = extractYAMLSection(named: "server", from: testConfig) {
    print("  ✅ Section found")
    
    if let boltPort = getYAMLString(key: "bolt_port", from: serverSection) {
        print("  bolt_port: '\(boltPort)' \(boltPort == "7687" ? "✅" : "❌")")
    }
    if let httpPort = getYAMLString(key: "http_port", from: serverSection) {
        print("  http_port: '\(httpPort)' \(httpPort == "7474" ? "✅" : "❌")")
    }
} else {
    print("  ❌ FAILED: Could not extract server section!")
}

// Test auth section
print("\n🔐 Testing AUTH section:")
if let authSection = extractYAMLSection(named: "auth", from: testConfig) {
    print("  ✅ Section found")
    
    if let jwtSecret = getYAMLString(key: "jwt_secret", from: authSection) {
        print("  jwt_secret: '\(jwtSecret)' ✅")
    } else {
        print("  jwt_secret: nil ❌")
    }
} else {
    print("  ❌ FAILED: Could not extract auth section!")
}

// Now test with ACTUAL file
print("\n" + separator())
print("TESTING WITH ACTUAL CONFIG FILE")
print(separator())

let configPath = NSString(string: "~/.nornicdb/config.yaml").expandingTildeInPath
if let actualConfig = try? String(contentsOfFile: configPath, encoding: .utf8) {
    print("📄 Loaded config from: \(configPath)")
    print("   File size: \(actualConfig.count) chars")
    
    print("\n📦 EMBEDDING from actual file:")
    if let embeddingSection = extractYAMLSection(named: "embedding", from: actualConfig) {
        print("  Section content:")
        for line in embeddingSection.components(separatedBy: .newlines) {
            print("    '\(line)'")
        }
        
        let enabled = getYAMLBool(key: "enabled", from: embeddingSection)
        let model = getYAMLString(key: "model", from: embeddingSection)
        let provider = getYAMLString(key: "provider", from: embeddingSection)
        
        print("  Parsed values:")
        print("    enabled: \(enabled)")
        print("    model: \(model ?? "nil")")
        print("    provider: \(provider ?? "nil")")
    } else {
        print("  ❌ Could not extract embedding section!")
        print("  Raw file content around 'embedding':")
        if let range = actualConfig.range(of: "embedding") {
            let start = actualConfig.index(range.lowerBound, offsetBy: -20, limitedBy: actualConfig.startIndex) ?? actualConfig.startIndex
            let end = actualConfig.index(range.upperBound, offsetBy: 100, limitedBy: actualConfig.endIndex) ?? actualConfig.endIndex
            print("  ...\(actualConfig[start..<end])...")
        }
    }
    
    print("\n🔮 HEIMDALL from actual file:")
    if let heimdallSection = extractYAMLSection(named: "heimdall", from: actualConfig) {
        print("  Section content:")
        for line in heimdallSection.components(separatedBy: .newlines) {
            print("    '\(line)'")
        }
        
        let enabled = getYAMLBool(key: "enabled", from: heimdallSection)
        let model = getYAMLString(key: "model", from: heimdallSection)
        
        print("  Parsed values:")
        print("    enabled: \(enabled)")
        print("    model: \(model ?? "nil")")
    } else {
        print("  ❌ Could not extract heimdall section!")
    }
} else {
    print("❌ Could not read config file at: \(configPath)")
}

print("\n" + separator())
print("TEST COMPLETE")
print(separator())
