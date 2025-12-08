// FileIndexer.swift
// Swift implementation of file indexing with .gitignore/.dockerignore support
// and Apple Vision integration for image descriptions

import Foundation
import NaturalLanguage
import Vision
import UniformTypeIdentifiers
import PDFKit

// MARK: - Ignore Pattern Handler

/// Handles .gitignore and .dockerignore pattern matching
class IgnorePatternHandler {
    private var patterns: [IgnorePattern] = []
    private var negatedPatterns: [IgnorePattern] = []
    
    struct IgnorePattern {
        let pattern: String
        let regex: NSRegularExpression?
        let isDirectory: Bool
        let isNegated: Bool
        
        init(pattern: String) {
            var p = pattern.trimmingCharacters(in: .whitespaces)
            
            // Check for negation
            self.isNegated = p.hasPrefix("!")
            if isNegated {
                p = String(p.dropFirst())
            }
            
            // Check if directory-only pattern
            self.isDirectory = p.hasSuffix("/")
            if isDirectory {
                p = String(p.dropLast())
            }
            
            self.pattern = p
            
            // Convert gitignore pattern to regex
            self.regex = IgnorePattern.patternToRegex(p)
        }
        
        static func patternToRegex(_ pattern: String) -> NSRegularExpression? {
            var regexPattern = pattern
            
            // Escape special regex characters except * and ?
            let escapeChars = [".", "+", "^", "$", "{", "}", "(", ")", "|", "[", "]", "\\"]
            for char in escapeChars {
                regexPattern = regexPattern.replacingOccurrences(of: char, with: "\\\(char)")
            }
            
            // Convert glob patterns to regex
            regexPattern = regexPattern.replacingOccurrences(of: "**", with: "<<<DOUBLESTAR>>>")
            regexPattern = regexPattern.replacingOccurrences(of: "*", with: "[^/]*")
            regexPattern = regexPattern.replacingOccurrences(of: "<<<DOUBLESTAR>>>", with: ".*")
            regexPattern = regexPattern.replacingOccurrences(of: "?", with: "[^/]")
            
            // Anchor pattern
            if !pattern.contains("/") {
                // Match in any directory
                regexPattern = "(^|.*/)\(regexPattern)(/.*)?$"
            } else if pattern.hasPrefix("/") {
                // Match from root only
                regexPattern = "^\(regexPattern.dropFirst())(/.*)?$"
            } else {
                regexPattern = "(^|.*/)\(regexPattern)(/.*)?$"
            }
            
            return try? NSRegularExpression(pattern: regexPattern, options: [])
        }
        
        func matches(_ path: String, isDirectory: Bool) -> Bool {
            guard let regex = regex else { return false }
            
            // Directory-only patterns only match directories
            if self.isDirectory && !isDirectory {
                return false
            }
            
            let range = NSRange(path.startIndex..., in: path)
            return regex.firstMatch(in: path, options: [], range: range) != nil
        }
    }
    
    init() {
        // Add default patterns
        addDefaultPatterns()
    }
    
    private func addDefaultPatterns() {
        let defaults = [
            "node_modules/",
            ".git/",
            ".DS_Store",
            "*.log",
            "build/",
            "dist/",
            "package-lock.json",
            ".Trash/",
            "*.pyc",
            "__pycache__/",
            ".cache/",
            "*.swp",
            "*.swo",
            "Thumbs.db"
        ]
        
        for pattern in defaults {
            addPattern(pattern)
        }
    }
    
    /// Load patterns from a file (e.g., .gitignore, .dockerignore)
    func loadIgnoreFile(at path: String) {
        let url = URL(fileURLWithPath: path)
        
        guard let content = try? String(contentsOf: url, encoding: .utf8) else {
            return
        }
        
        let lines = content.components(separatedBy: .newlines)
        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            
            // Skip empty lines and comments
            if trimmed.isEmpty || trimmed.hasPrefix("#") {
                continue
            }
            
            addPattern(trimmed)
        }
        
        print("✅ Loaded ignore patterns from \(path)")
    }
    
    /// Add a single pattern
    func addPattern(_ pattern: String) {
        let ignorePattern = IgnorePattern(pattern: pattern)
        
        if ignorePattern.isNegated {
            negatedPatterns.append(ignorePattern)
        } else {
            patterns.append(ignorePattern)
        }
    }
    
    /// Add multiple patterns
    func addPatterns(_ patterns: [String]) {
        for pattern in patterns {
            addPattern(pattern)
        }
    }
    
    /// Check if a path should be ignored
    func shouldIgnore(_ path: String, relativeTo root: String, isDirectory: Bool = false) -> Bool {
        // Get relative path
        var relativePath = path
        if path.hasPrefix(root) {
            relativePath = String(path.dropFirst(root.count))
            if relativePath.hasPrefix("/") {
                relativePath = String(relativePath.dropFirst())
            }
        }
        
        // Empty path means root - don't ignore
        if relativePath.isEmpty {
            return false
        }
        
        // Check if any pattern matches
        var isIgnored = false
        
        for pattern in patterns {
            if pattern.matches(relativePath, isDirectory: isDirectory) {
                isIgnored = true
                break
            }
        }
        
        // Check negated patterns (can un-ignore)
        if isIgnored {
            for pattern in negatedPatterns {
                if pattern.matches(relativePath, isDirectory: isDirectory) {
                    isIgnored = false
                    break
                }
            }
        }
        
        return isIgnored
    }
}

// MARK: - Document Parser

/// Extracts text content from various file formats
class DocumentParser {
    
    /// Supported document formats for text extraction
    static let supportedDocumentExtensions: Set<String> = [".pdf", ".docx", ".doc", ".rtf", ".txt"]
    
    /// Text file extensions
    static let textFileExtensions: Set<String> = [
        ".txt", ".md", ".markdown", ".rst",
        ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs",
        ".py", ".pyw", ".pyi",
        ".java", ".kt", ".kts", ".scala",
        ".go", ".rs", ".c", ".cpp", ".cc", ".cxx", ".h", ".hpp",
        ".cs", ".fs", ".vb",
        ".rb", ".php", ".pl", ".pm",
        ".swift", ".m", ".mm",
        ".sh", ".bash", ".zsh", ".fish",
        ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg",
        ".xml", ".html", ".htm", ".css", ".scss", ".sass", ".less",
        ".sql", ".graphql", ".gql",
        ".r", ".R", ".jl", ".lua", ".vim",
        ".dockerfile", ".makefile", ".cmake",
        ".env", ".properties", ".conf"
    ]
    
    /// Image file extensions
    static let imageExtensions: Set<String> = [
        ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".heic", ".heif", ".tiff", ".tif"
    ]
    
    /// Binary/skip extensions
    static let binaryExtensions: Set<String> = [
        ".exe", ".dll", ".so", ".dylib", ".bin", ".dat", ".app",
        ".zip", ".tar", ".gz", ".rar", ".7z", ".bz2",
        ".mp4", ".avi", ".mov", ".mkv", ".mp3", ".wav", ".m4a",
        ".ttf", ".otf", ".woff", ".woff2",
        ".db", ".sqlite", ".sqlite3",
        ".pyc", ".pyo", ".class", ".o", ".obj", ".wasm",
        ".ico", ".svg"
    ]
    
    /// Check if file is supported for text extraction
    func canExtractText(from path: String) -> Bool {
        let ext = (path as NSString).pathExtension.lowercased()
        let dotExt = ".\(ext)"
        
        return Self.textFileExtensions.contains(dotExt) ||
               Self.supportedDocumentExtensions.contains(dotExt)
    }
    
    /// Check if file is an image
    func isImage(_ path: String) -> Bool {
        let ext = (path as NSString).pathExtension.lowercased()
        return Self.imageExtensions.contains(".\(ext)")
    }
    
    /// Check if file should be skipped (binary)
    func shouldSkip(_ path: String) -> Bool {
        let ext = (path as NSString).pathExtension.lowercased()
        let dotExt = ".\(ext)"
        
        // Skip binary files
        if Self.binaryExtensions.contains(dotExt) {
            return true
        }
        
        // Skip lock files
        let filename = (path as NSString).lastPathComponent
        if filename.hasSuffix(".lock") || filename == "package-lock.json" || filename == "yarn.lock" {
            return true
        }
        
        // Skip sensitive files
        let sensitivePatterns = ["password", "secret", "credential", "token", "apikey", "private_key"]
        let lowercaseFilename = filename.lowercased()
        for pattern in sensitivePatterns {
            if lowercaseFilename.contains(pattern) {
                return true
            }
        }
        
        return false
    }
    
    /// Extract text from a file
    func extractText(from path: String) -> String? {
        let ext = (path as NSString).pathExtension.lowercased()
        let dotExt = ".\(ext)"
        
        // Plain text files
        if Self.textFileExtensions.contains(dotExt) {
            return try? String(contentsOfFile: path, encoding: .utf8)
        }
        
        // PDF
        if dotExt == ".pdf" {
            return extractPDFText(from: path)
        }
        
        // RTF
        if dotExt == ".rtf" {
            return extractRTFText(from: path)
        }
        
        // DOCX (basic extraction)
        if dotExt == ".docx" {
            return extractDOCXText(from: path)
        }
        
        return nil
    }
    
    /// Extract text from PDF
    private func extractPDFText(from path: String) -> String? {
        guard let pdf = PDFDocument(url: URL(fileURLWithPath: path)) else {
            return nil
        }
        
        var text = ""
        for i in 0..<pdf.pageCount {
            if let page = pdf.page(at: i), let pageText = page.string {
                text += pageText + "\n"
            }
        }
        
        return text.isEmpty ? nil : text
    }
    
    /// Extract text from RTF
    private func extractRTFText(from path: String) -> String? {
        guard let data = try? Data(contentsOf: URL(fileURLWithPath: path)),
              let attributedString = try? NSAttributedString(
                data: data,
                options: [.documentType: NSAttributedString.DocumentType.rtf],
                documentAttributes: nil
              ) else {
            return nil
        }
        
        return attributedString.string
    }
    
    /// Extract text from DOCX (basic - extracts from document.xml)
    private func extractDOCXText(from path: String) -> String? {
        // DOCX is a ZIP file containing XML
        // For a proper implementation, use a library like ZIPFoundation
        // This is a simplified version
        
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/unzip")
        process.arguments = ["-p", path, "word/document.xml"]
        
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = FileHandle.nullDevice
        
        do {
            try process.run()
            process.waitUntilExit()
            
            let data = pipe.fileHandleForReading.readDataToEndOfFile()
            guard let xml = String(data: data, encoding: .utf8) else {
                return nil
            }
            
            // Strip XML tags (basic extraction)
            let stripped = xml.replacingOccurrences(of: "<[^>]+>", with: " ", options: .regularExpression)
            let cleaned = stripped.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
            return cleaned.trimmingCharacters(in: .whitespaces)
        } catch {
            return nil
        }
    }
}

// MARK: - Apple Vision Image Describer

/// Uses Apple Vision framework to extract text and describe images
class AppleVisionImageDescriber {
    
    /// Check if Apple Vision is available for image description
    static var isAvailable: Bool {
        // VNRecognizeTextRequest is available on macOS 10.15+
        if #available(macOS 10.15, *) {
            return true
        }
        return false
    }
    
    /// Extract text from an image using OCR
    func extractTextFromImage(at path: String) -> String? {
        guard #available(macOS 10.15, *) else {
            return nil
        }
        
        guard let image = NSImage(contentsOfFile: path),
              let cgImage = image.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
            return nil
        }
        
        var recognizedText = ""
        let semaphore = DispatchSemaphore(value: 0)
        
        let request = VNRecognizeTextRequest { request, error in
            defer { semaphore.signal() }
            
            guard let observations = request.results as? [VNRecognizedTextObservation] else {
                return
            }
            
            for observation in observations {
                if let topCandidate = observation.topCandidates(1).first {
                    recognizedText += topCandidate.string + "\n"
                }
            }
        }
        
        request.recognitionLevel = .accurate
        request.usesLanguageCorrection = true
        
        let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
        
        do {
            try handler.perform([request])
            semaphore.wait()
        } catch {
            print("❌ Vision OCR error: \(error)")
            return nil
        }
        
        return recognizedText.isEmpty ? nil : recognizedText.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    
    /// Generate image classification labels
    @available(macOS 10.15, *)
    func classifyImage(at path: String) -> [String]? {
        guard let image = NSImage(contentsOfFile: path),
              let cgImage = image.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
            return nil
        }
        
        var labels: [String] = []
        let semaphore = DispatchSemaphore(value: 0)
        
        let request = VNClassifyImageRequest { request, error in
            defer { semaphore.signal() }
            
            guard let observations = request.results as? [VNClassificationObservation] else {
                return
            }
            
            // Get top 5 classifications with confidence > 0.1
            for observation in observations.prefix(5) {
                if observation.confidence > 0.1 {
                    labels.append(observation.identifier)
                }
            }
        }
        
        let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
        
        do {
            try handler.perform([request])
            semaphore.wait()
        } catch {
            print("❌ Vision classification error: \(error)")
            return nil
        }
        
        return labels.isEmpty ? nil : labels
    }
    
    /// Generate a description combining OCR text and classification
    func describeImage(at path: String) -> String? {
        var description = ""
        
        // Get OCR text
        if let ocrText = extractTextFromImage(at: path), !ocrText.isEmpty {
            description += "Text in image: \(ocrText)\n"
        }
        
        // Get classification labels
        if #available(macOS 10.15, *) {
            if let labels = classifyImage(at: path), !labels.isEmpty {
                description += "Image contains: \(labels.joined(separator: ", "))"
            }
        }
        
        return description.isEmpty ? nil : description
    }
}

// MARK: - File Indexer

/// Main file indexer that walks directories and extracts content
class FileIndexer {
    private let ignoreHandler: IgnorePatternHandler
    private let documentParser: DocumentParser
    private let imageDescriber: AppleVisionImageDescriber
    
    /// Whether to process images (requires Apple provider)
    var processImages: Bool = true
    
    /// Callback for indexed files
    var onFileIndexed: ((IndexedFile) -> Void)?
    
    /// Callback for progress updates
    var onProgress: ((Int, Int) -> Void)?  // (current, total)
    
    init() {
        self.ignoreHandler = IgnorePatternHandler()
        self.documentParser = DocumentParser()
        self.imageDescriber = AppleVisionImageDescriber()
    }
    
    /// Indexed file result
    struct IndexedFile {
        let path: String
        let relativePath: String
        let content: String
        let contentType: ContentType
        let size: Int64
        let fileExtension: String
        let language: String?
        let lastModified: Date
        
        enum ContentType {
            case text
            case document
            case imageDescription
            case imageOCR
        }
    }
    
    /// Index a directory
    func indexDirectory(
        at path: String,
        recursive: Bool = true,
        includeImages: Bool = true
    ) async -> [IndexedFile] {
        let rootPath = path
        
        // Load ignore files
        let gitignorePath = (path as NSString).appendingPathComponent(".gitignore")
        let dockerignorePath = (path as NSString).appendingPathComponent(".dockerignore")
        
        ignoreHandler.loadIgnoreFile(at: gitignorePath)
        ignoreHandler.loadIgnoreFile(at: dockerignorePath)
        
        // Collect all files
        var files: [String] = []
        await collectFiles(in: path, rootPath: rootPath, recursive: recursive, files: &files)
        
        print("📁 Found \(files.count) files to process")
        
        // Index files
        var indexedFiles: [IndexedFile] = []
        var processed = 0
        
        for filePath in files {
            processed += 1
            onProgress?(processed, files.count)
            
            // Check if image
            if documentParser.isImage(filePath) {
                if includeImages && processImages {
                    if let indexed = indexImage(at: filePath, relativeTo: rootPath) {
                        indexedFiles.append(indexed)
                        onFileIndexed?(indexed)
                    }
                }
                continue
            }
            
            // Check if we can extract text
            if documentParser.shouldSkip(filePath) {
                continue
            }
            
            if let indexed = indexTextFile(at: filePath, relativeTo: rootPath) {
                indexedFiles.append(indexed)
                onFileIndexed?(indexed)
            }
        }
        
        print("✅ Indexed \(indexedFiles.count) files")
        return indexedFiles
    }
    
    /// Recursively collect files
    private func collectFiles(in path: String, rootPath: String, recursive: Bool, files: inout [String]) async {
        let fileManager = FileManager.default
        
        guard let contents = try? fileManager.contentsOfDirectory(atPath: path) else {
            return
        }
        
        for item in contents {
            let fullPath = (path as NSString).appendingPathComponent(item)
            
            var isDirectory: ObjCBool = false
            guard fileManager.fileExists(atPath: fullPath, isDirectory: &isDirectory) else {
                continue
            }
            
            // Check if ignored
            if ignoreHandler.shouldIgnore(fullPath, relativeTo: rootPath, isDirectory: isDirectory.boolValue) {
                continue
            }
            
            if isDirectory.boolValue {
                if recursive {
                    await collectFiles(in: fullPath, rootPath: rootPath, recursive: recursive, files: &files)
                }
            } else {
                files.append(fullPath)
            }
        }
    }
    
    /// Index a text file
    private func indexTextFile(at path: String, relativeTo rootPath: String) -> IndexedFile? {
        guard let content = documentParser.extractText(from: path) else {
            return nil
        }
        
        // Skip if content is too small or appears binary
        if content.count < 10 || containsBinaryContent(content) {
            return nil
        }
        
        let ext = (path as NSString).pathExtension.lowercased()
        let relativePath = makeRelativePath(path, rootPath: rootPath)
        
        let attrs = try? FileManager.default.attributesOfItem(atPath: path)
        let size = (attrs?[.size] as? Int64) ?? 0
        let lastModified = (attrs?[.modificationDate] as? Date) ?? Date()
        
        let contentType: IndexedFile.ContentType = DocumentParser.supportedDocumentExtensions.contains(".\(ext)") ? .document : .text
        
        return IndexedFile(
            path: path,
            relativePath: relativePath,
            content: content,
            contentType: contentType,
            size: size,
            fileExtension: ext,
            language: detectLanguage(ext),
            lastModified: lastModified
        )
    }
    
    /// Index an image using Apple Vision
    private func indexImage(at path: String, relativeTo rootPath: String) -> IndexedFile? {
        guard AppleVisionImageDescriber.isAvailable else {
            return nil
        }
        
        // Try to get description/OCR
        guard let description = imageDescriber.describeImage(at: path), !description.isEmpty else {
            return nil
        }
        
        let ext = (path as NSString).pathExtension.lowercased()
        let relativePath = makeRelativePath(path, rootPath: rootPath)
        
        let attrs = try? FileManager.default.attributesOfItem(atPath: path)
        let size = (attrs?[.size] as? Int64) ?? 0
        let lastModified = (attrs?[.modificationDate] as? Date) ?? Date()
        
        // Determine content type based on what we got
        let hasOCR = description.contains("Text in image:")
        let contentType: IndexedFile.ContentType = hasOCR ? .imageOCR : .imageDescription
        
        return IndexedFile(
            path: path,
            relativePath: relativePath,
            content: description,
            contentType: contentType,
            size: size,
            fileExtension: ext,
            language: nil,
            lastModified: lastModified
        )
    }
    
    /// Make a relative path
    private func makeRelativePath(_ path: String, rootPath: String) -> String {
        if path.hasPrefix(rootPath) {
            var relative = String(path.dropFirst(rootPath.count))
            if relative.hasPrefix("/") {
                relative = String(relative.dropFirst())
            }
            return relative
        }
        return path
    }
    
    /// Detect programming language from extension
    private func detectLanguage(_ ext: String) -> String? {
        let languageMap: [String: String] = [
            "ts": "typescript", "tsx": "typescript",
            "js": "javascript", "jsx": "javascript", "mjs": "javascript",
            "py": "python", "pyw": "python",
            "java": "java", "kt": "kotlin",
            "go": "go", "rs": "rust",
            "c": "c", "cpp": "cpp", "cc": "cpp", "h": "c", "hpp": "cpp",
            "cs": "csharp", "fs": "fsharp",
            "rb": "ruby", "php": "php", "pl": "perl",
            "swift": "swift", "m": "objective-c",
            "sh": "bash", "bash": "bash", "zsh": "zsh",
            "json": "json", "yaml": "yaml", "yml": "yaml",
            "xml": "xml", "html": "html", "css": "css",
            "sql": "sql", "md": "markdown", "r": "r"
        ]
        return languageMap[ext]
    }
    
    /// Check if content appears to be binary
    private func containsBinaryContent(_ content: String) -> Bool {
        // Check for null bytes
        if content.contains("\0") {
            return true
        }
        
        // Check for high concentration of control characters
        let sampleSize = min(content.count, 8192)
        let sample = String(content.prefix(sampleSize))
        var controlCount = 0
        
        for char in sample.unicodeScalars {
            let code = char.value
            // Control chars 0x00-0x08, 0x0E-0x1F (excluding tab, newline, CR)
            if (code <= 0x08) || (code >= 0x0E && code <= 0x1F) {
                controlCount += 1
            }
        }
        
        // More than 10% control chars = likely binary
        return Double(controlCount) / Double(sampleSize) > 0.10
    }
}

// MARK: - File Indexer Service

/// Service that manages file indexing for NornicDB
class FileIndexerService {
    static let shared = FileIndexerService()
    
    private let indexer: FileIndexer
    private var isIndexing: Bool = false
    private var indexedFiles: [FileIndexer.IndexedFile] = []
    
    /// Whether to use Apple Vision for images
    var useAppleVisionForImages: Bool = true
    
    /// Embedding provider (determines image handling)
    var embeddingProvider: String = "local" {
        didSet {
            // Apple Vision image descriptions are ALWAYS available on macOS
            // They just get text from images that can then be embedded
            indexer.processImages = true
        }
    }
    
    private init() {
        self.indexer = FileIndexer()
    }
    
    /// Index a folder and prepare for embedding
    func indexFolder(at path: String, recursive: Bool = true) async -> [FileIndexer.IndexedFile] {
        guard !isIndexing else {
            print("⚠️ Indexing already in progress")
            return []
        }
        
        isIndexing = true
        defer { isIndexing = false }
        
        print("📂 Starting file indexing: \(path)")
        
        // Configure image handling based on provider
        let includeImages = useAppleVisionForImages
        
        // Index files
        let files = await indexer.indexDirectory(
            at: path,
            recursive: recursive,
            includeImages: includeImages
        )
        
        indexedFiles = files
        
        // Summary
        let textFiles = files.filter { $0.contentType == .text || $0.contentType == .document }
        let imageFiles = files.filter { $0.contentType == .imageDescription || $0.contentType == .imageOCR }
        
        print("📊 Indexing complete:")
        print("   - Text files: \(textFiles.count)")
        print("   - Images (with descriptions): \(imageFiles.count)")
        print("   - Total content size: \(formatBytes(files.reduce(0) { $0 + Int($1.size) }))")
        
        return files
    }
    
    /// Get indexed file content ready for embedding
    func getContentForEmbedding() -> [(path: String, content: String)] {
        return indexedFiles.map { ($0.relativePath, $0.content) }
    }
    
    /// Clear indexed files
    func clear() {
        indexedFiles = []
    }
    
    private func formatBytes(_ bytes: Int) -> String {
        let formatter = ByteCountFormatter()
        formatter.countStyle = .file
        return formatter.string(fromByteCount: Int64(bytes))
    }
}
