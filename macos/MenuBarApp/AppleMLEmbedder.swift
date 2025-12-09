import Foundation
import NaturalLanguage

/// AppleMLEmbedder provides text embeddings using Apple's NaturalLanguage framework.
/// This provides on-device, privacy-first embeddings without external dependencies.
///
/// Features:
/// - Uses Apple's native ML models (no downloads required)
/// - Supports multiple languages automatically
/// - Fast, on-device processing
/// - Zero cost (no API calls)
///
/// Example:
/// ```swift
/// let embedder = AppleMLEmbedder()
/// let embedding = try embedder.embed(text: "Hello, world!")
/// print("Dimensions: \(embedding.count)")
/// ```
class AppleMLEmbedder {
    
    // MARK: - Properties
    
    /// The embedding model to use
    private let embedding: NLEmbedding?
    
    /// Embedding dimensions (varies by model)
    let dimensions: Int
    
    /// Model identifier
    let model: String
    
    /// Supported embedding revisions
    enum EmbeddingRevision: Int {
        case revision1 = 1
        case revision2 = 2
        case revision3 = 3
    }
    
    // MARK: - Initialization
    
    /// Initialize embedder with specified revision
    /// - Parameter revision: Embedding model revision (1, 2, or 3)
    init(revision: EmbeddingRevision = .revision3) {
        // Try to load sentence embeddings in order of preference
        // Revision 3 is the latest and most accurate (macOS 13+)
        // Revision 2 is available on macOS 12+
        // Revision 1 is the oldest
        
        var loadedEmbedding: NLEmbedding? = nil
        var loadedModel = ""
        
        // Try specified revision first
        if let embedding = NLEmbedding.sentenceEmbedding(for: .english, revision: revision.rawValue) {
            loadedEmbedding = embedding
            loadedModel = "apple-ml-sentence-v\(revision.rawValue)"
        }
        // Try revision 2
        else if let embedding = NLEmbedding.sentenceEmbedding(for: .english, revision: 2) {
            loadedEmbedding = embedding
            loadedModel = "apple-ml-sentence-v2"
        }
        // Try revision 1
        else if let embedding = NLEmbedding.sentenceEmbedding(for: .english, revision: 1) {
            loadedEmbedding = embedding
            loadedModel = "apple-ml-sentence-v1"
        }
        // Try without revision (system default)
        else if let embedding = NLEmbedding.sentenceEmbedding(for: .english) {
            loadedEmbedding = embedding
            loadedModel = "apple-ml-sentence-default"
        }
        // Fallback to word embeddings (limited use - only single words)
        else {
            loadedEmbedding = NLEmbedding.wordEmbedding(for: .english)
            loadedModel = "apple-ml-word"
            print("⚠️  Sentence embeddings not available, using word embeddings (limited to single words)")
        }
        
        self.embedding = loadedEmbedding
        self.dimensions = loadedEmbedding?.dimension ?? 300
        self.model = loadedModel
        
        print("🧠 AppleMLEmbedder initialized: \(model) (\(dimensions) dimensions)")
    }
    
    // MARK: - Embedding Methods
    
    /// Generate embedding for a single text
    /// - Parameter text: Input text to embed
    /// - Returns: Embedding vector as array of floats
    /// - Throws: EmbeddingError if embedding fails
    func embed(text: String) throws -> [Float] {
        guard let embedding = embedding else {
            throw EmbeddingError.modelNotAvailable
        }
        
        guard !text.isEmpty else {
            throw EmbeddingError.emptyInput
        }
        
        // Get embedding vector
        guard let vector = embedding.vector(for: text) else {
            throw EmbeddingError.embeddingFailed
        }
        
        // Convert to Float array
        var result = [Float](repeating: 0.0, count: dimensions)
        for i in 0..<dimensions {
            result[i] = Float(vector[i])
        }
        
        return result
    }
    
    /// Generate embeddings for multiple texts (batch processing)
    /// - Parameter texts: Array of input texts
    /// - Returns: Array of embedding vectors
    /// - Throws: EmbeddingError if any embedding fails
    func embedBatch(texts: [String]) throws -> [[Float]] {
        var results: [[Float]] = []
        
        for text in texts {
            let embedding = try embed(text: text)
            results.append(embedding)
        }
        
        return results
    }
    
    /// Calculate cosine similarity between two embeddings
    /// - Parameters:
    ///   - a: First embedding vector
    ///   - b: Second embedding vector
    /// - Returns: Similarity score (0.0 to 1.0)
    func cosineSimilarity(_ a: [Float], _ b: [Float]) -> Float {
        guard a.count == b.count else { return 0.0 }
        
        var dotProduct: Float = 0.0
        var magnitudeA: Float = 0.0
        var magnitudeB: Float = 0.0
        
        for i in 0..<a.count {
            dotProduct += a[i] * b[i]
            magnitudeA += a[i] * a[i]
            magnitudeB += b[i] * b[i]
        }
        
        let magnitude = sqrt(magnitudeA) * sqrt(magnitudeB)
        guard magnitude > 0 else { return 0.0 }
        
        return dotProduct / magnitude
    }
    
    /// Find most similar text from a list
    /// - Parameters:
    ///   - query: Query text
    ///   - candidates: List of candidate texts
    ///   - topK: Number of top results to return
    /// - Returns: Array of (text, similarity) tuples, sorted by similarity
    func findSimilar(query: String, candidates: [String], topK: Int = 5) throws -> [(text: String, similarity: Float)] {
        let queryEmbedding = try embed(text: query)
        var results: [(text: String, similarity: Float)] = []
        
        for candidate in candidates {
            let candidateEmbedding = try embed(text: candidate)
            let similarity = cosineSimilarity(queryEmbedding, candidateEmbedding)
            results.append((text: candidate, similarity: similarity))
        }
        
        // Sort by similarity (descending) and take top K
        results.sort { $0.similarity > $1.similarity }
        return Array(results.prefix(topK))
    }
    
    // MARK: - Language Detection
    
    /// Detect language of input text
    /// - Parameter text: Input text
    /// - Returns: Detected language code (e.g., "en", "es", "fr")
    func detectLanguage(text: String) -> String? {
        let recognizer = NLLanguageRecognizer()
        recognizer.processString(text)
        return recognizer.dominantLanguage?.rawValue
    }
    
    /// Get embedding for detected language (if available)
    /// - Parameter text: Input text
    /// - Returns: Language-specific embedding or nil
    func embedWithLanguageDetection(text: String) throws -> [Float] {
        // Detect language
        if let languageCode = detectLanguage(text: text),
           let languageEmbedding = NLEmbedding.sentenceEmbedding(for: NLLanguage(rawValue: languageCode)) {
            
            // Use language-specific embedding
            guard let vector = languageEmbedding.vector(for: text) else {
                throw EmbeddingError.embeddingFailed
            }
            
            var result = [Float](repeating: 0.0, count: languageEmbedding.dimension)
            for i in 0..<languageEmbedding.dimension {
                result[i] = Float(vector[i])
            }
            
            return result
        }
        
        // Fallback to default embedding
        return try embed(text: text)
    }
}

// MARK: - Error Types

enum EmbeddingError: Error, LocalizedError {
    case modelNotAvailable
    case emptyInput
    case embeddingFailed
    case invalidDimensions
    
    var errorDescription: String? {
        switch self {
        case .modelNotAvailable:
            return "Apple ML embedding model is not available on this system"
        case .emptyInput:
            return "Input text cannot be empty"
        case .embeddingFailed:
            return "Failed to generate embedding for the provided text"
        case .invalidDimensions:
            return "Embedding dimensions do not match expected size"
        }
    }
}

// MARK: - Utility Extensions

extension AppleMLEmbedder {
    /// Estimate token count (approximate)
    /// - Parameter text: Input text
    /// - Returns: Approximate token count
    func estimateTokenCount(text: String) -> Int {
        // Rough approximation: ~4 characters per token
        return max(1, text.count / 4)
    }
    
    /// Check if embeddings are available on this system
    static func isAvailable() -> Bool {
        return NLEmbedding.sentenceEmbedding(for: .english) != nil ||
               NLEmbedding.wordEmbedding(for: .english) != nil
    }
    
    /// Get list of supported languages for sentence embeddings
    static func supportedLanguages() -> [NLLanguage] {
        // Check which languages have sentence embeddings available
        let candidates: [NLLanguage] = [
            .english, .german, .french, .spanish, .italian,
            .portuguese, .dutch, .russian, .simplifiedChinese, .japanese
        ]
        return candidates.filter { NLEmbedding.sentenceEmbedding(for: $0) != nil }
    }
}
