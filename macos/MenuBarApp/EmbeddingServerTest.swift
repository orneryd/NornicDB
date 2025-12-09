//
// EmbeddingServerTest.swift
// Standalone test for the Apple ML Embedding Server
//
// Compile: swiftc -o embedding-test -framework Foundation -framework NaturalLanguage -framework Network AppleMLEmbedder.swift EmbeddingServer.swift EmbeddingServerTest.swift
//

import Foundation

// Simple test runner for the embedding server
@MainActor
class EmbeddingServerTester {
    
    static func main() async {
        print("🧪 NornicDB Embedding Server Test")
        print("=" * 50)
        
        // Test 1: Check if embeddings are available
        print("\n📋 Test 1: Checking Apple ML availability...")
        let available = AppleMLEmbedder.isAvailable()
        print("   Apple ML Embeddings available: \(available)")
        
        if !available {
            print("❌ Apple ML embeddings not available on this system")
            print("   This requires macOS 11+ with NaturalLanguage framework")
            return
        }
        print("   ✅ PASSED")
        
        // Test 2: Create embedder
        print("\n📋 Test 2: Creating AppleMLEmbedder...")
        let embedder = AppleMLEmbedder()
        print("   Model: \(embedder.model)")
        print("   Dimensions: \(embedder.dimensions)")
        print("   ✅ PASSED")
        
        // Test 3: Generate single embedding
        print("\n📋 Test 3: Generating single embedding...")
        do {
            let text = "Hello, world! This is a test of Apple's on-device embeddings."
            let embedding = try embedder.embed(text: text)
            print("   Input: \"\(text)\"")
            print("   Output dimensions: \(embedding.count)")
            print("   First 5 values: \(embedding.prefix(5).map { String(format: "%.4f", $0) }.joined(separator: ", "))")
            print("   ✅ PASSED")
        } catch {
            print("   ❌ FAILED: \(error)")
            return
        }
        
        // Test 4: Generate batch embeddings
        print("\n📋 Test 4: Generating batch embeddings...")
        do {
            let texts = [
                "The quick brown fox jumps over the lazy dog.",
                "Machine learning is transforming technology.",
                "NornicDB is a high-performance graph database."
            ]
            let embeddings = try embedder.embedBatch(texts: texts)
            print("   Input count: \(texts.count)")
            print("   Output count: \(embeddings.count)")
            for (i, emb) in embeddings.enumerated() {
                print("   [\(i)] dims=\(emb.count), first value=\(String(format: "%.4f", emb[0]))")
            }
            print("   ✅ PASSED")
        } catch {
            print("   ❌ FAILED: \(error)")
            return
        }
        
        // Test 5: Cosine similarity
        print("\n📋 Test 5: Testing cosine similarity...")
        do {
            let similar1 = "The cat sat on the mat."
            let similar2 = "A cat was sitting on a mat."
            let different = "Quantum computing uses qubits."
            
            let emb1 = try embedder.embed(text: similar1)
            let emb2 = try embedder.embed(text: similar2)
            let emb3 = try embedder.embed(text: different)
            
            let simSimilar = embedder.cosineSimilarity(emb1, emb2)
            let simDifferent = embedder.cosineSimilarity(emb1, emb3)
            
            print("   Similar sentences similarity: \(String(format: "%.4f", simSimilar))")
            print("   Different sentences similarity: \(String(format: "%.4f", simDifferent))")
            print("   Similar > Different: \(simSimilar > simDifferent)")
            
            if simSimilar > simDifferent {
                print("   ✅ PASSED")
            } else {
                print("   ⚠️  WARNING: Similar sentences should have higher similarity")
            }
        } catch {
            print("   ❌ FAILED: \(error)")
            return
        }
        
        // Test 6: Start server and test HTTP endpoint
        print("\n📋 Test 6: Testing HTTP server...")
        let server = EmbeddingServer()
        
        do {
            try server.start()
            print("   Server started on port \(server.port)")
            
            // Wait for server to be ready
            try await Task.sleep(nanoseconds: 500_000_000) // 0.5 seconds
            
            // Test the health endpoint
            print("   Testing /health endpoint...")
            let healthURL = URL(string: "http://localhost:\(server.port)/health")!
            let (healthData, _) = try await URLSession.shared.data(from: healthURL)
            if let healthJSON = try? JSONSerialization.jsonObject(with: healthData) as? [String: Any] {
                print("   Health response: \(healthJSON["status"] ?? "unknown")")
            }
            
            // Test the embeddings endpoint
            print("   Testing /v1/embeddings endpoint...")
            let embeddingsURL = URL(string: "http://localhost:\(server.port)/v1/embeddings")!
            var request = URLRequest(url: embeddingsURL)
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            
            let requestBody: [String: Any] = [
                "input": "Test embedding via HTTP API",
                "model": "apple-ml-embeddings"
            ]
            request.httpBody = try JSONSerialization.data(withJSONObject: requestBody)
            
            let (data, response) = try await URLSession.shared.data(for: request)
            
            if let httpResponse = response as? HTTPURLResponse {
                print("   HTTP Status: \(httpResponse.statusCode)")
                
                if httpResponse.statusCode == 200 {
                    if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                       let dataArray = json["data"] as? [[String: Any]],
                       let firstEmbedding = dataArray.first,
                       let embedding = firstEmbedding["embedding"] as? [Double] {
                        print("   Embedding dimensions: \(embedding.count)")
                        print("   ✅ PASSED")
                    }
                } else {
                    print("   ❌ FAILED: Unexpected status code")
                }
            }
            
            server.stop()
            print("   Server stopped")
            
        } catch {
            print("   ❌ FAILED: \(error)")
            server.stop()
            return
        }
        
        print("\n" + "=" * 50)
        print("🎉 All tests completed!")
        print("\nTo use the embedding server:")
        print("  1. Start the NornicDB menu bar app")
        print("  2. Enable 'Embedding Server' in settings")
        print("  3. Use curl or any HTTP client:")
        print("")
        print("  curl http://localhost:11435/v1/embeddings \\")
        print("    -H 'Content-Type: application/json' \\")
        print("    -d '{\"input\": \"Your text here\", \"model\": \"apple-ml-embeddings\"}'")
    }
}

// String multiplication helper
extension String {
    static func * (left: String, right: Int) -> String {
        return String(repeating: left, count: right)
    }
}

// Main entry point
@main
struct EmbeddingServerTestMain {
    static func main() async {
        await EmbeddingServerTester.main()
    }
}
