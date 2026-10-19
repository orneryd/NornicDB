import XCTest
@testable import NornicDB

final class ProviderConfigurationTests: XCTestCase {
    private func roundTrip(_ root: [String: Any]) -> [String: Any] {
        let config = ConfigManager()
        config.loadProviderSettings(root)
        var result = root
        config.writeProviderSettings(&result)
        return result
    }

    func testExternalEmbeddingSettingsSurviveLoadAndSave() {
        let section: [String: Any] = ["enabled": true, "provider": "openai", "model": "text-embedding-3-small",
            "url": "https://api.openai.com", "api_key": "fixture-only", "dimensions": 1536]
        let result = roundTrip(["embedding": section, "embedding_worker": ["workers": 10], "custom": ["keep": "value"]])
        XCTAssertEqual(result["embedding"] as? NSDictionary, section as NSDictionary)
        XCTAssertEqual(result["embedding_worker"] as? NSDictionary, ["workers": 10] as NSDictionary)
        XCTAssertEqual(result["custom"] as? NSDictionary, ["keep": "value"] as NSDictionary)
    }

    func testExternalRerankingSettingsSurviveLoadAndSave() {
        let section: [String: Any] = ["enabled": true, "provider": "http", "model": "rerank-v3.5",
            "api_url": "https://api.cohere.com/v2/rerank", "api_key": "fixture-only"]
        XCTAssertEqual(roundTrip(["search_rerank": section])["search_rerank"] as? NSDictionary, section as NSDictionary)
    }

    func testDisabledExternalHeimdallRetainsItsConfiguration() {
        let section: [String: Any] = ["enabled": false, "provider": "openai", "model": "gpt-4.1-mini",
            "api_url": "https://api.openai.com/v1", "api_key": "fixture-only", "context_size": 12345]
        XCTAssertEqual(roundTrip(["heimdall": section])["heimdall"] as? NSDictionary, section as NSDictionary)
    }
}
