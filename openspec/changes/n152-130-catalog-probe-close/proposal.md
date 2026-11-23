# Publish the catalog probe close event once

N152-130 is a test-only defect. The real fixture Boundary can call manager.Close after an explicit close, and f25CatalogReceiptProbe.Close closed its observation channel twice. The original exact regression fails on f77ec7706de099588ac232b32b6908860511d3da with a caught close-of-closed-channel panic; no-observer and accepted-publication controls pass.

Guard only the observation event with sync.Once, leaving every underlying Engine.Close call and production lifecycle owner unchanged. The first observed close still publishes before delegation. No new shared helper or production API is needed.

Duplicate review: inspected nor183CatalogReadingEngine.Close and nor183RemoteView.Close. Their once guards own engine/callback completion and cached errors, whereas this probe must guard only its notification. Reuse standard sync.Once without merging distinct fixture lifecycle contracts. Indexed checker evidence is recorded separately; unavailable ingestion is not a clean result.

Delegation assessment: the correction is a mechanical test-only event guard. Under delegate-deepseek-work's prohibition on manufacturing trivial delegation, the parent implements and verifies it directly. No worker job is claimed. Performance benchmarks are inapplicable because runtime production code is unchanged.
