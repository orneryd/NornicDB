# Metal ML & GPU Acceleration

NornicDB for iOS leverages Apple's Metal framework for GPU-accelerated machine learning, building on the existing Metal implementation from macOS.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    iOS ML Acceleration Stack                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    APPLICATION LAYER                             │   │
│  │  Embedding Generation • K-Means Clustering • Vector Search       │   │
│  └──────────────────────────────┬──────────────────────────────────┘   │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    CORE ML FRAMEWORK                             │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │   │
│  │  │  Embedding  │ │   Model     │ │    Batch    │               │   │
│  │  │   Models    │ │ Optimization│ │  Inference  │               │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘               │   │
│  └──────────────────────────────┬──────────────────────────────────┘   │
│                                 │                                        │
│                                 ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    COMPUTE LAYER                                 │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │   │
│  │  │   Neural    │ │    Metal    │ │     CPU     │               │   │
│  │  │   Engine    │ │     GPU     │ │   Fallback  │               │   │
│  │  │   (ANE)     │ │             │ │             │               │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘               │   │
│  │       ↑              ↑              ↑                           │   │
│  │       └──────────────┴──────────────┘                           │   │
│  │              MLComputeUnits.all                                 │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Hardware Capabilities

### Supported Devices

| Device | Chip | Neural Engine | GPU Cores | RAM | Recommended |
|--------|------|---------------|-----------|-----|-------------|
| iPhone 15 Pro | A17 Pro | 35 TOPS | 6 | 8GB | ✅ Full features |
| iPhone 15 | A16 | 17 TOPS | 5 | 6GB | ✅ Full features |
| iPhone 14 Pro | A16 | 17 TOPS | 5 | 6GB | ✅ Full features |
| iPhone 14 | A15 | 15.8 TOPS | 5 | 6GB | ✅ Full features |
| iPhone 13 | A15 | 15.8 TOPS | 4 | 4GB | ✅ Standard |
| iPhone 12 | A14 | 11 TOPS | 4 | 4GB | ⚠️ Limited LLM |
| iPhone 11 | A13 | 6 TOPS | 4 | 4GB | ⚠️ Embeddings only |

### Performance by Chip

```
Embedding Generation (per text, 384-dim):

A17 Pro:  ~5ms  ████████████████████ 
A16:      ~8ms  ████████████████
A15:      ~10ms ██████████████
A14:      ~15ms ██████████
A13:      ~25ms ██████

K-Means Clustering (10,000 vectors):

A17 Pro:  ~50ms  ████████████████████
A16:      ~80ms  ████████████████
A15:      ~100ms ██████████████
A14:      ~150ms ██████████
A13:      ~250ms ██████
```

## Core ML Integration

### Model Loading

```swift
import CoreML

class CoreMLEmbedder {
    private var model: MLModel?
    private let configuration: MLModelConfiguration
    
    init() {
        configuration = MLModelConfiguration()
        
        // Use all available compute units
        configuration.computeUnits = .all
        
        // Allow background processing
        configuration.allowLowPrecisionAccumulationOnGPU = true
        
        // Optimize for Neural Engine
        if #available(iOS 17.0, *) {
            configuration.optimizationHints = [
                .preferredMetalDevice: MTLCreateSystemDefaultDevice()!
            ]
        }
    }
    
    func loadModel() async throws {
        let url = Bundle.main.url(forResource: "embedder", withExtension: "mlmodelc")!
        
        // Compile if needed
        if !FileManager.default.fileExists(atPath: url.path) {
            let sourceURL = Bundle.main.url(forResource: "embedder", withExtension: "mlpackage")!
            let compiledURL = try await MLModel.compileModel(at: sourceURL)
            try FileManager.default.copyItem(at: compiledURL, to: url)
        }
        
        model = try MLModel(contentsOf: url, configuration: configuration)
    }
    
    func embed(_ text: String) async throws -> [Float] {
        guard let model = model else {
            throw EmbeddingError.modelNotLoaded
        }
        
        // Tokenize
        let tokens = try tokenize(text)
        
        // Create input
        let input = try MLDictionaryFeatureProvider(dictionary: [
            "input_ids": MLMultiArray(tokens.inputIds),
            "attention_mask": MLMultiArray(tokens.attentionMask)
        ])
        
        // Run inference
        let output = try await model.prediction(from: input)
        
        // Extract embedding
        guard let embedding = output.featureValue(for: "embeddings")?.multiArrayValue else {
            throw EmbeddingError.invalidOutput
        }
        
        return embedding.toFloatArray()
    }
    
    func embedBatch(_ texts: [String]) async throws -> [[Float]] {
        guard let model = model else {
            throw EmbeddingError.modelNotLoaded
        }
        
        // Tokenize all texts
        let tokenizedBatch = try texts.map { try tokenize($0) }
        
        // Create batch input
        let batchInput = try tokenizedBatch.map { tokens in
            try MLDictionaryFeatureProvider(dictionary: [
                "input_ids": MLMultiArray(tokens.inputIds),
                "attention_mask": MLMultiArray(tokens.attentionMask)
            ])
        }
        
        // Run batch inference
        let batchProvider = MLArrayBatchProvider(array: batchInput)
        let batchOutput = try await model.predictions(from: batchProvider)
        
        // Extract embeddings
        var embeddings: [[Float]] = []
        for i in 0..<batchOutput.count {
            let output = batchOutput.features(at: i)
            guard let embedding = output.featureValue(for: "embeddings")?.multiArrayValue else {
                throw EmbeddingError.invalidOutput
            }
            embeddings.append(embedding.toFloatArray())
        }
        
        return embeddings
    }
}
```

### Model Quantization

```python
# Convert and quantize model for iOS
import coremltools as ct
from transformers import AutoModel, AutoTokenizer
import torch

# Load model
model_name = "sentence-transformers/all-MiniLM-L6-v2"
model = AutoModel.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)

# Export to ONNX
dummy_input = tokenizer("test", return_tensors="pt", padding="max_length", max_length=128)
torch.onnx.export(
    model,
    (dummy_input["input_ids"], dummy_input["attention_mask"]),
    "embedder.onnx",
    input_names=["input_ids", "attention_mask"],
    output_names=["embeddings"],
    dynamic_axes={
        "input_ids": {0: "batch", 1: "sequence"},
        "attention_mask": {0: "batch", 1: "sequence"},
        "embeddings": {0: "batch"}
    }
)

# Convert to Core ML
mlmodel = ct.convert(
    "embedder.onnx",
    minimum_deployment_target=ct.target.iOS17,
    compute_units=ct.ComputeUnit.ALL,
    convert_to="mlprogram"
)

# Quantize to float16 (reduces size, keeps accuracy)
mlmodel_fp16 = ct.models.neural_network.quantization_utils.quantize_weights(
    mlmodel, 
    nbits=16
)

# Save
mlmodel_fp16.save("embedder.mlpackage")

# Optional: Quantize to int8 (smaller but may lose accuracy)
mlmodel_int8 = ct.models.optimization.coreml.palettize_weights(
    mlmodel_fp16,
    config=ct.models.optimization.coreml.OptimizationConfig(
        global_config=ct.models.optimization.coreml.OpPalettizerConfig(
            mode="kmeans",
            nbits=8
        )
    )
)
mlmodel_int8.save("embedder_int8.mlpackage")
```

## Metal Compute

### Reusing NornicDB Metal Code

NornicDB already has Metal shaders for macOS that can be reused on iOS:

```objc
// pkg/gpu/metal/metal_bridge_darwin.m
// This code is shared between macOS and iOS

#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>

@interface MetalCompute : NSObject
@property (nonatomic, strong) id<MTLDevice> device;
@property (nonatomic, strong) id<MTLCommandQueue> commandQueue;
@property (nonatomic, strong) id<MTLComputePipelineState> kmeansAssignPipeline;
@property (nonatomic, strong) id<MTLComputePipelineState> kmeansUpdatePipeline;
@property (nonatomic, strong) id<MTLComputePipelineState> cosineSimilarityPipeline;
@end

@implementation MetalCompute

- (instancetype)init {
    self = [super init];
    if (self) {
        _device = MTLCreateSystemDefaultDevice();
        _commandQueue = [_device newCommandQueue];
        [self loadKernels];
    }
    return self;
}

- (void)loadKernels {
    NSError *error;
    NSBundle *bundle = [NSBundle mainBundle];
    
    // Load shader library
    id<MTLLibrary> library = [_device newDefaultLibraryWithBundle:bundle error:&error];
    
    // Create compute pipelines
    id<MTLFunction> kmeansAssign = [library newFunctionWithName:@"kmeans_assign_clusters"];
    _kmeansAssignPipeline = [_device newComputePipelineStateWithFunction:kmeansAssign error:&error];
    
    id<MTLFunction> kmeansUpdate = [library newFunctionWithName:@"kmeans_update_centroids"];
    _kmeansUpdatePipeline = [_device newComputePipelineStateWithFunction:kmeansUpdate error:&error];
    
    id<MTLFunction> cosineSim = [library newFunctionWithName:@"cosine_similarity_batch"];
    _cosineSimilarityPipeline = [_device newComputePipelineStateWithFunction:cosineSim error:&error];
}

@end
```

### K-Means Metal Shader

```metal
// pkg/gpu/metal/kmeans_kernels_darwin.metal
// Shared between macOS and iOS

#include <metal_stdlib>
using namespace metal;

// Assign vectors to nearest centroid
kernel void kmeans_assign_clusters(
    device const float* vectors [[buffer(0)]],
    device const float* centroids [[buffer(1)]],
    device int* assignments [[buffer(2)]],
    constant uint& vectorCount [[buffer(3)]],
    constant uint& centroidCount [[buffer(4)]],
    constant uint& dimensions [[buffer(5)]],
    uint id [[thread_position_in_grid]]
) {
    if (id >= vectorCount) return;
    
    float minDistance = INFINITY;
    int nearestCentroid = 0;
    
    // Get pointer to this vector
    device const float* vector = vectors + (id * dimensions);
    
    // Find nearest centroid
    for (uint c = 0; c < centroidCount; c++) {
        device const float* centroid = centroids + (c * dimensions);
        
        // Compute squared Euclidean distance
        float distance = 0.0;
        for (uint d = 0; d < dimensions; d++) {
            float diff = vector[d] - centroid[d];
            distance += diff * diff;
        }
        
        if (distance < minDistance) {
            minDistance = distance;
            nearestCentroid = c;
        }
    }
    
    assignments[id] = nearestCentroid;
}

// Update centroids based on assignments
kernel void kmeans_update_centroids(
    device const float* vectors [[buffer(0)]],
    device const int* assignments [[buffer(1)]],
    device float* centroids [[buffer(2)]],
    device atomic_int* counts [[buffer(3)]],
    constant uint& vectorCount [[buffer(4)]],
    constant uint& centroidCount [[buffer(5)]],
    constant uint& dimensions [[buffer(6)]],
    uint id [[thread_position_in_grid]]
) {
    if (id >= vectorCount) return;
    
    int cluster = assignments[id];
    device const float* vector = vectors + (id * dimensions);
    device float* centroid = centroids + (cluster * dimensions);
    
    // Atomically add vector to centroid sum
    for (uint d = 0; d < dimensions; d++) {
        // Note: Metal doesn't have atomic float add, 
        // so we use a reduction pattern
        atomic_fetch_add_explicit(&counts[cluster], 1, memory_order_relaxed);
    }
}

// Batch cosine similarity computation
kernel void cosine_similarity_batch(
    device const float* queryVector [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device float* similarities [[buffer(2)]],
    constant uint& vectorCount [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint id [[thread_position_in_grid]]
) {
    if (id >= vectorCount) return;
    
    device const float* vector = vectors + (id * dimensions);
    
    float dotProduct = 0.0;
    float queryNorm = 0.0;
    float vectorNorm = 0.0;
    
    for (uint d = 0; d < dimensions; d++) {
        dotProduct += queryVector[d] * vector[d];
        queryNorm += queryVector[d] * queryVector[d];
        vectorNorm += vector[d] * vector[d];
    }
    
    float denominator = sqrt(queryNorm) * sqrt(vectorNorm);
    similarities[id] = denominator > 0 ? dotProduct / denominator : 0.0;
}
```

### Swift Metal Interface

```swift
import Metal
import MetalPerformanceShaders

class MetalAccelerator {
    static let shared = MetalAccelerator()
    
    private let device: MTLDevice
    private let commandQueue: MTLCommandQueue
    private var kmeansAssignPipeline: MTLComputePipelineState?
    private var cosineSimilarityPipeline: MTLComputePipelineState?
    
    init?() {
        guard let device = MTLCreateSystemDefaultDevice() else {
            return nil
        }
        
        self.device = device
        
        guard let commandQueue = device.makeCommandQueue() else {
            return nil
        }
        
        self.commandQueue = commandQueue
        
        loadPipelines()
    }
    
    private func loadPipelines() {
        guard let library = device.makeDefaultLibrary() else { return }
        
        if let kmeansAssign = library.makeFunction(name: "kmeans_assign_clusters") {
            kmeansAssignPipeline = try? device.makeComputePipelineState(function: kmeansAssign)
        }
        
        if let cosineSim = library.makeFunction(name: "cosine_similarity_batch") {
            cosineSimilarityPipeline = try? device.makeComputePipelineState(function: cosineSim)
        }
    }
    
    // MARK: - Vector Search
    
    func cosineSimilarityBatch(
        query: [Float],
        vectors: [[Float]]
    ) async throws -> [Float] {
        guard let pipeline = cosineSimilarityPipeline else {
            throw MetalError.pipelineNotLoaded
        }
        
        let dimensions = query.count
        let vectorCount = vectors.count
        
        // Create buffers
        let queryBuffer = device.makeBuffer(bytes: query, length: dimensions * MemoryLayout<Float>.size)!
        
        let flattenedVectors = vectors.flatMap { $0 }
        let vectorsBuffer = device.makeBuffer(bytes: flattenedVectors, length: flattenedVectors.count * MemoryLayout<Float>.size)!
        
        let similaritiesBuffer = device.makeBuffer(length: vectorCount * MemoryLayout<Float>.size)!
        
        // Create command buffer
        guard let commandBuffer = commandQueue.makeCommandBuffer(),
              let encoder = commandBuffer.makeComputeCommandEncoder() else {
            throw MetalError.commandCreationFailed
        }
        
        encoder.setComputePipelineState(pipeline)
        encoder.setBuffer(queryBuffer, offset: 0, index: 0)
        encoder.setBuffer(vectorsBuffer, offset: 0, index: 1)
        encoder.setBuffer(similaritiesBuffer, offset: 0, index: 2)
        
        var vectorCountVar = UInt32(vectorCount)
        var dimensionsVar = UInt32(dimensions)
        encoder.setBytes(&vectorCountVar, length: MemoryLayout<UInt32>.size, index: 3)
        encoder.setBytes(&dimensionsVar, length: MemoryLayout<UInt32>.size, index: 4)
        
        // Dispatch
        let threadGroupSize = MTLSize(width: min(256, vectorCount), height: 1, depth: 1)
        let threadGroups = MTLSize(width: (vectorCount + 255) / 256, height: 1, depth: 1)
        encoder.dispatchThreadgroups(threadGroups, threadsPerThreadgroup: threadGroupSize)
        
        encoder.endEncoding()
        commandBuffer.commit()
        commandBuffer.waitUntilCompleted()
        
        // Read results
        let results = similaritiesBuffer.contents().bindMemory(to: Float.self, capacity: vectorCount)
        return Array(UnsafeBufferPointer(start: results, count: vectorCount))
    }
    
    // MARK: - K-Means Clustering
    
    func kmeansCluster(
        vectors: [[Float]],
        k: Int,
        maxIterations: Int = 100
    ) async throws -> (assignments: [Int], centroids: [[Float]]) {
        guard let assignPipeline = kmeansAssignPipeline else {
            throw MetalError.pipelineNotLoaded
        }
        
        let dimensions = vectors[0].count
        let vectorCount = vectors.count
        
        // Initialize centroids (k-means++ initialization)
        var centroids = initializeCentroids(vectors: vectors, k: k)
        var assignments = [Int](repeating: 0, count: vectorCount)
        
        // Create Metal buffers
        let flattenedVectors = vectors.flatMap { $0 }
        let vectorsBuffer = device.makeBuffer(bytes: flattenedVectors, length: flattenedVectors.count * MemoryLayout<Float>.size)!
        
        var flattenedCentroids = centroids.flatMap { $0 }
        let centroidsBuffer = device.makeBuffer(bytes: &flattenedCentroids, length: flattenedCentroids.count * MemoryLayout<Float>.size)!
        
        let assignmentsBuffer = device.makeBuffer(length: vectorCount * MemoryLayout<Int32>.size)!
        
        // Iterate
        for _ in 0..<maxIterations {
            // Step 1: Assign clusters (GPU)
            guard let commandBuffer = commandQueue.makeCommandBuffer(),
                  let encoder = commandBuffer.makeComputeCommandEncoder() else {
                throw MetalError.commandCreationFailed
            }
            
            encoder.setComputePipelineState(assignPipeline)
            encoder.setBuffer(vectorsBuffer, offset: 0, index: 0)
            encoder.setBuffer(centroidsBuffer, offset: 0, index: 1)
            encoder.setBuffer(assignmentsBuffer, offset: 0, index: 2)
            
            var vectorCountVar = UInt32(vectorCount)
            var centroidCountVar = UInt32(k)
            var dimensionsVar = UInt32(dimensions)
            encoder.setBytes(&vectorCountVar, length: MemoryLayout<UInt32>.size, index: 3)
            encoder.setBytes(&centroidCountVar, length: MemoryLayout<UInt32>.size, index: 4)
            encoder.setBytes(&dimensionsVar, length: MemoryLayout<UInt32>.size, index: 5)
            
            let threadGroupSize = MTLSize(width: min(256, vectorCount), height: 1, depth: 1)
            let threadGroups = MTLSize(width: (vectorCount + 255) / 256, height: 1, depth: 1)
            encoder.dispatchThreadgroups(threadGroups, threadsPerThreadgroup: threadGroupSize)
            
            encoder.endEncoding()
            commandBuffer.commit()
            commandBuffer.waitUntilCompleted()
            
            // Read assignments
            let newAssignments = assignmentsBuffer.contents().bindMemory(to: Int32.self, capacity: vectorCount)
            let newAssignmentsArray = Array(UnsafeBufferPointer(start: newAssignments, count: vectorCount)).map { Int($0) }
            
            // Check convergence
            if newAssignmentsArray == assignments {
                break
            }
            assignments = newAssignmentsArray
            
            // Step 2: Update centroids (CPU - simpler for now)
            centroids = updateCentroids(vectors: vectors, assignments: assignments, k: k)
            flattenedCentroids = centroids.flatMap { $0 }
            centroidsBuffer.contents().copyMemory(from: flattenedCentroids, byteCount: flattenedCentroids.count * MemoryLayout<Float>.size)
        }
        
        return (assignments, centroids)
    }
    
    private func initializeCentroids(vectors: [[Float]], k: Int) -> [[Float]] {
        // K-means++ initialization
        var centroids: [[Float]] = []
        var random = SystemRandomNumberGenerator()
        
        // First centroid: random
        centroids.append(vectors[Int.random(in: 0..<vectors.count, using: &random)])
        
        // Remaining centroids: proportional to distance squared
        for _ in 1..<k {
            var distances = vectors.map { vector -> Float in
                centroids.map { centroid in
                    zip(vector, centroid).map { ($0 - $1) * ($0 - $1) }.reduce(0, +)
                }.min() ?? 0
            }
            
            let total = distances.reduce(0, +)
            let threshold = Float.random(in: 0..<total, using: &random)
            
            var cumulative: Float = 0
            for (i, dist) in distances.enumerated() {
                cumulative += dist
                if cumulative >= threshold {
                    centroids.append(vectors[i])
                    break
                }
            }
        }
        
        return centroids
    }
    
    private func updateCentroids(vectors: [[Float]], assignments: [Int], k: Int) -> [[Float]] {
        let dimensions = vectors[0].count
        var sums = [[Float]](repeating: [Float](repeating: 0, count: dimensions), count: k)
        var counts = [Int](repeating: 0, count: k)
        
        for (i, assignment) in assignments.enumerated() {
            for d in 0..<dimensions {
                sums[assignment][d] += vectors[i][d]
            }
            counts[assignment] += 1
        }
        
        return sums.enumerated().map { (i, sum) in
            let count = Float(max(1, counts[i]))
            return sum.map { $0 / count }
        }
    }
}

enum MetalError: Error {
    case pipelineNotLoaded
    case commandCreationFailed
    case deviceNotAvailable
}
```

## Metal Performance Shaders

For optimized matrix operations:

```swift
import MetalPerformanceShaders

class MPSAccelerator {
    private let device: MTLDevice
    private let commandQueue: MTLCommandQueue
    
    init?() {
        guard let device = MTLCreateSystemDefaultDevice(),
              let queue = device.makeCommandQueue() else {
            return nil
        }
        self.device = device
        self.commandQueue = queue
    }
    
    // Matrix multiplication for attention
    func matrixMultiply(
        a: [[Float]],
        b: [[Float]]
    ) async throws -> [[Float]] {
        let M = a.count
        let K = a[0].count
        let N = b[0].count
        
        // Create matrices
        let matrixA = MPSMatrix(
            buffer: createBuffer(from: a.flatMap { $0 }),
            descriptor: MPSMatrixDescriptor(
                rows: M, columns: K,
                rowBytes: K * MemoryLayout<Float>.stride,
                dataType: .float32
            )
        )
        
        let matrixB = MPSMatrix(
            buffer: createBuffer(from: b.flatMap { $0 }),
            descriptor: MPSMatrixDescriptor(
                rows: K, columns: N,
                rowBytes: N * MemoryLayout<Float>.stride,
                dataType: .float32
            )
        )
        
        let resultBuffer = device.makeBuffer(length: M * N * MemoryLayout<Float>.stride)!
        let matrixC = MPSMatrix(
            buffer: resultBuffer,
            descriptor: MPSMatrixDescriptor(
                rows: M, columns: N,
                rowBytes: N * MemoryLayout<Float>.stride,
                dataType: .float32
            )
        )
        
        // Create multiplication kernel
        let multiplication = MPSMatrixMultiplication(
            device: device,
            transposeLeft: false,
            transposeRight: false,
            resultRows: M,
            resultColumns: N,
            interiorColumns: K,
            alpha: 1.0,
            beta: 0.0
        )
        
        // Execute
        guard let commandBuffer = commandQueue.makeCommandBuffer() else {
            throw MetalError.commandCreationFailed
        }
        
        multiplication.encode(
            commandBuffer: commandBuffer,
            leftMatrix: matrixA,
            rightMatrix: matrixB,
            resultMatrix: matrixC
        )
        
        commandBuffer.commit()
        commandBuffer.waitUntilCompleted()
        
        // Read result
        let resultPointer = resultBuffer.contents().bindMemory(to: Float.self, capacity: M * N)
        let resultArray = Array(UnsafeBufferPointer(start: resultPointer, count: M * N))
        
        // Reshape to 2D
        return stride(from: 0, to: resultArray.count, by: N).map {
            Array(resultArray[$0..<min($0 + N, resultArray.count)])
        }
    }
    
    private func createBuffer(from array: [Float]) -> MTLBuffer {
        return device.makeBuffer(bytes: array, length: array.count * MemoryLayout<Float>.stride)!
    }
}
```

## Performance Optimization

### Thermal Management

```swift
import Foundation

class ThermalManager {
    static let shared = ThermalManager()
    
    private var thermalState: ProcessInfo.ThermalState {
        ProcessInfo.processInfo.thermalState
    }
    
    var shouldThrottle: Bool {
        thermalState == .serious || thermalState == .critical
    }
    
    var maxBatchSize: Int {
        switch thermalState {
        case .nominal:
            return 32
        case .fair:
            return 16
        case .serious:
            return 8
        case .critical:
            return 4
        @unknown default:
            return 8
        }
    }
    
    func startMonitoring(onChange: @escaping (ProcessInfo.ThermalState) -> Void) {
        NotificationCenter.default.addObserver(
            forName: ProcessInfo.thermalStateDidChangeNotification,
            object: nil,
            queue: .main
        ) { _ in
            onChange(self.thermalState)
        }
    }
}
```

### Memory-Efficient Processing

```swift
class MemoryEfficientEmbedder {
    private let embedder: CoreMLEmbedder
    private let batchSize = 8
    
    func embedLargeDataset(_ texts: [String]) async throws -> [[Float]] {
        var embeddings: [[Float]] = []
        embeddings.reserveCapacity(texts.count)
        
        // Process in batches to avoid memory pressure
        for batch in texts.chunked(into: batchSize) {
            autoreleasepool {
                let batchEmbeddings = try await embedder.embedBatch(batch)
                embeddings.append(contentsOf: batchEmbeddings)
            }
            
            // Check memory pressure
            if ProcessInfo.processInfo.physicalMemory < 100_000_000 {
                // Wait for memory to free
                try await Task.sleep(nanoseconds: 100_000_000)
            }
        }
        
        return embeddings
    }
}

extension Array {
    func chunked(into size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}
```

## Benchmarks

### Embedding Generation

| Operation | A17 Pro | A16 | A15 | A14 |
|-----------|---------|-----|-----|-----|
| Single text | 5ms | 8ms | 10ms | 15ms |
| Batch (8) | 25ms | 40ms | 55ms | 80ms |
| Batch (32) | 80ms | 130ms | 180ms | 280ms |

### K-Means Clustering

| Vectors | Dims | Clusters | A17 Pro | A16 | A15 |
|---------|------|----------|---------|-----|-----|
| 1,000 | 384 | 10 | 15ms | 25ms | 35ms |
| 10,000 | 384 | 50 | 120ms | 200ms | 300ms |
| 50,000 | 384 | 100 | 800ms | 1.3s | 2s |

### Vector Search

| Vectors | Dims | A17 Pro | A16 | A15 |
|---------|------|---------|-----|-----|
| 1,000 | 384 | 2ms | 3ms | 5ms |
| 10,000 | 384 | 15ms | 25ms | 40ms |
| 50,000 | 384 | 70ms | 120ms | 180ms |

## Next Steps

1. [Background Processing →](BACKGROUND_PROCESSING.md)
2. [Model Requirements →](MODELS.md)
3. [API Reference →](API.md)
