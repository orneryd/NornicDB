//go:build cgovulkan && (linux || windows || darwin)

package vulkan

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// TestSPIRVShaderFileStructure tests that the SPIR-V shader file has correct structure
func TestSPIRVShaderFileStructure(t *testing.T) {
	// Find the shader file relative to the test
	shaderPath := filepath.Join("shaders", "cosine_similarity.spv")
	
	// Try multiple possible paths
	possiblePaths := []string{
		shaderPath,
		filepath.Join("pkg", "gpu", "vulkan", shaderPath),
		filepath.Join("..", "..", "..", shaderPath),
	}
	
	var data []byte
	var err error
	for _, path := range possiblePaths {
		data, err = os.ReadFile(path)
		if err == nil {
			t.Logf("Found SPIR-V file at: %s", path)
			break
		}
	}
	
	if err != nil {
		t.Skipf("SPIR-V shader file not found (this is OK if shaders haven't been compiled): %v", err)
		return
	}
	
	// Verify minimum size (SPIR-V header is at least 5 words = 20 bytes)
	if len(data) < 20 {
		t.Fatalf("SPIR-V file too small: %d bytes, need at least 20", len(data))
	}
	
	// Verify SPIR-V magic number (first 4 bytes should be 0x07230203)
	magic := binary.LittleEndian.Uint32(data[0:4])
	expectedMagic := uint32(0x07230203)
	if magic != expectedMagic {
		t.Errorf("SPIR-V magic number = 0x%08x, want 0x%08x", magic, expectedMagic)
	}
	
	// Verify version (bytes 4-8 should be 0x00010000 for SPIR-V 1.0)
	version := binary.LittleEndian.Uint32(data[4:8])
	expectedVersion := uint32(0x00010000)
	if version != expectedVersion {
		t.Errorf("SPIR-V version = 0x%08x, want 0x%08x", version, expectedVersion)
	}
	
	// Verify file size matches expected (2380 bytes = 595 words)
	expectedSize := 2380
	if len(data) != expectedSize {
		t.Errorf("SPIR-V file size = %d bytes, want %d", len(data), expectedSize)
	}
	
	// Verify size is a multiple of 4 (SPIR-V words are 32-bit)
	if len(data)%4 != 0 {
		t.Errorf("SPIR-V file size = %d is not a multiple of 4", len(data))
	}
	
	// Verify bound field (word at index 3) is reasonable
	if len(data) >= 16 {
		bound := binary.LittleEndian.Uint32(data[12:16])
		if bound == 0 {
			t.Error("SPIR-V bound is zero (invalid)")
		}
		if bound > 1000 {
			t.Errorf("SPIR-V bound is suspiciously large: %d", bound)
		}
		t.Logf("SPIR-V bound: %d", bound)
	}
	
	// Check for OpEntryPoint instruction (0x0006000f) - should be present in compute shader
	foundEntryPoint := false
	for i := 0; i < len(data)-4; i += 4 {
		word := binary.LittleEndian.Uint32(data[i : i+4])
		if word == 0x0006000f { // OpEntryPoint
			foundEntryPoint = true
			t.Logf("Found OpEntryPoint at offset %d", i)
			break
		}
	}
	
	if !foundEntryPoint {
		t.Error("SPIR-V shader does not contain OpEntryPoint instruction")
	}
	
	// Check for OpExecutionMode instruction (0x00060010) - should be present for local size
	foundExecutionMode := false
	for i := 0; i < len(data)-4; i += 4 {
		word := binary.LittleEndian.Uint32(data[i : i+4])
		if word == 0x00060010 { // OpExecutionMode
			foundExecutionMode = true
			t.Logf("Found OpExecutionMode at offset %d", i)
			break
		}
	}
	
	if !foundExecutionMode {
		t.Error("SPIR-V shader does not contain OpExecutionMode instruction")
	}
}

// TestSPIRVShaderIntegrity tests that the SPIR-V shader array is properly formatted
func TestSPIRVShaderIntegrity(t *testing.T) {
	if len(cosine_similarity_spirv) == 0 {
		t.Fatal("cosine_similarity_spirv array is empty")
	}

	// Check that all words are non-zero (SPIR-V shouldn't have all-zero words except in specific cases)
	zeroCount := 0
	for i, word := range cosine_similarity_spirv {
		if word == 0 {
			zeroCount++
			// Some zero words are valid (like schema field at index 4)
			if i != 4 && i < 10 {
				t.Logf("Warning: zero word at index %d (may be valid)", i)
			}
		}
	}

	// SPIR-V should have very few zero words
	if zeroCount > len(cosine_similarity_spirv)/10 {
		t.Errorf("Too many zero words in SPIR-V: %d out of %d", zeroCount, len(cosine_similarity_spirv))
	}

	// Verify the array is properly sized (multiple of 4 bytes)
	if len(cosine_similarity_spirv)*4 != int(cosine_similarity_spirv_size) {
		t.Errorf("Size mismatch: array has %d words (%d bytes), size constant = %d bytes",
			len(cosine_similarity_spirv), len(cosine_similarity_spirv)*4, cosine_similarity_spirv_size)
	}
}

// TestSPIRVShaderBound tests that the bound field (index 3) is reasonable
func TestSPIRVShaderBound(t *testing.T) {
	if len(cosine_similarity_spirv) < 4 {
		t.Fatal("SPIR-V array too small")
	}

	bound := cosine_similarity_spirv[3]
	// Bound should be a reasonable value (typically 100-200 for a compute shader)
	if bound == 0 {
		t.Error("SPIR-V bound is zero (invalid)")
	}
	if bound > 1000 {
		t.Errorf("SPIR-V bound is suspiciously large: %d", bound)
	}

	t.Logf("SPIR-V bound: %d", bound)
}

// TestSPIRVShaderGenerator tests that the generator field (index 2) is set
func TestSPIRVShaderGenerator(t *testing.T) {
	if len(cosine_similarity_spirv) < 3 {
		t.Fatal("SPIR-V array too small")
	}

	generator := cosine_similarity_spirv[2]
	// Generator should be non-zero (identifies the tool that generated the SPIR-V)
	if generator == 0 {
		t.Error("SPIR-V generator is zero (invalid)")
	}

	t.Logf("SPIR-V generator: 0x%08x", generator)
}

// TestSPIRVShaderSizeConstant tests that the size constant is correctly defined
func TestSPIRVShaderSizeConstant(t *testing.T) {
	// Size should be exactly the array length * 4 bytes
	expectedSize := uint64(len(cosine_similarity_spirv) * 4)
	if cosine_similarity_spirv_size != expectedSize {
		t.Errorf("cosine_similarity_spirv_size = %d, want %d", cosine_similarity_spirv_size, expectedSize)
	}

	// Size should be non-zero
	if cosine_similarity_spirv_size == 0 {
		t.Error("cosine_similarity_spirv_size is zero")
	}

	// Size should be a multiple of 4 (SPIR-V words are 32-bit)
	if cosine_similarity_spirv_size%4 != 0 {
		t.Errorf("cosine_similarity_spirv_size = %d is not a multiple of 4", cosine_similarity_spirv_size)
	}

	t.Logf("SPIR-V shader size: %d bytes (%d words)", cosine_similarity_spirv_size, len(cosine_similarity_spirv))
}

// TestSPIRVShaderAlignment tests that the shader data is properly aligned
func TestSPIRVShaderAlignment(t *testing.T) {
	// SPIR-V should be word-aligned (4 bytes)
	if len(cosine_similarity_spirv)*4 != int(cosine_similarity_spirv_size) {
		t.Error("SPIR-V array is not properly aligned")
	}

	// Each word should be a valid 32-bit value (this is always true for uint32, but we verify the array isn't corrupted)
	for i, word := range cosine_similarity_spirv {
		// Check for obviously corrupted values (all 0xFF or all 0x00 except in known positions)
		if word == 0xFFFFFFFF && i > 10 {
			t.Errorf("Suspicious all-ones word at index %d", i)
		}
	}
}

// TestSPIRVShaderContent tests that the shader contains expected SPIR-V instructions
func TestSPIRVShaderContent(t *testing.T) {
	// SPIR-V should contain OpEntryPoint (0x0006000f) for compute shader
	foundEntryPoint := false
	for i := 0; i < len(cosine_similarity_spirv)-1; i++ {
		// OpEntryPoint is instruction 15 (0x0F) with word count 6
		if cosine_similarity_spirv[i] == 0x0006000f {
			foundEntryPoint = true
			t.Logf("Found OpEntryPoint at index %d", i)
			break
		}
	}

	if !foundEntryPoint {
		t.Error("SPIR-V shader does not contain OpEntryPoint instruction")
	}

	// SPIR-V should contain OpExecutionMode (0x00060010) for local size
	foundExecutionMode := false
	for i := 0; i < len(cosine_similarity_spirv)-1; i++ {
		// OpExecutionMode is instruction 16 (0x10) with word count 6
		if cosine_similarity_spirv[i] == 0x00060010 {
			foundExecutionMode = true
			t.Logf("Found OpExecutionMode at index %d", i)
			break
		}
	}

	if !foundExecutionMode {
		t.Error("SPIR-V shader does not contain OpExecutionMode instruction")
	}
}

// BenchmarkSPIRVShaderAccess benchmarks accessing the SPIR-V shader array
func BenchmarkSPIRVShaderAccess(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cosine_similarity_spirv[i%len(cosine_similarity_spirv)]
	}
}

// BenchmarkSPIRVShaderSize benchmarks computing the shader size
func BenchmarkSPIRVShaderSize(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = uint64(len(cosine_similarity_spirv) * 4)
	}
}

