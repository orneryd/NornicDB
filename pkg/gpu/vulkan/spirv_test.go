//go:build !cgovulkan

// Package vulkan provides tests for SPIR-V shader validation that don't require CGO compilation.
// These tests can run without Vulkan SDK headers installed.

package vulkan

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// TestSPIRVShaderFileStructure tests that the SPIR-V shader file has correct structure
// This test can run without Vulkan SDK or CGO compilation
func TestSPIRVShaderFileStructure(t *testing.T) {
	// Find the shader file relative to the test
	shaderPath := filepath.Join("shaders", "cosine_similarity.spv")
	
	// Try multiple possible paths
	possiblePaths := []string{
		shaderPath,
		filepath.Join("pkg", "gpu", "vulkan", shaderPath),
		filepath.Join("..", "..", "..", shaderPath),
		filepath.Join(".", "pkg", "gpu", "vulkan", shaderPath),
	}
	
	var data []byte
	var err error
	var foundPath string
	for _, path := range possiblePaths {
		data, err = os.ReadFile(path)
		if err == nil {
			foundPath = path
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
		t.Errorf("SPIR-V file size = %d bytes, want %d (file: %s)", len(data), expectedSize, foundPath)
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
	
	// Verify the shader contains expected compute shader features
	// Look for OpCapability Shader (0x00020011 0x00000001)
	foundShaderCapability := false
	for i := 0; i < len(data)-8; i += 4 {
		if i+8 <= len(data) {
			word1 := binary.LittleEndian.Uint32(data[i : i+4])
			word2 := binary.LittleEndian.Uint32(data[i+4 : i+8])
			if word1 == 0x00020011 && word2 == 0x00000001 { // OpCapability Shader
				foundShaderCapability = true
				t.Logf("Found OpCapability Shader at offset %d", i)
				break
			}
		}
	}
	
	if !foundShaderCapability {
		t.Error("SPIR-V shader does not contain OpCapability Shader instruction")
	}
}

// TestSPIRVShaderBytecodeMatches tests that the embedded C array matches the SPIR-V file
func TestSPIRVShaderBytecodeMatches(t *testing.T) {
	// This test verifies that the SPIR-V bytecode in vulkan_bridge.go matches the .spv file
	// Note: This is a structural test - we can't directly access the C array from Go,
	// but we can verify the file exists and has the correct structure
	
	shaderPath := filepath.Join("shaders", "cosine_similarity.spv")
	possiblePaths := []string{
		shaderPath,
		filepath.Join("pkg", "gpu", "vulkan", shaderPath),
		filepath.Join("..", "..", "..", shaderPath),
		filepath.Join(".", "pkg", "gpu", "vulkan", shaderPath),
	}
	
	var data []byte
	var err error
	for _, path := range possiblePaths {
		data, err = os.ReadFile(path)
		if err == nil {
			break
		}
	}
	
	if err != nil {
		t.Skipf("SPIR-V shader file not found: %v", err)
		return
	}
	
	// Verify the file matches the expected size from the C code comments
	expectedSize := 2380 // bytes (595 words * 4)
	if len(data) != expectedSize {
		t.Errorf("SPIR-V file size = %d bytes, want %d (embedded array should match)", len(data), expectedSize)
	}
	
	// Verify magic number matches what's in the C code
	magic := binary.LittleEndian.Uint32(data[0:4])
	expectedMagic := uint32(0x07230203)
	if magic != expectedMagic {
		t.Errorf("SPIR-V magic = 0x%08x, embedded array should start with 0x%08x", magic, expectedMagic)
	}
}

// TestSPIRVShaderIntegrity performs basic integrity checks on the SPIR-V file
func TestSPIRVShaderIntegrity(t *testing.T) {
	shaderPath := filepath.Join("shaders", "cosine_similarity.spv")
	possiblePaths := []string{
		shaderPath,
		filepath.Join("pkg", "gpu", "vulkan", shaderPath),
		filepath.Join("..", "..", "..", shaderPath),
		filepath.Join(".", "pkg", "gpu", "vulkan", shaderPath),
	}
	
	var data []byte
	var err error
	for _, path := range possiblePaths {
		data, err = os.ReadFile(path)
		if err == nil {
			break
		}
	}
	
	if err != nil {
		t.Skipf("SPIR-V shader file not found: %v", err)
		return
	}
	
	// Check that file is not all zeros or all ones (corruption indicators)
	allZero := true
	allOnes := true
	for _, b := range data {
		if b != 0 {
			allZero = false
		}
		if b != 0xFF {
			allOnes = false
		}
		if !allZero && !allOnes {
			break
		}
	}
	
	if allZero {
		t.Error("SPIR-V file appears to be all zeros (corrupted?)")
	}
	if allOnes {
		t.Error("SPIR-V file appears to be all ones (corrupted?)")
	}
	
	// Verify word alignment
	if len(data)%4 != 0 {
		t.Errorf("SPIR-V file size %d is not word-aligned (must be multiple of 4)", len(data))
	}
	
	// Count zero words (some are valid, but too many indicates corruption)
	zeroWordCount := 0
	for i := 0; i < len(data); i += 4 {
		if i+4 <= len(data) {
			word := binary.LittleEndian.Uint32(data[i : i+4])
			if word == 0 {
				zeroWordCount++
			}
		}
	}
	
	// SPIR-V should have very few zero words (schema field at index 4 is valid)
	if zeroWordCount > len(data)/40 { // More than 2.5% zero words is suspicious
		t.Logf("Warning: SPIR-V has %d zero words out of %d total words", zeroWordCount, len(data)/4)
	}
}

