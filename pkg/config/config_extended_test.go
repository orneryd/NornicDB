package config

import (
	"testing"
	"time"
)

func TestValidate_InvalidEncryptionProvider(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Database.EncryptionProvider = "bogus-provider"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Expected error for invalid encryption provider")
	}
}

func TestValidate_AzureKeyVaultRequirements(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionProvider = "azure-keyvault"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Expected error for azure-keyvault without vault name/key name")
	}
}

func TestValidate_GCPCloudKMSRequirements(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionProvider = "gcp-cloudkms"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Expected error for gcp-cloudkms without project/location/key_ring/key_name")
	}
}

func TestValidate_PasswordProviderRequiresPassword(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionProvider = "password"
	cfg.Database.EncryptionPassword = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Expected error for password provider without password")
	}
}

func TestValidate_NegativeMVCCRetentionMaxVersions(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Database.MVCCRetentionMaxVersions = -1
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Expected error for negative MVCC retention max versions")
	}
}

func TestValidate_NegativeMVCCRetentionTTL(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Database.MVCCRetentionTTL = -time.Minute
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Expected error for negative MVCC retention TTL")
	}
}

func TestParseMemoryLimitMB_Negative(t *testing.T) {
	_, err := ParseMemoryLimitMB("-100")
	if err == nil {
		t.Fatal("Expected error for negative value")
	}
}

func TestParseMemoryLimitMB_Empty(t *testing.T) {
	_, err := ParseMemoryLimitMB("")
	if err == nil {
		t.Fatal("Expected error for empty string")
	}
}

func TestParseMemoryLimitMB_NonNumeric(t *testing.T) {
	_, err := ParseMemoryLimitMB("500MB")
	if err == nil {
		t.Fatal("Expected error for non-numeric")
	}
}

func TestParseMemoryLimitMB_Zero(t *testing.T) {
	v, err := ParseMemoryLimitMB("0")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if v != 0 {
		t.Errorf("Expected 0, got %d", v)
	}
}

func TestParseMemoryLimitMB_Overflow(t *testing.T) {
	// math.MaxInt64 / (1024*1024) + 1 would overflow
	_, err := ParseMemoryLimitMB("9223372036855")
	if err == nil {
		t.Fatal("Expected error for overflow value")
	}
}

func TestParseMemoryLimitMB_Valid(t *testing.T) {
	v, err := ParseMemoryLimitMB("500")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if v != 500*1024*1024 {
		t.Errorf("Expected %d, got %d", 500*1024*1024, v)
	}
}
