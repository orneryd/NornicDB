// Package encryption tests for data-at-rest encryption.
package encryption

import (
	"bytes"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"
)

func TestGenerateKey(t *testing.T) {
	key1, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	if len(key1) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(key1))
	}

	key2, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	// Keys should be unique
	if bytes.Equal(key1, key2) {
		t.Error("generated keys should be unique")
	}
}

func TestGenerateSalt(t *testing.T) {
	salt, err := GenerateSalt()
	if err != nil {
		t.Fatalf("GenerateSalt() error = %v", err)
	}
	if len(salt) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(salt))
	}
}

func TestDeriveKey(t *testing.T) {
	password := []byte("test-password")
	salt := []byte("test-salt-12345678901234")

	key1 := DeriveKey(password, salt, 1000)
	if len(key1) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(key1))
	}

	// Same inputs should produce same output
	key2 := DeriveKey(password, salt, 1000)
	if !bytes.Equal(key1, key2) {
		t.Error("same inputs should produce same key")
	}

	// Different password should produce different key
	key3 := DeriveKey([]byte("different"), salt, 1000)
	if bytes.Equal(key1, key3) {
		t.Error("different passwords should produce different keys")
	}

	// Default iterations
	key4 := DeriveKey(password, salt, 0)
	if len(key4) != 32 {
		t.Error("default iterations should work")
	}
}

func TestKeyValidate(t *testing.T) {
	t.Run("valid key", func(t *testing.T) {
		key, _ := GenerateKey()
		k := &Key{
			ID:        1,
			Material:  key,
			CreatedAt: time.Now(),
			Active:    true,
		}
		if err := k.Validate(); err != nil {
			t.Errorf("valid key should not error: %v", err)
		}
	})

	t.Run("invalid key length", func(t *testing.T) {
		k := &Key{
			ID:       1,
			Material: []byte("too-short"),
			Active:   true,
		}
		if err := k.Validate(); err != ErrInvalidKey {
			t.Errorf("expected ErrInvalidKey, got %v", err)
		}
	})

	t.Run("expired key", func(t *testing.T) {
		key, _ := GenerateKey()
		k := &Key{
			ID:        1,
			Material:  key,
			ExpiresAt: time.Now().Add(-time.Hour),
			Active:    true,
		}
		if err := k.Validate(); err != ErrKeyExpired {
			t.Errorf("expected ErrKeyExpired, got %v", err)
		}
	})

	t.Run("non-expiring key", func(t *testing.T) {
		key, _ := GenerateKey()
		k := &Key{
			ID:       1,
			Material: key,
			Active:   true,
			// ExpiresAt zero means never expires
		}
		if k.IsExpired() {
			t.Error("key with zero expiry should not be expired")
		}
	})
}

func TestKeyManager(t *testing.T) {
	config := DefaultConfig()
	km := NewKeyManager(config)

	t.Run("add and get key", func(t *testing.T) {
		material, _ := GenerateKey()
		key := &Key{
			ID:        1,
			Material:  material,
			CreatedAt: time.Now(),
			Active:    true,
		}

		if err := km.AddKey(key); err != nil {
			t.Fatalf("AddKey() error = %v", err)
		}

		got, err := km.GetKey(1)
		if err != nil {
			t.Fatalf("GetKey() error = %v", err)
		}
		if got.ID != 1 {
			t.Errorf("expected ID 1, got %d", got.ID)
		}
	})

	t.Run("current key", func(t *testing.T) {
		current, err := km.CurrentKey()
		if err != nil {
			t.Fatalf("CurrentKey() error = %v", err)
		}
		if current.ID != 1 {
			t.Errorf("expected current key ID 1, got %d", current.ID)
		}
	})

	t.Run("get nonexistent key", func(t *testing.T) {
		_, err := km.GetKey(999)
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("add invalid key", func(t *testing.T) {
		key := &Key{
			ID:       2,
			Material: []byte("short"),
		}
		if err := km.AddKey(key); err != ErrInvalidKey {
			t.Errorf("expected ErrInvalidKey, got %v", err)
		}
	})
}

func TestKeyManagerRotation(t *testing.T) {
	config := Config{
		Enabled: true,
		Rotation: KeyRotationConfig{
			Enabled:     true,
			Interval:    24 * time.Hour,
			RetainCount: 2,
		},
	}
	km := NewKeyManager(config)

	// Initial key
	key1, err := km.RotateKey()
	if err != nil {
		t.Fatalf("RotateKey() error = %v", err)
	}
	if key1.ID != 1 {
		t.Errorf("expected first key ID 1, got %d", key1.ID)
	}
	if !key1.Active {
		t.Error("new key should be active")
	}

	// Rotate again
	key2, err := km.RotateKey()
	if err != nil {
		t.Fatalf("RotateKey() error = %v", err)
	}
	if key2.ID != 2 {
		t.Errorf("expected second key ID 2, got %d", key2.ID)
	}

	// Original key should be deactivated
	k1, _ := km.GetKey(1)
	if k1.Active {
		t.Error("old key should be deactivated")
	}

	// Current key should be the latest
	current, _ := km.CurrentKey()
	if current.ID != 2 {
		t.Errorf("current should be 2, got %d", current.ID)
	}

	// Keep rotating to test cleanup
	km.RotateKey() // 3
	km.RotateKey() // 4
	km.RotateKey() // 5

	// Should only keep RetainCount + 1 = 3 keys
	if km.KeyCount() > 3 {
		t.Errorf("expected max 3 keys, got %d", km.KeyCount())
	}
}

func TestKeyManagerNoKey(t *testing.T) {
	km := NewKeyManager(DefaultConfig())

	_, err := km.CurrentKey()
	if err != ErrNoKey {
		t.Errorf("expected ErrNoKey, got %v", err)
	}
}

func TestEncryptor(t *testing.T) {
	material, _ := GenerateKey()
	km := NewKeyManager(DefaultConfig())
	km.AddKey(&Key{
		ID:        1,
		Material:  material,
		CreatedAt: time.Now(),
		Active:    true,
	})

	enc := NewEncryptor(km, true)

	t.Run("encrypt and decrypt bytes", func(t *testing.T) {
		plaintext := []byte("hello, world!")

		ciphertext, err := enc.Encrypt(plaintext)
		if err != nil {
			t.Fatalf("Encrypt() error = %v", err)
		}

		// Ciphertext should be different from plaintext
		if ciphertext == string(plaintext) {
			t.Error("ciphertext should differ from plaintext")
		}

		decrypted, err := enc.Decrypt(ciphertext)
		if err != nil {
			t.Fatalf("Decrypt() error = %v", err)
		}

		if !bytes.Equal(plaintext, decrypted) {
			t.Errorf("decrypted doesn't match: got %s, want %s", decrypted, plaintext)
		}
	})

	t.Run("encrypt and decrypt string", func(t *testing.T) {
		original := "sensitive data"

		encrypted, err := enc.EncryptString(original)
		if err != nil {
			t.Fatalf("EncryptString() error = %v", err)
		}

		decrypted, err := enc.DecryptString(encrypted)
		if err != nil {
			t.Fatalf("DecryptString() error = %v", err)
		}

		if decrypted != original {
			t.Errorf("got %s, want %s", decrypted, original)
		}
	})

	t.Run("encrypt empty data", func(t *testing.T) {
		ciphertext, err := enc.Encrypt([]byte{})
		if err != nil {
			t.Fatalf("Encrypt() empty error = %v", err)
		}

		decrypted, err := enc.Decrypt(ciphertext)
		if err != nil {
			t.Fatalf("Decrypt() empty error = %v", err)
		}

		if len(decrypted) != 0 {
			t.Error("expected empty decrypted data")
		}
	})

	t.Run("different encryptions differ", func(t *testing.T) {
		plaintext := []byte("test")

		enc1, _ := enc.Encrypt(plaintext)
		enc2, _ := enc.Encrypt(plaintext)

		if enc1 == enc2 {
			t.Error("encryptions should differ due to random nonce")
		}
	})
}

func TestEncryptorDisabled(t *testing.T) {
	km := NewKeyManager(DefaultConfig())
	enc := NewEncryptor(km, false)

	plaintext := []byte("hello")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("disabled encrypt error = %v", err)
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("disabled decrypt error = %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("disabled encryption should pass through")
	}

	if enc.IsEnabled() {
		t.Error("IsEnabled should return false")
	}
}

func TestEncryptorWithPassword(t *testing.T) {
	config := Config{
		Enabled: true,
		KeyDerivation: KeyDerivationConfig{
			Salt:       []byte("test-salt-12345678901234"),
			Iterations: 1000, // Low for testing speed
		},
	}

	enc, err := NewEncryptorWithPassword("my-password", config)
	if err != nil {
		t.Fatalf("NewEncryptorWithPassword() error = %v", err)
	}

	plaintext := "secret data"
	encrypted, err := enc.EncryptString(plaintext)
	if err != nil {
		t.Fatalf("Encrypt error = %v", err)
	}

	decrypted, err := enc.DecryptString(encrypted)
	if err != nil {
		t.Fatalf("Decrypt error = %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("got %s, want %s", decrypted, plaintext)
	}

	// Same password should decrypt
	enc2, _ := NewEncryptorWithPassword("my-password", config)
	decrypted2, err := enc2.DecryptString(encrypted)
	if err != nil {
		t.Fatalf("Decrypt with same password error = %v", err)
	}
	if decrypted2 != plaintext {
		t.Error("same password should decrypt correctly")
	}
}

func TestEncryptorWithPasswordDisabled(t *testing.T) {
	config := Config{Enabled: false}
	enc, err := NewEncryptorWithPassword("password", config)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if enc.IsEnabled() {
		t.Error("should be disabled")
	}
}

func TestEncryptField(t *testing.T) {
	material, _ := GenerateKey()
	km := NewKeyManager(DefaultConfig())
	km.AddKey(&Key{
		ID:        1,
		Material:  material,
		CreatedAt: time.Now(),
		Active:    true,
	})

	enc := NewEncryptor(km, true)

	t.Run("encrypt and decrypt field", func(t *testing.T) {
		value := "sensitive@email.com"

		encrypted, err := enc.EncryptField(value)
		if err != nil {
			t.Fatalf("EncryptField() error = %v", err)
		}

		// Should have enc:v1: prefix
		if len(encrypted) < 6 || encrypted[:4] != "enc:" {
			t.Errorf("expected enc: prefix, got %s", encrypted[:10])
		}

		decrypted, err := enc.DecryptField(encrypted)
		if err != nil {
			t.Fatalf("DecryptField() error = %v", err)
		}

		if decrypted != value {
			t.Errorf("got %s, want %s", decrypted, value)
		}
	})

	t.Run("decrypt unencrypted field", func(t *testing.T) {
		value := "plain-value"

		decrypted, err := enc.DecryptField(value)
		if err != nil {
			t.Fatalf("DecryptField() error = %v", err)
		}

		if decrypted != value {
			t.Error("unencrypted value should pass through")
		}
	})

	t.Run("disabled encryption passthrough", func(t *testing.T) {
		disabledEnc := NewEncryptor(km, false)

		value := "my-value"
		encrypted, _ := disabledEnc.EncryptField(value)
		if encrypted != value {
			t.Error("disabled should pass through")
		}

		decrypted, _ := disabledEnc.DecryptField(value)
		if decrypted != value {
			t.Error("disabled decrypt should pass through")
		}
	})
}

func TestDecryptInvalidData(t *testing.T) {
	material, _ := GenerateKey()
	km := NewKeyManager(DefaultConfig())
	km.AddKey(&Key{
		ID:        1,
		Material:  material,
		CreatedAt: time.Now(),
		Active:    true,
	})

	enc := NewEncryptor(km, true)

	t.Run("invalid base64", func(t *testing.T) {
		_, err := enc.Decrypt("not-valid-base64!!!")
		if err != ErrInvalidData {
			t.Errorf("expected ErrInvalidData, got %v", err)
		}
	})

	t.Run("data too short", func(t *testing.T) {
		_, err := enc.Decrypt("YWJj") // "abc" in base64
		if err != ErrInvalidData {
			t.Errorf("expected ErrInvalidData, got %v", err)
		}
	})

	t.Run("wrong key version", func(t *testing.T) {
		// Encrypt with key 1, then remove it
		ciphertext, _ := enc.Encrypt([]byte("test"))

		km2 := NewKeyManager(DefaultConfig())
		material2, _ := GenerateKey()
		km2.AddKey(&Key{
			ID:        2,
			Material:  material2,
			CreatedAt: time.Now(),
			Active:    true,
		})
		enc2 := NewEncryptor(km2, true)

		_, err := enc2.Decrypt(ciphertext)
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("tampered ciphertext", func(t *testing.T) {
		ciphertext, _ := enc.Encrypt([]byte("test"))

		// Tamper with the ciphertext
		data := []byte(ciphertext)
		if len(data) > 10 {
			data[10] ^= 0xFF
		}

		_, err := enc.Decrypt(string(data))
		// Could be invalid base64 or decryption error
		if err == nil {
			t.Error("expected error for tampered data")
		}
	})
}

func TestKeyRotationWithDecryption(t *testing.T) {
	config := Config{
		Enabled: true,
		Rotation: KeyRotationConfig{
			Enabled:     true,
			RetainCount: 5,
		},
	}
	km := NewKeyManager(config)

	// Create initial key
	km.RotateKey()
	enc := NewEncryptor(km, true)

	// Encrypt with key 1
	plaintext := []byte("original data")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt error = %v", err)
	}

	// Rotate key
	km.RotateKey()

	// Should still be able to decrypt with old key
	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt after rotation error = %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("should decrypt with old key after rotation")
	}
}

func TestHashKey(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()

	hash1 := HashKey(key1)
	hash2 := HashKey(key2)

	if len(hash1) != 32 { // 16 bytes = 32 hex chars
		t.Errorf("expected 32 char hash, got %d", len(hash1))
	}

	if hash1 == hash2 {
		t.Error("different keys should have different hashes")
	}

	// Same key should have same hash
	if HashKey(key1) != hash1 {
		t.Error("same key should have same hash")
	}
}

func TestSecureWipe(t *testing.T) {
	data := []byte("sensitive-data-to-wipe")
	original := make([]byte, len(data))
	copy(original, data)

	SecureWipe(data)

	// All bytes should be zero
	for i, b := range data {
		if b != 0 {
			t.Errorf("byte %d not wiped: %d", i, b)
		}
	}
}

func TestFieldEncryptionConfig(t *testing.T) {
	config := &FieldEncryptionConfig{}
	cases := testfixture.EncryptionPropertyCases()
	for _, property := range cases {
		switch property.Class {
		case "explicit":
			config.EncryptFields = append(config.EncryptFields, property.Name)
		case "PHI":
			config.PHIFields = append(config.PHIFields, property.Name)
		}
	}
	for _, group := range []struct {
		name, class string
		encrypted   bool
	}{
		{name: "encrypt fields", class: "explicit", encrypted: true},
		{name: "PHI fields", class: "PHI", encrypted: true},
		{name: "non-encrypted fields", class: "plain"},
	} {
		t.Run(group.name, func(t *testing.T) {
			for _, property := range cases {
				if property.Class == group.class && config.ShouldEncryptField(property.Name) != group.encrypted {
					t.Errorf("%s encrypted classification: got %v, want %v", property.Name, !group.encrypted, group.encrypted)
				}
			}
		})
	}
}

func TestDefaultPHIFields(t *testing.T) {
	fields := DefaultPHIFields()

	if len(fields) == 0 {
		t.Error("should have default PHI fields")
	}

	// Check some expected fields
	expected := []string{"ssn", "email", "password", "credit_card"}
	for _, e := range expected {
		found := false
		for _, f := range fields {
			if f == e {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected PHI field %s not found", e)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if !config.Enabled {
		t.Error("default should be enabled")
	}
	if config.KeyDerivation.Iterations != 600000 {
		t.Errorf("expected 600000 iterations, got %d", config.KeyDerivation.Iterations)
	}
	if config.Rotation.Interval != 90*24*time.Hour {
		t.Error("expected 90 day rotation interval")
	}
	if config.Rotation.RetainCount != 5 {
		t.Errorf("expected 5 retained keys, got %d", config.Rotation.RetainCount)
	}
}

func TestEncryptorKeyManager(t *testing.T) {
	km := NewKeyManager(DefaultConfig())
	enc := NewEncryptor(km, true)

	if enc.KeyManager() != km {
		t.Error("should return the same key manager")
	}
}

func TestEncryptNoKey(t *testing.T) {
	km := NewKeyManager(DefaultConfig())
	enc := NewEncryptor(km, true)

	_, err := enc.Encrypt([]byte("test"))
	if err != ErrNoKey {
		t.Errorf("expected ErrNoKey, got %v", err)
	}
}

func TestCurrentKeyInvalidKey(t *testing.T) {
	config := DefaultConfig()
	km := NewKeyManager(config)

	// Add an expired key directly
	km.mu.Lock()
	km.keys[1] = &Key{
		ID:        1,
		Material:  make([]byte, 32),
		ExpiresAt: time.Now().Add(-time.Hour),
		Active:    true,
	}
	km.current = 1
	km.mu.Unlock()

	_, err := km.CurrentKey()
	if err != ErrKeyExpired {
		t.Errorf("expected ErrKeyExpired, got %v", err)
	}
}

func TestCurrentKeyMissingFromMap(t *testing.T) {
	config := DefaultConfig()
	km := NewKeyManager(config)

	// Set current to a key that doesn't exist
	km.mu.Lock()
	km.current = 999
	km.mu.Unlock()

	_, err := km.CurrentKey()
	if err != ErrNoKey {
		t.Errorf("expected ErrNoKey, got %v", err)
	}
}

func TestDecryptStringInvalidBase64(t *testing.T) {
	material, _ := GenerateKey()
	km := NewKeyManager(DefaultConfig())
	km.AddKey(&Key{
		ID:        1,
		Material:  material,
		CreatedAt: time.Now(),
		Active:    true,
	})
	enc := NewEncryptor(km, true)

	_, err := enc.DecryptString("invalid-base64!!!")
	if err == nil {
		t.Error("expected error for invalid base64")
	}
}

func TestDecryptFieldInvalidEncrypted(t *testing.T) {
	material, _ := GenerateKey()
	km := NewKeyManager(DefaultConfig())
	km.AddKey(&Key{
		ID:        1,
		Material:  material,
		CreatedAt: time.Now(),
		Active:    true,
	})
	enc := NewEncryptor(km, true)

	// Invalid prefix format - should return as-is (passthrough)
	result, err := enc.DecryptField("enc:invalid")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "enc:invalid" {
		t.Errorf("expected passthrough, got %s", result)
	}

	// Valid prefix but invalid data - should return error
	_, err = enc.DecryptField("enc:v1:invalid-base64!!!")
	if err == nil {
		t.Error("expected error for invalid encrypted data")
	}
}

func TestEncryptFieldNoKey(t *testing.T) {
	km := NewKeyManager(DefaultConfig())
	enc := NewEncryptor(km, true)

	_, err := enc.EncryptField("test-value")
	if err != ErrNoKey {
		t.Errorf("expected ErrNoKey, got %v", err)
	}
}

func TestNewEncryptorWithPasswordInvalidSalt(t *testing.T) {
	config := Config{
		Enabled: true,
		KeyDerivation: KeyDerivationConfig{
			Salt:       []byte("too-short"), // Short salt - uses default
			Iterations: 1000,
		},
	}

	// Short salt uses default, doesn't error
	enc, err := NewEncryptorWithPassword("password", config)
	if err != nil {
		t.Fatalf("error = %v", err)
	}

	// Should still work
	encrypted, _ := enc.EncryptString("test")
	decrypted, _ := enc.DecryptString(encrypted)
	if decrypted != "test" {
		t.Error("should decrypt correctly")
	}
}

func TestNewEncryptorWithPasswordGenerateSalt(t *testing.T) {
	config := Config{
		Enabled: true,
		KeyDerivation: KeyDerivationConfig{
			Salt:       nil, // No salt, should generate
			Iterations: 1000,
		},
	}

	enc, err := NewEncryptorWithPassword("password", config)
	if err != nil {
		t.Fatalf("error = %v", err)
	}

	// Should be able to encrypt/decrypt
	encrypted, err := enc.EncryptString("test")
	if err != nil {
		t.Fatalf("encrypt error = %v", err)
	}

	decrypted, err := enc.DecryptString(encrypted)
	if err != nil {
		t.Fatalf("decrypt error = %v", err)
	}

	if decrypted != "test" {
		t.Error("decrypted doesn't match")
	}
}

func TestKeyRotationNoRetention(t *testing.T) {
	config := Config{
		Enabled: true,
		Rotation: KeyRotationConfig{
			Enabled:     false, // Rotation disabled
			RetainCount: 0,
		},
	}
	km := NewKeyManager(config)

	// Rotate multiple times
	km.RotateKey()
	km.RotateKey()
	km.RotateKey()
	km.RotateKey()
	km.RotateKey()

	// Without retention, all keys should be kept
	if km.KeyCount() != 5 {
		t.Errorf("expected 5 keys without retention, got %d", km.KeyCount())
	}
}

func TestCleanupOldKeysEdgeCases(t *testing.T) {
	t.Run("retention count equals key count", func(t *testing.T) {
		config := Config{
			Enabled: true,
			Rotation: KeyRotationConfig{
				Enabled:     true,
				RetainCount: 5,
			},
		}
		km := NewKeyManager(config)

		// Add exactly 5 keys
		for i := 0; i < 5; i++ {
			km.RotateKey()
		}

		// Should keep all 5
		if km.KeyCount() != 5 {
			t.Errorf("expected 5 keys, got %d", km.KeyCount())
		}
	})

	t.Run("retain count of 1", func(t *testing.T) {
		config := Config{
			Enabled: true,
			Rotation: KeyRotationConfig{
				Enabled:     true,
				RetainCount: 1,
			},
		}
		km := NewKeyManager(config)

		km.RotateKey()
		km.RotateKey()
		km.RotateKey()

		// Should keep 2 (1 + current)
		if km.KeyCount() != 2 {
			t.Errorf("expected 2 keys, got %d", km.KeyCount())
		}
	})
}
