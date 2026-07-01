package wechat

import (
	"crypto/aes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// encryptAesEcb encrypts plaintext using AES-128-ECB with PKCS7 padding.
func encryptAesEcb(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	bs := block.BlockSize()
	padded := pkcs7Pad(plaintext, bs)
	ciphertext := make([]byte, len(padded))
	for i := 0; i < len(padded); i += bs {
		block.Encrypt(ciphertext[i:i+bs], padded[i:i+bs])
	}
	return ciphertext, nil
}

// aesPaddedSize returns the AES-128-ECB padded size (PKCS7, block=16).
func aesPaddedSize(size int) int {
	return ((size + 1 + 15) / 16) * 16
}

// pkcs7Pad adds PKCS7 padding to data.
func pkcs7Pad(data []byte, blockSize int) []byte {
	padLen := blockSize - (len(data) % blockSize)
	padding := byte(padLen)
	padded := make([]byte, len(data)+padLen)
	copy(padded, data)
	for i := len(data); i < len(padded); i++ {
		padded[i] = padding
	}
	return padded
}

// decryptAesEcb decrypts data using AES-128-ECB with PKCS7 unpadding.
func decryptAesEcb(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	bs := block.BlockSize()
	if len(ciphertext) == 0 || len(ciphertext)%bs != 0 {
		return nil, fmt.Errorf("ciphertext length %d not multiple of block size %d", len(ciphertext), bs)
	}
	plaintext := make([]byte, len(ciphertext))
	for i := 0; i < len(ciphertext); i += bs {
		block.Decrypt(plaintext[i:i+bs], ciphertext[i:i+bs])
	}
	return pkcs7Unpad(plaintext, bs)
}

// pkcs7Unpad removes PKCS7 padding.
func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}
	padding := int(data[len(data)-1])
	if padding < 1 || padding > blockSize || padding > len(data) {
		return nil, fmt.Errorf("invalid PKCS7 padding %d", padding)
	}
	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, fmt.Errorf("invalid PKCS7 padding at byte %d", i)
		}
	}
	return data[:len(data)-padding], nil
}

// parseAesKey parses a base64-encoded AES key.
func parseAesKey(aesKeyBase64 string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(aesKeyBase64)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(aesKeyBase64)
		if err != nil {
			return nil, fmt.Errorf("base64 decode aes_key: %w", err)
		}
	}
	if len(decoded) == 16 {
		return decoded, nil
	}
	if len(decoded) == 32 {
		// hex-encoded key: base64 -> hex string -> raw bytes
		raw, err := hex.DecodeString(string(decoded))
		if err == nil && len(raw) == 16 {
			return raw, nil
		}
	}
	return nil, fmt.Errorf("aes_key must decode to 16 raw bytes or 32-char hex, got %d bytes", len(decoded))
}

// hexToBase64 converts a hex string to base64 encoding.
func hexToBase64(hexStr string) string {
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}
