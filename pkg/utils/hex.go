package utils

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// HexToBytes converts a hex string to a byte slice.
// It handles the "0x" prefix if present.
func HexToBytes(s string) ([]byte, error) {
	if strings.HasPrefix(s, "0x") {
		s = s[2:]
	}
	if len(s)%2 != 0 {
		s = "0" + s
	}
	data, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid hex string: %s", s)
	}
	return data, nil
}

func BytesToHex(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}
