package main

import (
	"bytes"
)

func equal(sb []byte, s string) bool {
	return bytes.Equal(bytes.ToLower(sb), bytes.ToLower([]byte(s)))
}

func contains(sb []byte, s string) bool {
	return bytes.Contains(bytes.ToLower(sb), bytes.ToLower([]byte(s)))
}
