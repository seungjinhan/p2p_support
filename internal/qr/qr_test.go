package qr

import (
	"strings"
	"testing"
)

func TestGenerateTerminalQR(t *testing.T) {
	testURL := "http://192.168.1.100:8080/?room=ABC123"
	qrStr, err := GenerateTerminalQR(testURL)
	if err != nil {
		t.Fatalf("GenerateTerminalQR returned error: %v", err)
	}

	if len(qrStr) == 0 {
		t.Errorf("GenerateTerminalQR returned empty string")
	}

	// Should contain newline characters as it's a multi-line ASCII block
	if !strings.Contains(qrStr, "\n") {
		t.Errorf("expected multi-line ASCII QR, got: %q", qrStr)
	}
}

func TestGeneratePNG(t *testing.T) {
	testURL := "http://192.168.1.100:8080/?room=ABC123"
	pngBytes, err := GeneratePNG(testURL, 256)
	if err != nil {
		t.Fatalf("GeneratePNG returned error: %v", err)
	}

	if len(pngBytes) < 8 {
		t.Fatalf("PNG byte slice too short: %d", len(pngBytes))
	}

	// Check PNG magic number \x89PNG\r\n\x1a\n
	pngHeader := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}
	for i := 0; i < 8; i++ {
		if pngBytes[i] != pngHeader[i] {
			t.Errorf("invalid PNG header byte %d: got %x, expected %x", i, pngBytes[i], pngHeader[i])
		}
	}
}
