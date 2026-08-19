package qr

import (
	"fmt"

	qrcode "github.com/skip2/go-qrcode"
)

// GenerateTerminalQR generates a compact ASCII QR code string suitable for printing in terminal
func GenerateTerminalQR(url string) (string, error) {
	q, err := qrcode.New(url, qrcode.Medium)
	if err != nil {
		return "", fmt.Errorf("failed to generate QR code: %w", err)
	}

	// Use small ASCII blocks (two pixels per character) for crisp terminal rendering
	return q.ToSmallString(false), nil
}

// GeneratePNG generates PNG image bytes of QR code
func GeneratePNG(url string, size int) ([]byte, error) {
	return qrcode.Encode(url, qrcode.Medium, size)
}
