package protocol

import (
	"bytes"
	"testing"
)

func TestPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		packet   *Packet
		validate func(*testing.T, *Packet)
	}{
		{
			name:   "Chat Packet",
			packet: NewChatPacket("안녕하세요! Hello P2P!"),
			validate: func(t *testing.T, p *Packet) {
				if p.Type != TypeChat {
					t.Errorf("expected TypeChat(1), got %d", p.Type)
				}
				if p.FileID != 0 {
					t.Errorf("expected FileID 0, got %d", p.FileID)
				}
				expectedText := "안녕하세요! Hello P2P!"
				if string(p.Payload) != expectedText {
					t.Errorf("expected payload %q, got %q", expectedText, string(p.Payload))
				}
				if p.SeqOrLen != uint32(len([]byte(expectedText))) {
					t.Errorf("expected SeqOrLen %d, got %d", len([]byte(expectedText)), p.SeqOrLen)
				}
			},
		},
		{
			name:   "File Meta Packet",
			packet: NewFileMetaPacket(1001, []byte(`{"name":"test.zip","size":1048576,"totalChunks":32}`)),
			validate: func(t *testing.T, p *Packet) {
				if p.Type != TypeFileMeta {
					t.Errorf("expected TypeFileMeta(2), got %d", p.Type)
				}
				if p.FileID != 1001 {
					t.Errorf("expected FileID 1001, got %d", p.FileID)
				}
				if !bytes.Contains(p.Payload, []byte("test.zip")) {
					t.Errorf("payload missing filename: %s", string(p.Payload))
				}
			},
		},
		{
			name: "File Chunk Packet with Binary Data",
			packet: func() *Packet {
				chunk := make([]byte, 32*1024) // 32KB
				for i := range chunk {
					chunk[i] = byte(i % 256)
				}
				return NewFileChunkPacket(2002, 5, chunk)
			}(),
			validate: func(t *testing.T, p *Packet) {
				if p.Type != TypeFileChunk {
					t.Errorf("expected TypeFileChunk(3), got %d", p.Type)
				}
				if p.FileID != 2002 {
					t.Errorf("expected FileID 2002, got %d", p.FileID)
				}
				if p.SeqOrLen != 5 {
					t.Errorf("expected SeqOrLen 5, got %d", p.SeqOrLen)
				}
				if len(p.Payload) != 32*1024 {
					t.Errorf("expected payload len 32KB, got %d", len(p.Payload))
				}
				if p.Payload[10] != 10 || p.Payload[255] != 255 {
					t.Errorf("payload data corrupted")
				}
			},
		},
		{
			name:   "File ACK Packet",
			packet: NewFileAckPacket(3003, 12),
			validate: func(t *testing.T, p *Packet) {
				if p.Type != TypeFileAck {
					t.Errorf("expected TypeFileAck(4), got %d", p.Type)
				}
				if p.FileID != 3003 || p.SeqOrLen != 12 {
					t.Errorf("mismatched FileID/Seq: %d, %d", p.FileID, p.SeqOrLen)
				}
				if len(p.Payload) != 0 {
					t.Errorf("expected empty payload, got %d bytes", len(p.Payload))
				}
			},
		},
		{
			name:   "Ping and Pong Packets",
			packet: NewPingPacket(123456789),
			validate: func(t *testing.T, p *Packet) {
				if p.Type != TypePing {
					t.Errorf("expected TypePing(6), got %d", p.Type)
				}
				if p.SeqOrLen != 123456789 {
					t.Errorf("expected timestamp 123456789, got %d", p.SeqOrLen)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.packet.Encode()
			if len(encoded) < HeaderSize {
				t.Fatalf("encoded bytes length %d is less than HeaderSize %d", len(encoded), HeaderSize)
			}

			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("unexpected decode error: %v", err)
			}

			tt.validate(t, decoded)
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	// Less than 9 bytes
	tooShort := []byte{0x01, 0x00, 0x00, 0x00}
	_, err := Decode(tooShort)
	if err == nil {
		t.Errorf("expected error for data shorter than HeaderSize, got nil")
	}
}
