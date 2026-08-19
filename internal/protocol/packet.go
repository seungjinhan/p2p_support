package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// HeaderSize is the fixed size of packet header (9 bytes)
// [0]   : Type (1 byte)
// [1:5] : FileID (4 bytes uint32)
// [5:9] : SeqOrLen (4 bytes uint32 - chunk sequence number or text length)
// [9:]  : Payload (variable length raw bytes)
const HeaderSize = 9

// Packet Types
const (
	TypeChat       uint8 = 0x01 // 텍스트 채팅 메시지
	TypeFileMeta   uint8 = 0x02 // 파일 전송 메타데이터 (JSON: name, size, totalChunks, sha256 등)
	TypeFileChunk  uint8 = 0x03 // 파일 바이너리 청크
	TypeFileAck    uint8 = 0x04 // 청크 수신 확인 및 흐름 제어 (ACK)
	TypeFileCancel uint8 = 0x05 // 파일 전송 취소
	TypePing       uint8 = 0x06 // 레이턴시 측정 및 킵얼라이브 Ping
	TypePong       uint8 = 0x07 // Pong 응답
)

var (
	ErrPacketTooShort = errors.New("packet data too short for 9-byte header")
)

// Packet represents a parsed P2P protocol packet
type Packet struct {
	Type     uint8
	FileID   uint32
	SeqOrLen uint32
	Payload  []byte
}

// NewChatPacket creates a text chat message packet
func NewChatPacket(text string) *Packet {
	payload := []byte(text)
	return &Packet{
		Type:     TypeChat,
		FileID:   0,
		SeqOrLen: uint32(len(payload)),
		Payload:  payload,
	}
}

// NewFileMetaPacket creates a file metadata packet
func NewFileMetaPacket(fileID uint32, metaJSON []byte) *Packet {
	return &Packet{
		Type:     TypeFileMeta,
		FileID:   fileID,
		SeqOrLen: uint32(len(metaJSON)),
		Payload:  metaJSON,
	}
}

// NewFileChunkPacket creates a raw binary file chunk packet
func NewFileChunkPacket(fileID uint32, chunkIndex uint32, chunkData []byte) *Packet {
	return &Packet{
		Type:     TypeFileChunk,
		FileID:   fileID,
		SeqOrLen: chunkIndex,
		Payload:  chunkData,
	}
}

// NewFileAckPacket creates an ACK packet for received chunk
func NewFileAckPacket(fileID uint32, chunkIndex uint32) *Packet {
	return &Packet{
		Type:     TypeFileAck,
		FileID:   fileID,
		SeqOrLen: chunkIndex,
		Payload:  nil,
	}
}

// NewCancelPacket creates a cancellation packet
func NewCancelPacket(fileID uint32) *Packet {
	return &Packet{
		Type:     TypeFileCancel,
		FileID:   fileID,
		SeqOrLen: 0,
		Payload:  nil,
	}
}

// NewPingPacket creates a Ping packet
func NewPingPacket(timestamp uint32) *Packet {
	return &Packet{
		Type:     TypePing,
		FileID:   0,
		SeqOrLen: timestamp,
		Payload:  nil,
	}
}

// NewPongPacket creates a Pong packet
func NewPongPacket(timestamp uint32) *Packet {
	return &Packet{
		Type:     TypePong,
		FileID:   0,
		SeqOrLen: timestamp,
		Payload:  nil,
	}
}

// Encode serializes the Packet into a 9-byte header + raw payload byte slice
func (p *Packet) Encode() []byte {
	buf := make([]byte, HeaderSize+len(p.Payload))
	buf[0] = p.Type
	binary.BigEndian.PutUint32(buf[1:5], p.FileID)
	binary.BigEndian.PutUint32(buf[5:9], p.SeqOrLen)
	if len(p.Payload) > 0 {
		copy(buf[HeaderSize:], p.Payload)
	}
	return buf
}

// Decode deserializes raw binary bytes into a Packet
func Decode(data []byte) (*Packet, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("%w: got %d bytes, expected at least %d", ErrPacketTooShort, len(data), HeaderSize)
	}

	p := &Packet{
		Type:     data[0],
		FileID:   binary.BigEndian.Uint32(data[1:5]),
		SeqOrLen: binary.BigEndian.Uint32(data[5:9]),
	}

	if len(data) > HeaderSize {
		p.Payload = make([]byte, len(data)-HeaderSize)
		copy(p.Payload, data[HeaderSize:])
	}

	return p, nil
}
