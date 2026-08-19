package main

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go_messanger/internal/protocol"
	"go_messanger/internal/qr"
	"go_messanger/internal/signaling"
	"go_messanger/web"

	"github.com/gorilla/websocket"
)

func TestFullServerE2E(t *testing.T) {
	// 1. Setup Hub & FileServer like in main()
	hub := signaling.NewHub()
	go hub.Run()

	staticFS, err := fs.Sub(web.StaticFS, "static")
	if err != nil {
		t.Fatalf("failed to sub static FS: %v", err)
	}
	fileServer := http.FileServer(http.FS(staticFS))

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.ServeWS)
	mux.HandleFunc("/api/qr", func(w http.ResponseWriter, r *http.Request) {
		text := r.URL.Query().Get("text")
		if text == "" {
			http.Error(w, "missing 'text'", http.StatusBadRequest)
			return
		}
		pngBytes, err := qr.GeneratePNG(text, 256)
		if err != nil {
			http.Error(w, "failed to gen qr", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	})
	mux.Handle("/static/", http.StripPrefix("/static/", fileServer))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, err := fs.ReadFile(staticFS, "index.html")
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	// 2. Test GET / (HTML response)
	resp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / returned status %d, expected 200", resp.StatusCode)
	}
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	if !strings.Contains(buf.String(), "P2P Direct Share") {
		t.Errorf("GET / missing expected HTML title/brand")
	}

	// 3. Test GET /static/protocol.js
	respJs, err := http.Get(server.URL + "/static/protocol.js")
	if err != nil {
		t.Fatalf("GET /static/protocol.js failed: %v", err)
	}
	if respJs.StatusCode != http.StatusOK {
		t.Errorf("GET /static/protocol.js status %d, expected 200", respJs.StatusCode)
	}

	// 4. Test GET /api/qr?text=http://test
	respQR, err := http.Get(server.URL + "/api/qr?text=http://localhost:8080/?room=ABC123")
	if err != nil {
		t.Fatalf("GET /api/qr failed: %v", err)
	}
	if respQR.StatusCode != http.StatusOK {
		t.Errorf("GET /api/qr status %d", respQR.StatusCode)
	}
	if ct := respQR.Header.Get("Content-Type"); ct != "image/png" {
		t.Errorf("expected Content-Type image/png, got %s", ct)
	}

	// 5. Test WebSocket Signaling E2E: PC ↔ Mobile connection
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"

	// Connect PC client
	wsPC, _, err := websocket.DefaultDialer.Dial(wsURL+"?room=DEMO1&client_id=pc-1&device=MacBook", nil)
	if err != nil {
		t.Fatalf("failed to connect PC websocket: %v", err)
	}
	defer wsPC.Close()

	var msgPC signaling.SignalMessage
	_ = wsPC.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := wsPC.ReadJSON(&msgPC); err != nil {
		t.Fatalf("PC failed to read room_status: %v", err)
	}
	if msgPC.Type != "room_status" || msgPC.PeerCount != 1 {
		t.Errorf("expected room_status peerCount 1, got %+v", msgPC)
	}

	// Connect Mobile client (iPhone / Galaxy Tab)
	wsMobile, _, err := websocket.DefaultDialer.Dial(wsURL+"?room=DEMO1&client_id=mob-2&device=GalaxyTab", nil)
	if err != nil {
		t.Fatalf("failed to connect mobile websocket: %v", err)
	}
	defer wsMobile.Close()

	// Both receive peer_joined
	var joinedPC, joinedMobile signaling.SignalMessage
	_ = wsPC.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := wsPC.ReadJSON(&joinedPC); err != nil {
		t.Fatalf("PC failed to read peer_joined: %v", err)
	}
	if joinedPC.Type != "peer_joined" || joinedPC.Device != "GalaxyTab" {
		t.Errorf("unexpected joinedPC: %+v", joinedPC)
	}

	_ = wsMobile.SetReadDeadline(time.Now().Add(2 * time.Second))
	_ = wsMobile.ReadJSON(&joinedMobile) // discard room_status
	if err := wsMobile.ReadJSON(&joinedMobile); err != nil {
		t.Fatalf("Mobile failed to read peer_joined: %v", err)
	}
	if joinedMobile.Type != "peer_joined" || joinedMobile.Device != "MacBook" {
		t.Errorf("unexpected joinedMobile: %+v", joinedMobile)
	}

	// Forward Offer from PC -> Mobile
	offerMsg := signaling.SignalMessage{
		Type:    "offer",
		Payload: json.RawMessage(`{"type":"offer","sdp":"mock-sdp-offer"}`),
	}
	if err := wsPC.WriteJSON(offerMsg); err != nil {
		t.Fatalf("PC write offer failed: %v", err)
	}

	var recOffer signaling.SignalMessage
	_ = wsMobile.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := wsMobile.ReadJSON(&recOffer); err != nil {
		t.Fatalf("Mobile failed to receive offer: %v", err)
	}
	if recOffer.Type != "offer" {
		t.Errorf("expected offer type, got %s", recOffer.Type)
	}

	// 6. Test Binary 9-Byte Packet Roundtrip for File Transfer Simulation
	testChunkData := bytes.Repeat([]byte{0xAB, 0xCD, 0xEF, 0x12}, 8192) // 32KB
	chunkPkt := protocol.NewFileChunkPacket(999, 42, testChunkData)
	encoded := chunkPkt.Encode()

	if len(encoded) != protocol.HeaderSize+32768 {
		t.Errorf("expected total byte size %d, got %d", protocol.HeaderSize+32768, len(encoded))
	}

	decoded, err := protocol.Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.Type != protocol.TypeFileChunk || decoded.FileID != 999 || decoded.SeqOrLen != 42 {
		t.Errorf("decoded chunk packet mismatch: %+v", decoded)
	}
	if !bytes.Equal(decoded.Payload, testChunkData) {
		t.Errorf("decoded chunk payload data corruption")
	}
}
