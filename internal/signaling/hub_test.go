package signaling

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSignalingHubFullFlow(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWS(w, r)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// 1. Client A joins room "TEST01"
	urlA := wsURL + "?room=TEST01&client_id=client-A&device=MacBook"
	wsA, _, err := websocket.DefaultDialer.Dial(urlA, nil)
	if err != nil {
		t.Fatalf("failed to connect client A: %v", err)
	}
	defer wsA.Close()

	// Client A should receive room_status with peerCount=1
	var msgA SignalMessage
	if err := wsA.ReadJSON(&msgA); err != nil {
		t.Fatalf("failed to read initial msg from client A: %v", err)
	}
	if msgA.Type != "room_status" || msgA.PeerCount != 1 {
		t.Errorf("expected room_status peerCount 1, got %+v", msgA)
	}

	// 2. Client B joins room "TEST01"
	urlB := wsURL + "?room=TEST01&client_id=client-B&device=GalaxyTab"
	wsB, _, err := websocket.DefaultDialer.Dial(urlB, nil)
	if err != nil {
		t.Fatalf("failed to connect client B: %v", err)
	}
	defer wsB.Close()

	// Client B gets room_status
	var msgB SignalMessage
	if err := wsB.ReadJSON(&msgB); err != nil {
		t.Fatalf("failed to read initial msg from client B: %v", err)
	}
	if msgB.Type != "room_status" {
		t.Errorf("expected room_status, got %+v", msgB)
	}

	// Both A and B should receive "peer_joined"
	var peerJoinedA, peerJoinedB SignalMessage
	_ = wsA.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := wsA.ReadJSON(&peerJoinedA); err != nil {
		t.Fatalf("client A did not receive peer_joined: %v", err)
	}
	if peerJoinedA.Type != "peer_joined" || peerJoinedA.SenderID != "client-B" || peerJoinedA.Device != "GalaxyTab" {
		t.Errorf("unexpected peer_joined on client A: %+v", peerJoinedA)
	}

	_ = wsB.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := wsB.ReadJSON(&peerJoinedB); err != nil {
		t.Fatalf("client B did not receive peer_joined: %v", err)
	}
	if peerJoinedB.Type != "peer_joined" || peerJoinedB.SenderID != "client-A" || peerJoinedB.Device != "MacBook" {
		t.Errorf("unexpected peer_joined on client B: %+v", peerJoinedB)
	}

	// 3. Client A sends an "offer"
	offerMsg := SignalMessage{
		Type:    "offer",
		Payload: json.RawMessage(`{"sdp":"v=0...fake-offer","type":"offer"}`),
	}
	if err := wsA.WriteJSON(offerMsg); err != nil {
		t.Fatalf("client A failed to send offer: %v", err)
	}

	// Client B should receive the offer
	var receivedOffer SignalMessage
	_ = wsB.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := wsB.ReadJSON(&receivedOffer); err != nil {
		t.Fatalf("client B failed to receive offer: %v", err)
	}
	if receivedOffer.Type != "offer" || receivedOffer.SenderID != "client-A" {
		t.Errorf("unexpected offer received on B: %+v", receivedOffer)
	}

	// 4. Client C tries to join room "TEST01" (Room is full, max 2)
	urlC := wsURL + "?room=TEST01&client_id=client-C&device=iPhone"
	wsC, _, err := websocket.DefaultDialer.Dial(urlC, nil)
	if err == nil {
		var msgC SignalMessage
		_ = wsC.SetReadDeadline(time.Now().Add(2 * time.Second))
		_ = wsC.ReadJSON(&msgC)
		if msgC.Type != "error" {
			t.Errorf("expected error 'Room is full' for client C, got %+v", msgC)
		}
		wsC.Close()
	}

	// 5. Client B disconnects -> Client A receives "peer_left"
	wsB.Close()
	time.Sleep(100 * time.Millisecond)

	var peerLeftA SignalMessage
	_ = wsA.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := wsA.ReadJSON(&peerLeftA); err != nil {
		t.Fatalf("client A did not receive peer_left: %v", err)
	}
	if peerLeftA.Type != "peer_left" || peerLeftA.SenderID != "client-B" {
		t.Errorf("unexpected peer_left received on A: %+v", peerLeftA)
	}
}
