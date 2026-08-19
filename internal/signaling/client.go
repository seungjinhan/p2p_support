package signaling

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024 // 512 KB max for SDP/ICE payloads
)

// SignalMessage represents messages exchanged over WebSocket for WebRTC signaling
type SignalMessage struct {
	Type      string          `json:"type"`                // "join", "peer_joined", "peer_left", "offer", "answer", "candidate", "room_status", "error"
	RoomID    string          `json:"roomId,omitempty"`   // 6-digit room code
	SenderID  string          `json:"senderId,omitempty"` // Unique client UUID
	TargetID  string          `json:"targetId,omitempty"` // Target client UUID (optional for 1:1)
	Device    string          `json:"device,omitempty"`   // Device info (e.g., "MacBook", "Galaxy Tab")
	Payload   json.RawMessage `json:"payload,omitempty"`  // SDP or ICE candidate object
	Message   string          `json:"message,omitempty"`  // Error or status string
	PeerCount int             `json:"peerCount,omitempty"` // Number of peers in room
}

// Client represents an active WebSocket connection
type Client struct {
	Hub      *Hub
	Conn     *websocket.Conn
	Send     chan []byte
	RoomID   string
	ClientID string
	Device   string

	mu sync.Mutex
}

// ReadPump listens for incoming WebSocket messages from the client
func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		_ = c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[Signaling] Client %s read error: %v", c.ClientID, err)
			}
			break
		}

		var msg SignalMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("[Signaling] Invalid JSON from client %s: %v", c.ClientID, err)
			continue
		}

		msg.SenderID = c.ClientID
		msg.RoomID = c.RoomID
		if msg.Device == "" {
			msg.Device = c.Device
		}

		c.Hub.Broadcast <- &msg
	}
}

// WritePump pushes queued messages to the WebSocket connection
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// SendJSON safely serializes and sends a SignalMessage to this client
func (c *Client) SendJSON(msg *SignalMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.Send <- data:
	default:
		// Send buffer full
	}
	return nil
}
