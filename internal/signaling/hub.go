package signaling

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow any origin for LAN and P2P access across devices
		return true
	},
}

// Hub maintains active clients and handles routing WebRTC signaling messages
type Hub struct {
	rooms      map[string]map[*Client]bool
	register   chan *Client
	Unregister chan *Client
	Broadcast  chan *SignalMessage

	mu sync.RWMutex
}

// NewHub creates a new Hub instance
func NewHub() *Hub {
	return &Hub{
		rooms:      make(map[string]map[*Client]bool),
		register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan *SignalMessage, 256),
	}
}

// Run starts the signaling event loop
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.handleRegister(client)

		case client := <-h.Unregister:
			h.handleUnregister(client)

		case msg := <-h.Broadcast:
			h.handleBroadcast(msg)
		}
	}
}

func (h *Hub) handleRegister(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	roomClients, exists := h.rooms[client.RoomID]
	if !exists {
		roomClients = make(map[*Client]bool)
		h.rooms[client.RoomID] = roomClients
	}

	if len(roomClients) >= 2 {
		// Room is full for 1:1 P2P pair
		_ = client.SendJSON(&SignalMessage{
			Type:    "error",
			RoomID:  client.RoomID,
			Message: "Room is full (maximum 2 devices per 1:1 session)",
		})
		return
	}

	roomClients[client] = true
	log.Printf("[Signaling] Client %s (%s) joined room %s (Total in room: %d)",
		client.ClientID, client.Device, client.RoomID, len(roomClients))

	// Send current room status to the newly joined client
	_ = client.SendJSON(&SignalMessage{
		Type:      "room_status",
		RoomID:    client.RoomID,
		SenderID:  client.ClientID,
		PeerCount: len(roomClients),
	})

	// If there is already another peer, notify both of each other
	if len(roomClients) == 2 {
		var peers []*Client
		for c := range roomClients {
			peers = append(peers, c)
		}

		c1, c2 := peers[0], peers[1]

		// Notify c1 about c2
		_ = c1.SendJSON(&SignalMessage{
			Type:      "peer_joined",
			RoomID:    client.RoomID,
			SenderID:  c2.ClientID,
			Device:    c2.Device,
			PeerCount: 2,
		})

		// Notify c2 about c1
		_ = c2.SendJSON(&SignalMessage{
			Type:      "peer_joined",
			RoomID:    client.RoomID,
			SenderID:  c1.ClientID,
			Device:    c1.Device,
			PeerCount: 2,
		})
	}
}

func (h *Hub) handleUnregister(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	roomClients, exists := h.rooms[client.RoomID]
	if exists {
		if _, ok := roomClients[client]; ok {
			delete(roomClients, client)
			close(client.Send)
			log.Printf("[Signaling] Client %s left room %s", client.ClientID, client.RoomID)

			if len(roomClients) == 0 {
				delete(h.rooms, client.RoomID)
			} else {
				// Notify remaining peer that the other peer left
				for remaining := range roomClients {
					_ = remaining.SendJSON(&SignalMessage{
						Type:      "peer_left",
						RoomID:    client.RoomID,
						SenderID:  client.ClientID,
						PeerCount: len(roomClients),
					})
				}
			}
		}
	}
}

func (h *Hub) handleBroadcast(msg *SignalMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	roomClients, exists := h.rooms[msg.RoomID]
	if !exists {
		return
	}

	for client := range roomClients {
		// Forward message to the OTHER peer in the room
		if client.ClientID != msg.SenderID {
			_ = client.SendJSON(msg)
		}
	}
}

// ServeWS handles WebSocket upgrading and attaches client to room
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Signaling] WebSocket upgrade error: %v", err)
		return
	}

	query := r.URL.Query()
	roomID := query.Get("room")
	if roomID == "" {
		roomID = "DEFAULT"
	}
	clientID := query.Get("client_id")
	if clientID == "" {
		clientID = "peer-" + r.RemoteAddr
	}
	device := query.Get("device")
	if device == "" {
		device = "Unknown Device"
	}

	client := &Client{
		Hub:      h,
		Conn:     conn,
		Send:     make(chan []byte, 256),
		RoomID:   roomID,
		ClientID: clientID,
		Device:   device,
	}

	h.register <- client

	go client.WritePump()
	go client.ReadPump()
}

// GetRoomPeerCount returns active peer count in a given room (for testing & status)
func (h *Hub) GetRoomPeerCount(roomID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if roomClients, exists := h.rooms[roomID]; exists {
		return len(roomClients)
	}
	return 0
}
