package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go_messanger/internal/network"
	"go_messanger/internal/qr"
	"go_messanger/internal/signaling"
	"go_messanger/web"
)

func randomRoomCode(length int) string {
	const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[r.Intn(len(chars))]
	}
	return string(result)
}

func loadEnvPassword() string {
	data, err := os.ReadFile(".p2p.env")
	if err != nil {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ACCESS_PASSWORD=") {
			return strings.TrimPrefix(line, "ACCESS_PASSWORD=")
		}
	}
	return ""
}

func main() {
	portFlag := flag.Int("port", 8080, "Port to listen on (e.g. 8080)")
	roomFlag := flag.String("room", "", "Predefined 6-character room code (optional)")
	hostFlag := flag.String("host", "", "Custom host/IP to advertise (e.g. 192.168.0.15)")
	flag.Parse()

	envPassword := loadEnvPassword()
	roomCode := strings.ToUpper(strings.TrimSpace(*roomFlag))
	if roomCode == "" {
		roomCode = randomRoomCode(6)
	}

	// 1. Setup Signaling Hub
	hub := signaling.NewHub()
	go hub.Run()

	// 2. Setup embedded static filesystem
	staticFS, err := fs.Sub(web.StaticFS, "static")
	if err != nil {
		log.Fatalf("Failed to load embedded static filesystem: %v", err)
	}
	fileServer := http.FileServer(http.FS(staticFS))

	// 3. Setup HTTP Multiplexer
	mux := http.NewServeMux()

	// WebSocket Signaling endpoint
	mux.HandleFunc("/ws", hub.ServeWS)

	// QR Code image endpoint (returns PNG)
	mux.HandleFunc("/api/qr", func(w http.ResponseWriter, r *http.Request) {
		text := r.URL.Query().Get("text")
		if text == "" {
			http.Error(w, "missing 'text' query parameter", http.StatusBadRequest)
			return
		}
		size := 256
		if sizeParam := r.URL.Query().Get("size"); sizeParam != "" {
			if s, err := strconv.Atoi(sizeParam); err == nil && s > 64 && s <= 1024 {
				size = s
			}
		}

		pngBytes, err := qr.GeneratePNG(text, size)
		if err != nil {
			http.Error(w, "failed to generate QR code", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		_, _ = w.Write(pngBytes)
	})

	// Static assets & Root HTML
	mux.Handle("/static/", http.StripPrefix("/static/", fileServer))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			fileServer.ServeHTTP(w, r)
			return
		}
		data, err := fs.ReadFile(staticFS, "index.html")
		if err != nil {
			http.Error(w, "index.html not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})

	// 4. IP & URLs determination
	lanIP := strings.TrimSpace(*hostFlag)
	if lanIP == "" {
		lanIP = network.GetPreferredOutboundIP()
	}
	allIPs := network.GetLocalIPs()

	localURL := fmt.Sprintf("http://localhost:%d/?room=%s", *portFlag, roomCode)
	lanURL := fmt.Sprintf("http://%s:%d/?room=%s", lanIP, *portFlag, roomCode)

	// 5. Generate Terminal ASCII QR Code
	asciiQR, qrErr := qr.GenerateTerminalQR(lanURL)

	// 6. Print Console Banner
	fmt.Println()
	fmt.Println("==========================================================================")
	fmt.Println("  ⚡ P2P WebRTC DataChannel Messenger & File Transfer (9B Header)")
	fmt.Println("==========================================================================")
	fmt.Println()
	fmt.Println("  [ 스마트폰 / 갤럭시탭 / 아이패드에서 접속 ]")
	fmt.Println("  ⚠️ 스마트폰이 맥북과 '동일한 Wi-Fi'에 연결되어 있어야 합니다! (LTE/5G X)")
	fmt.Println("  👉 기본 카메라로 아래 QR 코드를 비추면 즉시 연결됩니다:")
	fmt.Println()
	if qrErr == nil {
		fmt.Println(asciiQR)
	}
	fmt.Println()
	fmt.Println("  [ 접속 링크 안내 ]")
	fmt.Printf("  👉 로컬 링크 (같은 PC):     %s\n", localURL)
	fmt.Printf("  👉 QR 코드 연결 링크 (LAN): %s\n", lanURL)
	if len(allIPs) > 1 {
		fmt.Println("  👉 기타 감지된 네트워크 IP:")
		for _, ip := range allIPs {
			if ip != lanIP {
				fmt.Printf("     - http://%s:%d/?room=%s\n", ip, *portFlag, roomCode)
			}
		}
	}
	fmt.Println()
	fmt.Printf("  👉 방 코드 (Room Code):     %s\n", roomCode)
	if envPassword != "" {
		fmt.Printf("  🔒 보안 비밀번호 (.p2p.env): %s\n", envPassword)
	} else {
		fmt.Println("  🔒 보안 비밀번호 (.p2p.env): (미설정 - 브라우저에서 직접 입력)")
	}
	fmt.Println("==========================================================================")
	fmt.Println("  [서버 실행 중...] 종료하려면 Ctrl+C 를 누르세요.")
	fmt.Println()

	// 7. Start HTTP Server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *portFlag),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Graceful shutdown channel
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server Listen error: %v", err)
		}
	}()

	<-stopChan
	fmt.Println("\n[알림] 서버를 안전하게 종료하는 중...")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	fmt.Println("[알림] 서버가 종료되었습니다.")
}
