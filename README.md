# ⚡ P2P Direct Share (WebRTC DataChannel 기반 초고속 메신저 & 파일 전송기)

MacBook, Windows PC, iPad, Galaxy Tab, 스마트폰(iOS/Android) 등 **모든 디바이스 간에 별도의 앱 설치 없이** 즉시 연결하여 실시간 채팅 및 대용량 파일을 전송하는 P2P 시스템입니다.

---

## 🌟 주요 특징

1. **중앙 중계 서버 Zero (P2P 통신)**:
   - 파일 데이터와 메시지가 어떤 외부 서버도 거치지 않고 기기 간 1:1 직통(`WebRTC DataChannel`)으로 전송됩니다.
   - 통신 비용 0원, 전송 속도 제한 없음 (회선 최대 대역폭 활용).
2. **초경량 9바이트 고정 헤더 바이너리 프로토콜**:
   - JSON이나 Base64 인코딩 오버헤드 없이, 32KB 청크 단위 순수 바이너리(`ArrayBuffer`)로 고속 스트리밍합니다.
3. **QR 코드 원스캔 연결**:
   - PC에서 프로그램을 실행하면 터미널과 웹 화면에 QR 코드가 즉시 출력됩니다.
   - 스마트폰이나 태블릿 기본 카메라로 QR 코드를 비추면 1초 만에 브라우저가 열리며 P2P 연결이 완료됩니다.
4. **단일 실행 파일 (Single Binary)**:
   - Go의 `embed` 기능으로 프론트엔드(HTML/JS/CSS)가 단일 바이너리 안에 내장되어 있어 별도의 웹서버 설치가 필요 없습니다.
5. **PWA (앱처럼 사용)**:
   - 모바일 브라우저에서 "홈 화면에 추가"를 누르면 앱스토어 앱처럼 전체 화면으로 실행됩니다.

---

## 🚀 빠른 시작 (실행 방법)

### 1. 소스 코드로 바로 실행
```bash
go run main.go
```
또는 포트를 지정하여 실행:
```bash
go run main.go -port 8080
```

### 2. 바이너리 빌드 및 실행
```bash
# 빌드
go build -o p2p-messenger main.go

# 실행
./p2p-messenger
```

---

## 📱 기기별 사용 시나리오

### 시나리오 1: PC ↔ 스마트폰 / 태블릿 (iPad, Galaxy Tab)
1. PC 터미널에서 `./p2p-messenger` 실행
2. 화면에 출력된 **QR 코드를 스마트폰/태블릿 카메라로 스캔**
3. 브라우저가 열리며 1:1 P2P 연결 완료!
4. 파일 드래그앤드롭 또는 사진첩/문서 선택으로 자유롭게 전송

### 시나리오 2: 같은 PC에서 2개의 창으로 테스트
1. PC에서 `./p2p-messenger` 실행
2. 브라우저에서 `http://localhost:8080` 열기
3. 웹 화면의 **[📋 초대 링크 복사]** 버튼 클릭
4. 새 탭(또는 시크릿 창/다른 브라우저)을 열고 주소창에 붙여넣기 하면 즉시 1:1 연결!

### 시나리오 3: 맥북 ↔ 윈도우 PC
1. 맥북에서 실행 후 화면에 표시된 **네트워크 링크(예: `http://192.168.0.15:8080/?room=ABC123`)**를 윈도우 PC 브라우저에 입력
2. 즉시 P2P 연결 완료!

---

## 📦 크로스 플랫폼 빌드 (Mac / Windows / Linux)

```bash
# macOS (Apple Silicon M1/M2/M3)
GOOS=darwin GOARCH=arm64 go build -o p2p-messenger-mac-arm64 main.go

# Windows (64bit)
GOOS=windows GOARCH=amd64 go build -o p2p-messenger-win.exe main.go

# Linux (64bit)
GOOS=linux GOARCH=amd64 go build -o p2p-messenger-linux main.go
```

---

## 🧪 테스트 실행
```bash
# 단위 테스트 및 E2E 테스트 (Race Detector 포함)
go test -v -race ./...
```
