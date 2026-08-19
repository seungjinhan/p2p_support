# ⚡ P2P Direct Share (GitHub Pages Serverless Edition)

GitHub Pages에서 서버 비용 0원으로 호스팅되는 **WebRTC DataChannel 기반 1:1 P2P 파일 전송 및 실시간 메신저**입니다.

MacBook, Windows PC, iPad, Galaxy Tab, iPhone, Android 스마트폰 등 **모든 디바이스 간에 별도의 앱 설치 없이 웹 브라우저에서 바로 동작**합니다.

---

## 🌟 핵심 특징

1. **완전한 서버리스 (Serverless & Zero Cost)**:
   - GitHub Pages 무료 정적 웹 호스팅(HTTPS)에서 구동됩니다.
2. **서버 경유 0% (100% P2P 직통 전송)**:
   - 파일 및 채팅 데이터는 어떤 서버도 거치지 않고 양쪽 기기 간 `WebRTC DataChannel`로 직접 전송됩니다. (E2EE DTLS 종단간 암호화)
3. **초경량 9바이트 바이너리 패킷 엔진 (`protocol.js`)**:
   - JSON 및 Base64 변환 오버헤드 없이 32KB 청크 단위 순수 바이너리(`ArrayBuffer`)로 0-Copy 초고속 스트리밍합니다.
4. **QR 코드 1초 연결**:
   - PC 화면에 뜬 QR 코드를 스마트폰/태블릿 기본 카메라로 스캔하면 1초 만에 브라우저가 열리며 1:1 연결됩니다.
5. **PWA 지원**:
   - 모바일 브라우저에서 **[홈 화면에 추가]**를 누르면 앱스토어 앱처럼 전체 화면으로 실행됩니다.

---

## 📁 프로젝트 파일 구성

```
├── index.html        # GitHub Pages 메인 페이지 (반응형 UI)
├── style.css         # 모던 다크 테마 & 드래그앤드롭 스타일
├── protocol.js       # 9바이트 바이너리 패킷 코덱
├── qr.min.js         # 브라우저용 순수 자바스크립트 QR 생성기 (외부 의존성 X)
├── app.js            # WebRTC P2P DataChannel & 시그널링 연동 로직
└── README.md         # 프로젝트 안내 및 배포 가이드
```

---

## 🚀 GitHub Pages 배포 방법 (3단계)

### 1단계: Git 저장소 초기화 및 푸시
```bash
# Git 초기화
git init

# 파일 추가 및 커밋
git add .
git commit -m "Deploy P2P WebRTC Messenger to GitHub Pages"

# GitHub 원격 저장소 연결 (본인의 GitHub 저장소 URL 입력)
git branch -M main
git remote add origin https://github.com/<내-깃허브-아이디>/<저장소-이름>.git
git push -u origin main
```

### 2단계: GitHub Pages 활성화
1. GitHub 저장소 페이지 접속
2. 상단 메뉴의 **Settings** 클릭
3. 좌측 사이드바에서 **Pages** 클릭
4. **Build and deployment** > **Source**에서 `Deploy from a branch` 선택
5. **Branch**를 `main` / `/(root)` 로 선택 후 **Save** 클릭

### 3단계: 접속 및 사용
- 약 30초 후 상단에 생성된 주소(예: `https://<내-아이디>.github.io/<저장소-이름>/`)로 접속합니다.
- **PC, 스마트폰(5G/LTE), 태블릿** 어디서든 주소를 열고 QR 코드를 스캔하여 자유롭게 사용하세요!

---

## 📱 사용 시나리오

1. **PC ↔ 스마트폰 (LTE/5G or Wi-Fi)**:
   - PC에서 배포된 GitHub Pages 주소 접속
   - 스마트폰 기본 카메라로 PC 화면의 **QR 코드 스캔**
   - 1초 만에 P2P 연결 완료 → 파일 드래그앤드롭 또는 사진첩/문서 전송!
2. **같은 PC에서 2개 창 열기**:
   - 한쪽 창에서 **[📋 초대 링크 복사]** 클릭
   - 새 탭 또는 다른 브라우저에 붙여넣기 하면 즉시 1:1 연결!
3. **친구/동료에게 원격 전송**:
   - 생성된 6자리 방 코드(예: `A7K92X`)를 알려주고 [입장] 버튼을 누르면 원격 P2P 연결!
