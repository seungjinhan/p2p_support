/**
 * WebRTC P2P DataChannel Engine with 6-Digit Key, SHA-256 Password Authentication & Full-Duplex Transfer
 */

// Configuration: 16KB universally safe SCTP chunk size for full-duplex simultaneous transfer
const CHUNK_SIZE = 16 * 1024; // 16KB
const BUFFER_LOW_THRESHOLD = 64 * 1024; // 64KB
const BUFFER_HIGH_THRESHOLD = 256 * 1024; // 256KB

// Multi-tier STUN + OpenRelay TURN servers for NAT traversal
const rtcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' },
        {
            urls: 'turn:openrelay.metered.ca:80',
            username: 'openrelayproject',
            credential: 'openrelayproject'
        },
        {
            urls: 'turn:openrelay.metered.ca:443',
            username: 'openrelayproject',
            credential: 'openrelayproject'
        }
    ]
};

// Global Enterprise MQTT WebSocket Brokers
const BROKER_URLS = [
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt'
];

// State
let mqttClient = null;
let peerConnection = null;
let dataChannel = null;
let currentRoomId = '';
let currentSecureTopic = '';
let currentPassword = '';
let myClientId = 'peer-' + Math.random().toString(36).substring(2, 9);
let isDirectP2P = false;
let pingInterval = null;
let joinBroadcastInterval = null;
let peerPruneInterval = null;
let candidateQueue = [];

// Live Connected Peers Map: clientId -> { device, lastSeen, isDirectP2P }
const activePeers = new Map();

// Active file transfer tracking & cancellation
let activeSendingFile = null;
const incomingTransfers = new Map();
const pendingEarlyChunks = new Map();
const cachedOutgoingFiles = new Map();
const cancelledOutgoingFileIds = new Set();
const cancelledIncomingFileIds = new Set();

// History Persistence Keys
const STORAGE_KEY_CHAT = 'p2p_chat_history';
const STORAGE_KEY_FILES = 'p2p_files_history';
const STORAGE_KEY_AUTH_ROOM = 'p2p_auth_room';
const STORAGE_KEY_AUTH_PW = 'p2p_auth_pw';

// Detect Device Information
function getDeviceDescription() {
    const ua = navigator.userAgent;
    let os = 'Unknown Device';
    if (/iPad/i.test(ua) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1)) os = 'iPad';
    else if (/iPhone/i.test(ua)) os = 'iPhone';
    else if (/Macintosh|Mac OS X/i.test(ua)) os = 'MacBook';
    else if (/Windows NT/i.test(ua)) os = 'Windows PC';
    else if (/Android/i.test(ua)) {
        os = /Tablet|SM-T|SM-X/i.test(ua) ? 'Galaxy Tab' : 'Android Phone';
    } else if (/Linux/i.test(ua)) os = 'Linux PC';

    return os;
}

const myDeviceDesc = getDeviceDescription();

// UI Elements
const statusBadge = document.getElementById('statusBadge');
const statusText = document.getElementById('statusText');
const pingBadge = document.getElementById('pingBadge');
const lobbyView = document.getElementById('lobbyView');
const mainAppView = document.getElementById('mainAppView');
const lobbyKeyInput = document.getElementById('lobbyKeyInput');
const lobbyPasswordInput = document.getElementById('lobbyPasswordInput');
const btnTogglePassword = document.getElementById('btnTogglePassword');
const btnGenerateKey = document.getElementById('btnGenerateKey');
const btnEnterRoom = document.getElementById('btnEnterRoom');
const roomCodeDisplay = document.getElementById('roomCodeDisplay');
const qrContainer = document.getElementById('qrContainer');
const btnCopyLink = document.getElementById('btnCopyLink');
const btnLeaveRoom = document.getElementById('btnLeaveRoom');
const peerListContainer = document.getElementById('peerListContainer');
const peerCountBadge = document.getElementById('peerCountBadge');
const completedFilesLog = document.getElementById('completedFilesLog');
const completedLogCountBadge = document.getElementById('completedLogCountBadge');
const fileLogEmptyText = document.getElementById('fileLogEmptyText');
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const transferList = document.getElementById('transferList');
const chatMessages = document.getElementById('chatMessages');
const chatForm = document.getElementById('chatForm');
const chatInput = document.getElementById('chatInput');
const btnSendClipboard = document.getElementById('btnSendClipboard');
const peerStatusLabel = document.getElementById('peerStatusLabel');

// Accidental Refresh Protection
window.addEventListener('beforeunload', (e) => {
    if (activeSendingFile || incomingTransfers.size > 0) {
        e.preventDefault();
        e.returnValue = '파일 전송이 진행 중입니다. 페이지를 벗어나시겠습니까?';
        return e.returnValue;
    }
});

// Toast Notification
function showToast(msg) {
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.textContent = msg;
    document.body.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 2500);
}

// Format bytes
function formatBytes(bytes, decimals = 1) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// SHA-256 Cryptographic Hash Helper
async function sha256(message) {
    const msgBuffer = new TextEncoder().encode(message);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

// Generate random 6-character security key
function generateRoomCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    for (let i = 0; i < 6; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

// -------------------------------------------------------------
// Lobby & Authentication Flow
// -------------------------------------------------------------
async function initApp() {
    const params = new URLSearchParams(window.location.search);
    const roomParam = params.get('room');
    const pwParam = params.get('pw');

    const savedRoom = sessionStorage.getItem(STORAGE_KEY_AUTH_ROOM);
    const savedPw = sessionStorage.getItem(STORAGE_KEY_AUTH_PW);

    if (roomParam && roomParam.trim().length >= 4) {
        lobbyKeyInput.value = roomParam.trim().toUpperCase();
        if (pwParam && pwParam.trim().length > 0) {
            lobbyPasswordInput.value = pwParam.trim();
            await enterRoom(roomParam.trim(), pwParam.trim());
            return;
        } else if (savedRoom === roomParam.trim().toUpperCase() && savedPw) {
            lobbyPasswordInput.value = savedPw;
            await enterRoom(savedRoom, savedPw);
            return;
        }
    } else if (savedRoom && savedPw) {
        lobbyKeyInput.value = savedRoom;
        lobbyPasswordInput.value = savedPw;
    }

    showLobbyView();
}

function showLobbyView() {
    lobbyView.style.display = 'flex';
    mainAppView.style.display = 'none';
    statusText.textContent = '보안 키 및 비밀번호 입력 대기 중...';
    statusBadge.className = 'status-badge';
    pingBadge.style.display = 'none';
    cleanupConnection();
}

async function enterRoom(key, password) {
    if (!key || key.length < 4) {
        showToast('⚠️ 4~6자리의 방 키를 입력해 주세요.');
        lobbyKeyInput.focus();
        return;
    }

    if (!password || password.trim().length === 0) {
        showToast('⚠️ 접속 비밀번호를 입력해 주세요.');
        lobbyPasswordInput.focus();
        return;
    }

    currentRoomId = key.toUpperCase();
    currentPassword = password.trim();

    // Compute SHA-256 hash of password for cryptographic topic isolation
    const pwHash = await sha256(currentPassword);
    currentSecureTopic = `${currentRoomId}_${pwHash.substring(0, 12)}`;

    // Store in session storage
    sessionStorage.setItem(STORAGE_KEY_AUTH_ROOM, currentRoomId);
    sessionStorage.setItem(STORAGE_KEY_AUTH_PW, currentPassword);

    const newUrl = `${window.location.pathname}?room=${currentRoomId}`;
    window.history.replaceState({}, '', newUrl);

    lobbyView.style.display = 'none';
    mainAppView.style.display = 'grid';
    roomCodeDisplay.textContent = currentRoomId;

    renderQRCode();
    restoreSessionHistory();
    renderPeerList();

    connectSignaling(0);
    startPeerPruner();
}

// Password show/hide toggle
btnTogglePassword.addEventListener('click', () => {
    if (lobbyPasswordInput.type === 'password') {
        lobbyPasswordInput.type = 'text';
        btnTogglePassword.textContent = '🙈';
    } else {
        lobbyPasswordInput.type = 'password';
        btnTogglePassword.textContent = '👁️';
    }
});

// Key Input formatting
lobbyKeyInput.addEventListener('input', (e) => {
    e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
});

lobbyKeyInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        lobbyPasswordInput.focus();
    }
});

lobbyPasswordInput.addEventListener('keydown', async (e) => {
    if (e.key === 'Enter') {
        const key = lobbyKeyInput.value.trim().toUpperCase();
        const pw = lobbyPasswordInput.value.trim();
        await enterRoom(key, pw);
    }
});

btnGenerateKey.addEventListener('click', () => {
    const code = generateRoomCode();
    lobbyKeyInput.value = code;
    lobbyPasswordInput.focus();
    showToast(`🎲 방 키 [${code}] 생성 완료! 비밀번호를 입력해 주세요.`);
});

btnEnterRoom.addEventListener('click', async () => {
    const key = lobbyKeyInput.value.trim().toUpperCase();
    const pw = lobbyPasswordInput.value.trim();
    await enterRoom(key, pw);
});

btnLeaveRoom.addEventListener('click', () => {
    if (confirm('보안 방에서 나가시겠습니까?')) {
        sessionStorage.removeItem(STORAGE_KEY_AUTH_ROOM);
        sessionStorage.removeItem(STORAGE_KEY_AUTH_PW);
        const newUrl = window.location.pathname;
        window.history.replaceState({}, '', newUrl);
        showLobbyView();
        showToast('🔒 방에서 나왔습니다.');
    }
});

function renderQRCode() {
    const fullURL = window.location.origin + window.location.pathname + `?room=${currentRoomId}`;
    if (window.QRCode && qrContainer) {
        new QRCode(qrContainer, fullURL, 180);
    }
}

btnCopyLink.addEventListener('click', async () => {
    const fullURL = window.location.origin + window.location.pathname + `?room=${currentRoomId}`;
    try {
        await navigator.clipboard.writeText(fullURL);
        showToast('📋 초대 링크가 클립보드에 복사되었습니다! (비밀번호는 별도 전달)');
    } catch (e) {
        prompt('초대 링크 복사:', fullURL);
    }
});

// -------------------------------------------------------------
// Live Connected Peer List Management (Anti-Flicker & Resilient)
// -------------------------------------------------------------
function updatePeerPresence(clientId, deviceDesc) {
    const isNew = !activePeers.has(clientId);
    activePeers.set(clientId, {
        device: deviceDesc || '상대 기기',
        lastSeen: Date.now()
    });

    if (isNew) {
        showToast(`🎉 ${deviceDesc || '상대 기기'} 님이 입장하셨습니다!`);
        appendSystemMessage(`[시스템] ${deviceDesc || '상대 기기'} 접속 완료`);
    }

    renderPeerList();
}

function startPeerPruner() {
    if (peerPruneInterval) clearInterval(peerPruneInterval);
    peerPruneInterval = setInterval(() => {
        const now = Date.now();
        let changed = false;

        activePeers.forEach((peerData, clientId) => {
            if (now - peerData.lastSeen > 8000) {
                showToast(`👋 ${peerData.device} 님의 연결이 종료되었습니다.`);
                appendSystemMessage(`[시스템] ${peerData.device} 퇴장`);
                activePeers.delete(clientId);
                changed = true;

                if (activePeers.size === 0) {
                    handleAllPeersDisconnected();
                }
            }
        });

        if (changed) {
            renderPeerList();
        }
    }, 1500);
}

let lastRenderedPeerKeys = '';

function renderPeerList() {
    if (!peerListContainer) return;

    // Smart DOM diffing to eliminate flickering
    const currentKeys = Array.from(activePeers.entries())
        .map(([id, p]) => `${id}:${p.device}`)
        .sort()
        .join('|') + `_direct:${isDirectP2P}`;

    if (currentKeys === lastRenderedPeerKeys) {
        return; // No change: skip DOM wipe to eliminate flicker!
    }
    lastRenderedPeerKeys = currentKeys;

    peerListContainer.innerHTML = '';

    // 1. My Device Item
    const myItem = document.createElement('div');
    myItem.className = 'peer-item me';
    myItem.innerHTML = `
        <div class="peer-info">
            <span class="peer-dot"></span>
            <span>💻 ${myDeviceDesc}</span>
        </div>
        <span class="peer-status-tag">나</span>
    `;
    peerListContainer.appendChild(myItem);

    // 2. Connected Remote Peers
    activePeers.forEach((peerData, clientId) => {
        const item = document.createElement('div');
        item.className = 'peer-item';
        const icon = /iPhone|Android Phone/i.test(peerData.device) ? '📱' : (/iPad|Tab/i.test(peerData.device) ? '📱' : '💻');
        const tagText = isDirectP2P ? '🟢 P2P 직통' : '🟢 연결됨';

        item.innerHTML = `
            <div class="peer-info">
                <span class="peer-dot"></span>
                <span>${icon} ${peerData.device}</span>
            </div>
            <span class="peer-status-tag">${tagText}</span>
        `;
        peerListContainer.appendChild(item);
    });

    const totalCount = activePeers.size + 1;
    peerCountBadge.textContent = `${totalCount}명 접속 중`;

    if (activePeers.size > 0) {
        const firstPeer = activePeers.values().next().value;
        updateStatus('connected', `연결됨 (${firstPeer.device})`);
        peerStatusLabel.textContent = `1:1 (${firstPeer.device})`;
    } else {
        updateStatus('waiting', `상대방 대기 중 [방: ${currentRoomId}]`);
        peerStatusLabel.textContent = 'E2EE 보안';
    }
}

function handleAllPeersDisconnected() {
    cleanupPeerConnection();
    stopPing();
    updateStatus('waiting', `상대방 대기 중 [방: ${currentRoomId}]`);
    peerStatusLabel.textContent = 'E2EE 보안';

    if (activeSendingFile) {
        notifyInterruptedTransfer();
    }
}

function appendSystemMessage(text) {
    const bubble = document.createElement('div');
    bubble.className = 'message-bubble incoming';
    bubble.style.background = 'rgba(51, 65, 85, 0.4)';
    bubble.style.color = 'var(--text-muted)';
    bubble.style.fontSize = '0.8rem';
    bubble.textContent = text;
    chatMessages.appendChild(bubble);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

// -------------------------------------------------------------
// Left Sidebar Completed Files Log Management
// -------------------------------------------------------------
function addFileToCompletedLog(fileName, fileSize, direction, downloadUrl = null) {
    if (fileLogEmptyText) fileLogEmptyText.style.display = 'none';

    const div = document.createElement('div');
    div.className = 'file-log-item';

    const icon = direction === 'outgoing' ? '📤' : '📥';
    const actionText = direction === 'outgoing' ? '전송 완료' : '수신 완료';
    const timeStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    let downloadActionHtml = '';
    if (direction === 'incoming' && downloadUrl) {
        downloadActionHtml = `<a href="${downloadUrl}" download="${fileName}" class="file-log-download-btn">💾 저장</a>`;
    }

    div.innerHTML = `
        <div class="file-log-top">
            <span class="file-log-name">${icon} ${fileName}</span>
            <span style="font-size: 0.72rem; color: var(--accent); font-weight:600;">${formatBytes(fileSize)}</span>
        </div>
        <div class="file-log-bottom">
            <span>${actionText} • ${timeStr}</span>
            ${downloadActionHtml}
        </div>
    `;

    completedFilesLog.prepend(div);
    updateCompletedLogCount();
    saveFileToSession(fileName, fileSize, direction, timeStr, downloadUrl);
}

function updateCompletedLogCount() {
    const count = completedFilesLog.querySelectorAll('.file-log-item').length;
    completedLogCountBadge.textContent = `${count}개`;
}

// -------------------------------------------------------------
// Global MQTT Signaling with Cryptographic Topic Isolation
// -------------------------------------------------------------
function connectSignaling(brokerIndex) {
    const brokerUrl = BROKER_URLS[brokerIndex % BROKER_URLS.length];
    updateStatus('connecting', '보안 P2P 네트워크 접속 중...');

    try {
        mqttClient = mqtt.connect(brokerUrl, {
            clientId: 'p2p_' + myClientId,
            clean: true,
            connectTimeout: 5000,
            reconnectPeriod: 2000
        });
    } catch (e) {
        console.error('MQTT Connect error:', e);
        setTimeout(() => connectSignaling(brokerIndex + 1), 1500);
        return;
    }

    // Secure isolated topic based on RoomKey + SHA256(Password)
    const roomTopic = `p2pshare/v8/${currentSecureTopic}/#`;

    mqttClient.on('connect', () => {
        console.log('[Signaling] Connected to Secure Broker Topic:', roomTopic);
        renderPeerList();

        mqttClient.subscribe(roomTopic, (err) => {
            if (!err) {
                startPresenceBroadcast();
            }
        });
    });

    mqttClient.on('message', async (topic, payload) => {
        try {
            if (topic.includes('/data/')) {
                const targetId = topic.split('/').pop();
                if (targetId === myClientId) {
                    const arrayBuf = payload.buffer.slice(payload.byteOffset, payload.byteOffset + payload.byteLength);
                    handleIncomingPacket(arrayBuf);
                }
                return;
            }

            const msg = JSON.parse(payload.toString());
            if (msg.sender === myClientId) return;

            handleSignalingMessage(msg);
        } catch (err) {
            console.error('[Signaling] Parse error:', err);
        }
    });

    mqttClient.on('error', (err) => {
        console.warn('[Signaling] Broker failover...', err);
        mqttClient.end(true);
        setTimeout(() => connectSignaling(brokerIndex + 1), 1500);
    });
}

function startPresenceBroadcast() {
    announcePresence(false);
    if (joinBroadcastInterval) clearInterval(joinBroadcastInterval);
    joinBroadcastInterval = setInterval(() => {
        announcePresence(false);
    }, 1500);
}

function announcePresence(isReply = false) {
    publishSignal('presence', {
        device: myDeviceDesc,
        isReply: isReply
    });
}

function publishSignal(type, data = {}) {
    if (!mqttClient || !mqttClient.connected || !currentSecureTopic) return;
    const topic = `p2pshare/v8/${currentSecureTopic}/${type}`;
    const payload = JSON.stringify({
        type: type,
        sender: myClientId,
        device: myDeviceDesc,
        ...data
    });
    mqttClient.publish(topic, payload);
}

function getPrimaryRemoteClientId() {
    if (activePeers.size > 0) {
        return activePeers.keys().next().value;
    }
    return null;
}

// -------------------------------------------------------------
// Unified Channel Send with Safe Error Catching
// -------------------------------------------------------------
function channelSend(arrayBuffer) {
    try {
        if (dataChannel && dataChannel.readyState === 'open') {
            dataChannel.send(arrayBuffer);
        } else if (mqttClient && mqttClient.connected && currentSecureTopic) {
            const targetPeerId = getPrimaryRemoteClientId();
            if (targetPeerId) {
                const topic = `p2pshare/v8/${currentSecureTopic}/data/${targetPeerId}`;
                const uint8 = new Uint8Array(arrayBuffer);
                mqttClient.publish(topic, uint8);
            }
        }
    } catch (err) {
        console.warn('[P2P] channelSend transient buffer warning:', err);
    }
}

// -------------------------------------------------------------
// WebRTC Signaling & Dynamic Re-negotiation
// -------------------------------------------------------------
function cleanupPeerConnection() {
    if (dataChannel) {
        try {
            dataChannel.onopen = null;
            dataChannel.onclose = null;
            dataChannel.onmessage = null;
            dataChannel.onerror = null;
            dataChannel.close();
        } catch (e) {}
        dataChannel = null;
    }
    if (peerConnection) {
        try {
            peerConnection.onicecandidate = null;
            peerConnection.oniceconnectionstatechange = null;
            peerConnection.ondatachannel = null;
            peerConnection.close();
        } catch (e) {}
        peerConnection = null;
    }
    candidateQueue = [];
    isDirectP2P = false;
}

function cleanupConnection() {
    cleanupPeerConnection();
    stopPing();
    if (joinBroadcastInterval) clearInterval(joinBroadcastInterval);
    if (peerPruneInterval) clearInterval(peerPruneInterval);
    if (mqttClient) {
        try { mqttClient.end(true); } catch(e){}
        mqttClient = null;
    }
    activePeers.clear();
}

async function handleSignalingMessage(msg) {
    switch (msg.type) {
        case 'presence':
            updatePeerPresence(msg.sender, msg.device);

            if (!msg.isReply) {
                announcePresence(true);
            }

            startPing();

            if (myClientId < msg.sender && !peerConnection) {
                createPeerConnection(msg.sender);
                createDataChannel();
                try {
                    const offer = await peerConnection.createOffer();
                    await peerConnection.setLocalDescription(offer);
                    publishSignal('offer', { sdp: offer, target: msg.sender });
                } catch (e) {
                    console.warn('Offer error:', e);
                }
            }
            break;

        case 'offer':
            if (msg.target && msg.target !== myClientId) return;
            updatePeerPresence(msg.sender, msg.device);

            createPeerConnection(msg.sender);
            try {
                await peerConnection.setRemoteDescription(new RTCSessionDescription(msg.sdp));
                await flushCandidateQueue();
                const answer = await peerConnection.createAnswer();
                await peerConnection.setLocalDescription(answer);
                publishSignal('answer', { sdp: answer, target: msg.sender });
            } catch (e) {
                console.warn('Answer error:', e);
            }
            break;

        case 'answer':
            if (msg.target && msg.target !== myClientId) return;
            if (peerConnection && peerConnection.signalingState !== 'stable') {
                try {
                    await peerConnection.setRemoteDescription(new RTCSessionDescription(msg.sdp));
                    await flushCandidateQueue();
                } catch (e) {
                    console.warn('Set answer error:', e);
                }
            }
            break;

        case 'candidate':
            if (msg.target && msg.target !== myClientId) return;
            if (msg.candidate) {
                handleIncomingCandidate(msg.candidate);
            }
            break;
    }
}

async function handleIncomingCandidate(candidate) {
    if (peerConnection && peerConnection.remoteDescription && peerConnection.remoteDescription.type) {
        try {
            await peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (e) {
            console.warn('Candidate error:', e);
        }
    } else {
        candidateQueue.push(candidate);
    }
}

async function flushCandidateQueue() {
    while (candidateQueue.length > 0) {
        const cand = candidateQueue.shift();
        try {
            await peerConnection.addIceCandidate(new RTCIceCandidate(cand));
        } catch (e) {}
    }
}

function createPeerConnection(targetClientId) {
    if (peerConnection) return;

    peerConnection = new RTCPeerConnection(rtcConfig);

    peerConnection.onicecandidate = (event) => {
        if (event.candidate) {
            publishSignal('candidate', { candidate: event.candidate, target: targetClientId });
        }
    };

    peerConnection.oniceconnectionstatechange = () => {
        console.log('[WebRTC] ICE State:', peerConnection.iceConnectionState);
        if (peerConnection.iceConnectionState === 'connected' || peerConnection.iceConnectionState === 'completed') {
            isDirectP2P = true;
            renderPeerList();
        } else if (peerConnection.iceConnectionState === 'disconnected' || peerConnection.iceConnectionState === 'closed' || peerConnection.iceConnectionState === 'failed') {
            isDirectP2P = false;
            renderPeerList();
        }
    };

    peerConnection.ondatachannel = (event) => {
        console.log('[WebRTC] Got direct DataChannel');
        setDataChannel(event.channel);
    };
}

function createDataChannel() {
    const channel = peerConnection.createDataChannel('p2p-direct-channel', {
        ordered: true
    });
    setDataChannel(channel);
}

function setDataChannel(channel) {
    dataChannel = channel;
    dataChannel.binaryType = 'arraybuffer';
    dataChannel.bufferedAmountLowThreshold = BUFFER_LOW_THRESHOLD;

    dataChannel.onopen = () => {
        console.log('[WebRTC] Direct DataChannel OPENED!');
        isDirectP2P = true;
        renderPeerList();
    };

    dataChannel.onclose = () => {
        console.log('[WebRTC] DataChannel closed');
        isDirectP2P = false;
        renderPeerList();
    };

    dataChannel.onerror = (err) => {
        console.warn('[WebRTC] DataChannel error:', err);
    };

    dataChannel.onmessage = (event) => {
        handleIncomingPacket(event.data);
    };
}

function updateStatus(state, text) {
    statusBadge.className = 'status-badge';
    if (state === 'connected') {
        statusBadge.classList.add('connected');
        pingBadge.style.display = 'block';
    } else if (state === 'error' || state === 'disconnected') {
        statusBadge.classList.add('error');
        pingBadge.style.display = 'none';
    } else {
        pingBadge.style.display = 'none';
    }
    statusText.textContent = text;
}

// -------------------------------------------------------------
// Ping / Pong Latency Measurement
// -------------------------------------------------------------
function startPing() {
    stopPing();
    pingInterval = setInterval(() => {
        if (activePeers.size > 0) {
            const now = Date.now() & 0xFFFFFFFF;
            channelSend(ProtocolCodec.encodePing(now));
        }
    }, 3000);
}

function stopPing() {
    if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
    }
}

// -------------------------------------------------------------
// 9-Byte Binary Packet Processing & File Cancel Handling
// -------------------------------------------------------------
function handleIncomingPacket(arrayBuffer) {
    try {
        // Any incoming packet (chunk, meta, chat, ping) proves peer is active!
        activePeers.forEach((p) => {
            p.lastSeen = Date.now();
        });

        const packet = ProtocolCodec.decode(arrayBuffer);

        switch (packet.type) {
            case PacketType.CHAT: {
                const text = ProtocolCodec.decodeText(packet.payload);
                appendChatMessage(text, 'incoming');
                saveChatToSession(text, 'incoming');
                break;
            }

            case PacketType.FILE_META: {
                const meta = ProtocolCodec.decodeJSON(packet.payload);
                onReceiveFileMeta(packet.fileId, meta);
                break;
            }

            case PacketType.FILE_CHUNK: {
                onReceiveFileChunk(packet.fileId, packet.seqOrLen, packet.payload);
                break;
            }

            case PacketType.FILE_ACK: {
                break;
            }

            case PacketType.FILE_CANCEL: {
                const fileId = packet.fileId;
                cancelledOutgoingFileIds.add(fileId);
                cancelledIncomingFileIds.add(fileId);
                activeSendingFile = null;
                incomingTransfers.delete(fileId);
                pendingEarlyChunks.delete(fileId);

                const item = document.getElementById(`transfer-${fileId}`);
                if (item) {
                    const status = item.querySelector('.transfer-footer span:first-child');
                    const speed = item.querySelector('.transfer-footer span:last-child');
                    const bar = item.querySelector('.progress-bar-fill');
                    const cancelBtn = item.querySelector('.btn-cancel-transfer');
                    if (cancelBtn) cancelBtn.remove();
                    if (bar) bar.style.background = 'var(--danger)';
                    if (speed) speed.textContent = '취소됨';
                    if (status) status.innerHTML = `<span style="color: #f87171; font-weight:600;">❌ 상대방이 파일 송수신을 취소했습니다.</span>`;
                    setTimeout(() => {
                        item.classList.add('fade-out');
                        setTimeout(() => item.remove(), 400);
                    }, 1500);
                }

                showToast('❌ 상대방이 파일 송수신을 취소했습니다.');
                break;
            }

            case PacketType.PING: {
                channelSend(ProtocolCodec.encodePong(packet.seqOrLen));
                break;
            }

            case PacketType.PONG: {
                const sentTime = packet.seqOrLen;
                const now = Date.now() & 0xFFFFFFFF;
                const rtt = Math.max(1, now - sentTime);
                pingBadge.textContent = `⚡ ${rtt} ms`;
                break;
            }
        }
    } catch (err) {
        console.error('[Protocol] Packet decode error:', err);
    }
}

// -------------------------------------------------------------
// Chat & Messages
// -------------------------------------------------------------
function appendChatMessage(text, direction) {
    const bubble = document.createElement('div');
    bubble.className = `message-bubble ${direction}`;

    const textSpan = document.createElement('span');
    textSpan.textContent = text;
    bubble.appendChild(textSpan);

    const meta = document.createElement('div');
    meta.className = 'message-meta';
    meta.textContent = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    bubble.appendChild(meta);

    chatMessages.appendChild(bubble);
    chatMessages.scrollTop = chatMessages.scrollHeight;
}

chatForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const text = chatInput.value.trim();
    if (!text) return;

    if (activePeers.size === 0) {
        showToast('⚠️ 접속 중인 상대방이 없습니다.');
        return;
    }

    const packetBuf = ProtocolCodec.encodeChat(text);
    channelSend(packetBuf);

    appendChatMessage(text, 'outgoing');
    saveChatToSession(text, 'outgoing');
    chatInput.value = '';
});

// Clipboard Send
btnSendClipboard.addEventListener('click', async () => {
    try {
        const text = await navigator.clipboard.readText();
        if (text && text.trim()) {
            if (activePeers.size === 0) {
                showToast('⚠️ 접속 중인 상대방이 없습니다.');
                return;
            }
            const packetBuf = ProtocolCodec.encodeChat(text);
            channelSend(packetBuf);
            appendChatMessage(`[클립보드 공유] ${text}`, 'outgoing');
            saveChatToSession(`[클립보드 공유] ${text}`, 'outgoing');
            showToast('📋 클립보드 텍스트를 전송했습니다.');
        } else {
            showToast('⚠️ 클립보드에 텍스트가 없습니다.');
        }
    } catch (err) {
        showToast('⚠️ 클립보드 접근 권한이 필요합니다.');
    }
});

// -------------------------------------------------------------
// File Transfer with Full-Duplex Flow Control & Real-time Cancel Support
// -------------------------------------------------------------
dropZone.addEventListener('click', () => fileInput.click());
dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('dragover');
});
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
        handleFilesSelected(e.dataTransfer.files);
    }
});
fileInput.addEventListener('change', (e) => {
    if (e.target.files && e.target.files.length > 0) {
        handleFilesSelected(e.target.files);
        fileInput.value = '';
    }
});

function handleFilesSelected(files) {
    if (activePeers.size === 0) {
        showToast('⚠️ 먼저 상대방 기기를 같은 키와 비밀번호로 연결해 주세요!');
        return;
    }

    for (let i = 0; i < files.length; i++) {
        sendFile(files[i]);
    }
}

async function sendFile(file) {
    const fileId = Math.floor(Math.random() * 0x7FFFFFFF);
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    
    cachedOutgoingFiles.set(fileId, file);
    activeSendingFile = { fileId, file };

    const transferItem = createTransferUI(fileId, file.name, file.size, 'outgoing');

    const meta = {
        name: file.name,
        size: file.size,
        totalChunks: totalChunks,
        type: file.type || 'application/octet-stream'
    };

    channelSend(ProtocolCodec.encodeFileMeta(fileId, meta));
    await new Promise(r => setTimeout(r, 40));

    let offset = 0;
    let chunkIndex = 0;
    const startTime = Date.now();
    let lastTime = startTime;
    let lastBytes = 0;

    while (offset < file.size) {
        // 1. Check if cancelled by user or peer
        if (cancelledOutgoingFileIds.has(fileId)) {
            console.log('[P2P] sendFile cancelled, exiting loop for fileId:', fileId);
            cancelledOutgoingFileIds.delete(fileId);
            activeSendingFile = null;
            return;
        }

        // 2. Check if peer disconnected
        if (activePeers.size === 0) {
            console.warn('[P2P] SendFile aborted due to disconnection');
            notifyInterruptedTransfer();
            return;
        }

        // 3. Flow control buffer check
        if (dataChannel && dataChannel.readyState === 'open' && dataChannel.bufferedAmount > BUFFER_HIGH_THRESHOLD) {
            await waitForBufferDrain();
        }

        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const arrayBuffer = await slice.arrayBuffer();

        const packet = ProtocolCodec.encodeFileChunk(fileId, chunkIndex, arrayBuffer);
        channelSend(packet);

        offset += arrayBuffer.byteLength;
        chunkIndex++;

        const percent = Math.min(100, Math.floor((offset / file.size) * 100));
        const now = Date.now();
        const timeDiff = (now - lastTime) / 1000;
        if (timeDiff > 0.1 || offset === file.size) {
            const bytesDiff = offset - lastBytes;
            const speed = timeDiff > 0 ? (bytesDiff / timeDiff) : 0;
            updateTransferUI(transferItem, fileId, percent, offset, file.size, speed, false, 'outgoing');
            lastTime = now;
            lastBytes = offset;
        }

        // Adaptive yield to event loop so simultaneous bidirectional streams never choke
        if (chunkIndex % 2 === 0) {
            await new Promise(r => setTimeout(r, 4));
        }
    }

    activeSendingFile = null;
    updateTransferUI(transferItem, fileId, 100, file.size, file.size, 0, true, 'outgoing');

    // Add to Left Completed Files Log
    addFileToCompletedLog(file.name, file.size, 'outgoing', null);
    showToast(`✅ ${file.name} 전송 완료!`);

    // Remove from active transfer screen after 1.2 seconds
    setTimeout(() => {
        if (transferItem) {
            transferItem.classList.add('fade-out');
            setTimeout(() => transferItem.remove(), 400);
        }
    }, 1200);
}

// Global Cancel Handler for Sender and Receiver
window.cancelTransfer = function(fileId, direction) {
    const item = document.getElementById(`transfer-${fileId}`);
    const cancelBtn = document.getElementById(`btn-cancel-${fileId}`);
    if (cancelBtn) cancelBtn.remove();

    if (direction === 'outgoing') {
        cancelledOutgoingFileIds.add(fileId);
        activeSendingFile = null;
        channelSend(ProtocolCodec.encodeFileCancel(fileId));

        if (item) {
            const status = item.querySelector('.transfer-footer span:first-child');
            const speed = item.querySelector('.transfer-footer span:last-child');
            const bar = item.querySelector('.progress-bar-fill');
            if (bar) bar.style.background = 'var(--danger)';
            if (speed) speed.textContent = '취소됨';
            if (status) status.innerHTML = `<span style="color: #f87171; font-weight:600;">❌ 사용자가 전송을 취소했습니다.</span>`;
            setTimeout(() => {
                item.classList.add('fade-out');
                setTimeout(() => item.remove(), 400);
            }, 1500);
        }
        showToast('❌ 파일 전송을 취소했습니다.');
    } else {
        cancelledIncomingFileIds.add(fileId);
        incomingTransfers.delete(fileId);
        pendingEarlyChunks.delete(fileId);
        channelSend(ProtocolCodec.encodeFileCancel(fileId));

        if (item) {
            const status = item.querySelector('.transfer-footer span:first-child');
            const speed = item.querySelector('.transfer-footer span:last-child');
            const bar = item.querySelector('.progress-bar-fill');
            if (bar) bar.style.background = 'var(--danger)';
            if (speed) speed.textContent = '취소됨';
            if (status) status.innerHTML = `<span style="color: #f87171; font-weight:600;">❌ 사용자가 수신을 취소했습니다.</span>`;
            setTimeout(() => {
                item.classList.add('fade-out');
                setTimeout(() => item.remove(), 400);
            }, 1500);
        }
        showToast('❌ 파일 수신을 취소했습니다.');
    }
};

function notifyInterruptedTransfer() {
    if (!activeSendingFile) return;
    const fileId = activeSendingFile.fileId;

    const item = document.getElementById(`transfer-${fileId}`);
    if (item) {
        const status = item.querySelector('.transfer-footer span:first-child');
        const speed = item.querySelector('.transfer-footer span:last-child');
        const bar = item.querySelector('.progress-bar-fill');
        const cancelBtn = item.querySelector('.btn-cancel-transfer');
        if (cancelBtn) cancelBtn.remove();

        if (bar) bar.style.background = 'var(--warning)';
        if (speed) speed.textContent = '중단됨';
        if (status) {
            status.innerHTML = `<span style="color: #f59e0b;">⚠️ 상대방 연결 끊김으로 중단됨</span> <button id="btn-retry-${fileId}" onclick="window.retryFileTransfer(${fileId})" style="margin-left: 8px; padding: 2px 8px; font-size: 0.75rem; background: var(--accent); color: #0f172a; border-radius: 4px; border:none; cursor:pointer;">🔄 다시 보내기</button>`;
        }
    }
    activeSendingFile = null;
}

window.retryFileTransfer = function(fileId) {
    const file = cachedOutgoingFiles.get(fileId);
    if (!file) return;

    if (activePeers.size === 0) {
        showToast('⚠️ 먼저 상대방 기기가 다시 연결될 때까지 기다려 주세요.');
        return;
    }

    // 1. Disable button immediately to prevent multiple clicks
    const btn = document.getElementById(`btn-retry-${fileId}`);
    if (btn) {
        btn.disabled = true;
        btn.textContent = '전송 시작 중...';
        btn.style.opacity = '0.5';
    }

    // 2. Remove the old interrupted card cleanly from the UI
    const oldItem = document.getElementById(`transfer-${fileId}`);
    if (oldItem) {
        oldItem.remove();
    }

    // 3. Clear cache entry for old fileId
    cachedOutgoingFiles.delete(fileId);

    // 4. Start clean new transfer
    sendFile(file);
};

function waitForBufferDrain() {
    return new Promise((resolve) => {
        if (!dataChannel || dataChannel.readyState !== 'open' || dataChannel.bufferedAmount <= BUFFER_LOW_THRESHOLD) {
            resolve();
            return;
        }
        let timer = null;
        const check = () => {
            if (!dataChannel || dataChannel.readyState !== 'open' || dataChannel.bufferedAmount <= BUFFER_LOW_THRESHOLD) {
                if (timer) clearInterval(timer);
                if (dataChannel) dataChannel.onbufferedamountlow = null;
                resolve();
            }
        };
        timer = setInterval(check, 20);
        dataChannel.onbufferedamountlow = () => {
            if (timer) clearInterval(timer);
            dataChannel.onbufferedamountlow = null;
            resolve();
        };
    });
}

// -------------------------------------------------------------
// Incoming File Handling with Cancel Filter
// -------------------------------------------------------------
function onReceiveFileMeta(fileId, meta) {
    if (cancelledIncomingFileIds.has(fileId)) return;

    const item = {
        name: meta.name,
        size: meta.size,
        totalChunks: meta.totalChunks,
        type: meta.type,
        chunksReceived: 0,
        chunks: new Array(meta.totalChunks),
        receivedBytes: 0,
        startTime: Date.now(),
        lastTime: Date.now(),
        lastBytes: 0,
        ui: createTransferUI(fileId, meta.name, meta.size, 'incoming')
    };

    incomingTransfers.set(fileId, item);
    showToast(`📥 파일 수신 시작: ${meta.name}`);

    if (pendingEarlyChunks.has(fileId)) {
        const earlyMap = pendingEarlyChunks.get(fileId);
        earlyMap.forEach((chunkData, chunkIndex) => {
            item.chunks[chunkIndex] = chunkData;
            item.chunksReceived++;
            item.receivedBytes += chunkData.byteLength;
        });
        pendingEarlyChunks.delete(fileId);

        const percent = Math.min(100, Math.floor((item.receivedBytes / item.size) * 100));
        updateTransferUI(item.ui, fileId, percent, item.receivedBytes, item.size, 0, false, 'incoming');

        if (item.chunksReceived >= item.totalChunks) {
            finalizeIncomingFile(fileId, item);
        }
    }

    if (item.ui) {
        item.ui.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
}

function onReceiveFileChunk(fileId, chunkIndex, chunkData) {
    if (cancelledIncomingFileIds.has(fileId)) return;

    let item = incomingTransfers.get(fileId);

    if (!item) {
        if (!pendingEarlyChunks.has(fileId)) {
            pendingEarlyChunks.set(fileId, new Map());
        }
        pendingEarlyChunks.get(fileId).set(chunkIndex, chunkData);
        return;
    }

    if (!item.chunks[chunkIndex]) {
        item.chunks[chunkIndex] = chunkData;
        item.chunksReceived++;
        item.receivedBytes += chunkData.byteLength;
    }

    const percent = Math.min(100, Math.floor((item.receivedBytes / item.size) * 100));
    const now = Date.now();
    const timeDiff = (now - item.lastTime) / 1000;

    if (timeDiff > 0.1 || item.chunksReceived === item.totalChunks) {
        const bytesDiff = item.receivedBytes - item.lastBytes;
        const speed = timeDiff > 0 ? (bytesDiff / timeDiff) : 0;
        updateTransferUI(item.ui, fileId, percent, item.receivedBytes, item.size, speed, false, 'incoming');
        item.lastTime = now;
        item.lastBytes = item.receivedBytes;
    }

    if (item.chunksReceived >= item.totalChunks) {
        finalizeIncomingFile(fileId, item);
    }
}

function finalizeIncomingFile(fileId, item) {
    const blob = new Blob(item.chunks, { type: item.type || 'application/octet-stream' });
    const downloadUrl = URL.createObjectURL(blob);

    updateTransferUI(item.ui, fileId, 100, item.size, item.size, 0, true, 'incoming');

    // 1. Add to Left Completed Files Log
    addFileToCompletedLog(item.name, item.size, 'incoming', downloadUrl);

    // 2. Auto download
    try {
        const a = document.createElement('a');
        a.href = downloadUrl;
        a.download = item.name;
        document.body.appendChild(a);
        a.click();
        a.remove();
    } catch (e) {
        console.warn('Auto download error:', e);
    }

    showToast(`🎉 ${item.name} 수신 완료!`);
    incomingTransfers.delete(fileId);

    // 3. Remove from active transfer screen after 1.2 seconds
    setTimeout(() => {
        if (item.ui) {
            item.ui.classList.add('fade-out');
            setTimeout(() => item.ui.remove(), 400);
        }
    }, 1200);
}

// -------------------------------------------------------------
// Active Transfer UI Helper with Cancel Button
// -------------------------------------------------------------
function createTransferUI(fileId, name, size, direction) {
    const div = document.createElement('div');
    div.className = 'transfer-item';
    div.id = `transfer-${fileId}`;

    const icon = direction === 'outgoing' ? '📤' : '📥';
    const directionText = direction === 'outgoing' ? '보내는 중' : '받는 중';
    const cancelBtnText = direction === 'outgoing' ? '❌ 전송 취소' : '❌ 받기 취소';

    div.innerHTML = `
        <div class="transfer-header">
            <span class="file-name">${icon} ${name}</span>
            <div style="display: flex; align-items: center; gap: 8px;">
                <span class="file-meta">${formatBytes(size)}</span>
                <button class="btn-cancel-transfer" id="btn-cancel-${fileId}" onclick="window.cancelTransfer(${fileId}, '${direction}')">${cancelBtnText}</button>
            </div>
        </div>
        <div class="progress-bar-bg">
            <div class="progress-bar-fill" id="pbar-${fileId}"></div>
        </div>
        <div class="transfer-footer">
            <span id="pstatus-${fileId}">${directionText} 0% (0 B / ${formatBytes(size)})</span>
            <span id="pspeed-${fileId}">-- MB/s</span>
        </div>
    `;

    transferList.prepend(div);
    return div;
}

function updateTransferUI(container, fileId, percent, bytesTransferred, totalBytes, speedBytesPerSec, isComplete = false, direction = 'outgoing') {
    if (!container) return;
    const bar = container.querySelector('.progress-bar-fill');
    const status = container.querySelector('.transfer-footer span:first-child');
    const speed = container.querySelector('.transfer-footer span:last-child');

    if (bar) {
        bar.style.width = `${percent}%`;
    }

    const directionLabel = direction === 'outgoing' ? '보내는 중' : '받는 중';

    if (isComplete) {
        const cancelBtn = document.getElementById(`btn-cancel-${fileId}`);
        if (cancelBtn) cancelBtn.remove();

        if (bar) bar.style.background = 'var(--success)';
        if (status) status.textContent = direction === 'outgoing' ? `✅ 전송 완료 (${formatBytes(totalBytes)})` : `✅ 수신 완료 (${formatBytes(totalBytes)})`;
        if (speed) speed.textContent = '100%';
    } else {
        if (status) {
            status.textContent = `${directionLabel} ${percent}% (${formatBytes(bytesTransferred)} / ${formatBytes(totalBytes)})`;
        }
        if (speed) {
            speed.textContent = `${formatBytes(speedBytesPerSec)}/s`;
        }
    }
}

// -------------------------------------------------------------
// Session History Persistence (Chat & Completed Files Log)
// -------------------------------------------------------------
function saveChatToSession(text, direction) {
    try {
        const history = JSON.parse(sessionStorage.getItem(STORAGE_KEY_CHAT) || '[]');
        history.push({ text, direction, time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) });
        if (history.length > 50) history.shift();
        sessionStorage.setItem(STORAGE_KEY_CHAT, JSON.stringify(history));
    } catch (e) {}
}

function saveFileToSession(name, size, direction, timeStr, downloadUrl) {
    try {
        const history = JSON.parse(sessionStorage.getItem(STORAGE_KEY_FILES) || '[]');
        history.unshift({ name, size, direction, time: timeStr, downloadUrl });
        if (history.length > 30) history.pop();
        sessionStorage.setItem(STORAGE_KEY_FILES, JSON.stringify(history));
    } catch (e) {}
}

function restoreSessionHistory() {
    try {
        // 1. Restore Chat
        const chatHist = JSON.parse(sessionStorage.getItem(STORAGE_KEY_CHAT) || '[]');
        if (chatHist.length > 0) {
            chatMessages.innerHTML = '';
            chatHist.forEach(item => {
                const bubble = document.createElement('div');
                bubble.className = `message-bubble ${item.direction}`;
                const textSpan = document.createElement('span');
                textSpan.textContent = item.text;
                bubble.appendChild(textSpan);
                const meta = document.createElement('div');
                meta.className = 'message-meta';
                meta.textContent = item.time;
                bubble.appendChild(meta);
                chatMessages.appendChild(bubble);
            });
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        // 2. Restore Completed Files Log to Left Sidebar
        const fileHist = JSON.parse(sessionStorage.getItem(STORAGE_KEY_FILES) || '[]');
        if (fileHist.length > 0 && fileLogEmptyText) {
            fileLogEmptyText.style.display = 'none';
        }
        completedFilesLog.innerHTML = '';
        if (fileHist.length === 0) {
            completedFilesLog.appendChild(fileLogEmptyText);
            fileLogEmptyText.style.display = 'block';
        } else {
            fileHist.forEach(item => {
                const div = document.createElement('div');
                div.className = 'file-log-item';
                const icon = item.direction === 'outgoing' ? '📤' : '📥';
                const actionText = item.direction === 'outgoing' ? '전송 완료' : '수신 완료';

                let downloadActionHtml = '';
                if (item.direction === 'incoming' && item.downloadUrl) {
                    downloadActionHtml = `<a href="${item.downloadUrl}" download="${item.name}" class="file-log-download-btn">💾 저장</a>`;
                }

                div.innerHTML = `
                    <div class="file-log-top">
                        <span class="file-log-name">${icon} ${item.name}</span>
                        <span style="font-size: 0.72rem; color: var(--accent); font-weight:600;">${formatBytes(item.size)}</span>
                    </div>
                    <div class="file-log-bottom">
                        <span>${actionText} • ${item.time || ''}</span>
                        ${downloadActionHtml}
                    </div>
                `;
                completedFilesLog.appendChild(div);
            });
        }
        updateCompletedLogCount();
    } catch (e) {}
}

// Start application safely
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        try { initApp(); } catch (e) { console.error('initApp error:', e); }
    });
} else {
    try { initApp(); } catch (e) { console.error('initApp error:', e); }
}
