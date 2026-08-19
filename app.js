/**
 * WebRTC P2P DataChannel Engine with 6-Digit Key Lobby & Live Connected Peer List
 */

// Configuration
const CHUNK_SIZE = 32 * 1024; // 32KB chunks
const BUFFER_LOW_THRESHOLD = 128 * 1024;
const BUFFER_HIGH_THRESHOLD = 512 * 1024;

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
let myClientId = 'peer-' + Math.random().toString(36).substring(2, 9);
let isDirectP2P = false;
let pingInterval = null;
let joinBroadcastInterval = null;
let peerPruneInterval = null;
let candidateQueue = [];

// Live Connected Peers Map: clientId -> { device, lastSeen, isDirectP2P }
const activePeers = new Map();

// Active file transfer tracking
let activeSendingFile = null;
const incomingTransfers = new Map();
const pendingEarlyChunks = new Map();
const cachedOutgoingFiles = new Map();

// History Persistence Keys
const STORAGE_KEY_CHAT = 'p2p_chat_history';
const STORAGE_KEY_FILES = 'p2p_files_history';

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
const btnGenerateKey = document.getElementById('btnGenerateKey');
const btnEnterRoom = document.getElementById('btnEnterRoom');
const roomCodeDisplay = document.getElementById('roomCodeDisplay');
const qrContainer = document.getElementById('qrContainer');
const btnCopyLink = document.getElementById('btnCopyLink');
const btnLeaveRoom = document.getElementById('btnLeaveRoom');
const peerListContainer = document.getElementById('peerListContainer');
const peerCountBadge = document.getElementById('peerCountBadge');
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');
const transferList = document.getElementById('transferList');
const chatMessages = document.getElementById('chatMessages');
const chatForm = document.getElementById('chatForm');
const chatInput = document.getElementById('chatInput');
const btnSendClipboard = document.getElementById('btnSendClipboard');
const peerStatusLabel = document.getElementById('peerStatusLabel');

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
// Lobby & Initialization Flow
// -------------------------------------------------------------
function initApp() {
    const params = new URLSearchParams(window.location.search);
    const roomParam = params.get('room');

    if (roomParam && roomParam.trim().length >= 4) {
        // Direct link / QR scan entry: Auto-fill and enter room immediately!
        const key = roomParam.trim().toUpperCase();
        lobbyKeyInput.value = key;
        enterRoom(key);
    } else {
        // Show Lobby view to require entering/generating key
        showLobbyView();
    }
}

function showLobbyView() {
    lobbyView.style.display = 'flex';
    mainAppView.style.display = 'none';
    statusText.textContent = '보안 키 입력 대기 중...';
    statusBadge.className = 'status-badge';
    pingBadge.style.display = 'none';
    cleanupConnection();
}

function enterRoom(key) {
    if (!key || key.length < 4) {
        showToast('⚠️ 4~6자리의 보안 키를 입력해 주세요.');
        return;
    }

    currentRoomId = key.toUpperCase();
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

// Key Input auto-formatting
lobbyKeyInput.addEventListener('input', (e) => {
    e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
});

lobbyKeyInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        const key = lobbyKeyInput.value.trim().toUpperCase();
        if (key.length >= 4) enterRoom(key);
    }
});

btnGenerateKey.addEventListener('click', () => {
    const code = generateRoomCode();
    lobbyKeyInput.value = code;
    lobbyKeyInput.focus();
    showToast(`🎲 보안 키 [${code}] 생성 완료!`);
});

btnEnterRoom.addEventListener('click', () => {
    const key = lobbyKeyInput.value.trim().toUpperCase();
    enterRoom(key);
});

btnLeaveRoom.addEventListener('click', () => {
    if (confirm('보안 방에서 나가시겠습니까?')) {
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
        showToast('📋 초대 링크가 클립보드에 복사되었습니다!');
    } catch (e) {
        prompt('초대 링크 복사:', fullURL);
    }
});

// -------------------------------------------------------------
// Live Connected Peer List Management
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
            // If peer hasn't sent heartbeat in 3.5 seconds, consider disconnected
            if (now - peerData.lastSeen > 3500) {
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
    }, 1000);
}

function renderPeerList() {
    if (!peerListContainer) return;
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
        updateStatus('waiting', `상대방 대기 중 [키: ${currentRoomId}]`);
        peerStatusLabel.textContent = 'E2EE 보안';
    }
}

function handleAllPeersDisconnected() {
    cleanupPeerConnection();
    stopPing();
    updateStatus('waiting', `상대방 대기 중 [키: ${currentRoomId}]`);
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
// Global MQTT Signaling with Auto-Reconnection & Mutual Handshake
// -------------------------------------------------------------
function connectSignaling(brokerIndex) {
    const brokerUrl = BROKER_URLS[brokerIndex % BROKER_URLS.length];
    updateStatus('connecting', 'P2P 네트워크 접속 중...');

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

    const roomTopic = `p2pshare/v7/${currentRoomId}/#`;

    mqttClient.on('connect', () => {
        console.log('[Signaling] Connected to Broker:', brokerUrl);
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
    }, 1200);
}

function announcePresence(isReply = false) {
    publishSignal('presence', {
        device: myDeviceDesc,
        isReply: isReply
    });
}

function publishSignal(type, data = {}) {
    if (!mqttClient || !mqttClient.connected) return;
    const topic = `p2pshare/v7/${currentRoomId}/${type}`;
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
// Unified Channel Send
// -------------------------------------------------------------
function channelSend(arrayBuffer) {
    if (dataChannel && dataChannel.readyState === 'open') {
        dataChannel.send(arrayBuffer);
    } else if (mqttClient && mqttClient.connected) {
        const targetPeerId = getPrimaryRemoteClientId();
        if (targetPeerId) {
            const topic = `p2pshare/v7/${currentRoomId}/data/${targetPeerId}`;
            const uint8 = new Uint8Array(arrayBuffer);
            mqttClient.publish(topic, uint8);
        }
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

            // Mutual handshake reply
            if (!msg.isReply) {
                announcePresence(true);
            }

            startPing();

            // Initiate WebRTC upgrade if smaller ClientId
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
// 9-Byte Binary Packet Processing
// -------------------------------------------------------------
function handleIncomingPacket(arrayBuffer) {
    try {
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
                showToast('❌ 파일 전송이 취소되었습니다.');
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
// File Transfer (Streaming + Flow Control + 1-Click Retry)
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
        showToast('⚠️ 먼저 상대방 기기를 같은 키로 연결해 주세요!');
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
        if (activePeers.size === 0) {
            console.warn('[P2P] SendFile aborted due to disconnection');
            notifyInterruptedTransfer();
            return;
        }

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
            updateTransferUI(transferItem, percent, offset, file.size, speed, false, null, 'outgoing');
            lastTime = now;
            lastBytes = offset;
        }

        if (chunkIndex % 3 === 0) {
            await new Promise(r => setTimeout(r, 4));
        }
    }

    activeSendingFile = null;
    updateTransferUI(transferItem, 100, file.size, file.size, 0, true, null, 'outgoing');
    saveFileToSession(file.name, file.size, 'outgoing');
    showToast(`✅ ${file.name} 전송 완료!`);
}

function notifyInterruptedTransfer() {
    if (!activeSendingFile) return;
    const fileId = activeSendingFile.fileId;

    const item = document.getElementById(`transfer-${fileId}`);
    if (item) {
        const status = item.querySelector('.transfer-footer span:first-child');
        const speed = item.querySelector('.transfer-footer span:last-child');
        const bar = item.querySelector('.progress-bar-fill');

        if (bar) bar.style.background = 'var(--warning)';
        if (speed) speed.textContent = '중단됨';
        if (status) {
            status.innerHTML = `<span style="color: #f59e0b;">⚠️ 상대방 연결 끊김으로 중단됨</span> <button onclick="window.retryFileTransfer(${fileId})" style="margin-left: 8px; padding: 2px 8px; font-size: 0.75rem; background: var(--accent); color: #0f172a; border-radius: 4px; border:none; cursor:pointer;">🔄 다시 보내기</button>`;
        }
    }
    activeSendingFile = null;
}

window.retryFileTransfer = function(fileId) {
    const file = cachedOutgoingFiles.get(fileId);
    if (file) {
        if (activePeers.size === 0) {
            showToast('⚠️ 먼저 상대방 기기가 다시 연결될 때까지 기다려 주세요.');
            return;
        }
        sendFile(file);
    }
};

function waitForBufferDrain() {
    return new Promise((resolve) => {
        dataChannel.onbufferedamountlow = () => {
            dataChannel.onbufferedamountlow = null;
            resolve();
        };
    });
}

// -------------------------------------------------------------
// Incoming File Handling
// -------------------------------------------------------------
function onReceiveFileMeta(fileId, meta) {
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
        updateTransferUI(item.ui, percent, item.receivedBytes, item.size, 0, false, null, 'incoming');

        if (item.chunksReceived >= item.totalChunks) {
            finalizeIncomingFile(fileId, item);
        }
    }

    if (item.ui) {
        item.ui.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
}

function onReceiveFileChunk(fileId, chunkIndex, chunkData) {
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
        updateTransferUI(item.ui, percent, item.receivedBytes, item.size, speed, false, null, 'incoming');
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

    updateTransferUI(item.ui, 100, item.size, item.size, 0, true, downloadUrl, 'incoming', item.name);
    saveFileToSession(item.name, item.size, 'incoming');

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
}

// -------------------------------------------------------------
// Transfer UI Helper
// -------------------------------------------------------------
function createTransferUI(fileId, name, size, direction) {
    const div = document.createElement('div');
    div.className = 'transfer-item';
    div.id = `transfer-${fileId}`;

    const icon = direction === 'outgoing' ? '📤' : '📥';
    const directionText = direction === 'outgoing' ? '보내는 중' : '받는 중';

    div.innerHTML = `
        <div class="transfer-header">
            <span class="file-name">${icon} ${name}</span>
            <span class="file-meta">${formatBytes(size)}</span>
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

function updateTransferUI(container, percent, bytesTransferred, totalBytes, speedBytesPerSec, isComplete = false, downloadUrl = null, direction = 'outgoing', fileName = '') {
    if (!container) return;
    const bar = container.querySelector('.progress-bar-fill');
    const status = container.querySelector('.transfer-footer span:first-child');
    const speed = container.querySelector('.transfer-footer span:last-child');

    if (bar) {
        bar.style.width = `${percent}%`;
    }

    const directionLabel = direction === 'outgoing' ? '보내는 중' : '받는 중';

    if (isComplete) {
        if (bar) bar.style.background = 'var(--success)';
        if (direction === 'incoming' && downloadUrl) {
            status.innerHTML = `✅ 수신 완료 (${formatBytes(totalBytes)}) <a href="${downloadUrl}" download="${fileName}" style="margin-left: 8px; color: #38bdf8; text-decoration: underline; font-weight: bold;">[💾 파일 다시 저장]</a>`;
        } else {
            status.textContent = `✅ 전송 완료 (${formatBytes(totalBytes)})`;
        }
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
// Session History Persistence
// -------------------------------------------------------------
function restoreSessionHistory() {
    try {
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

        const fileHist = JSON.parse(sessionStorage.getItem(STORAGE_KEY_FILES) || '[]');
        fileHist.forEach(item => {
            const div = document.createElement('div');
            div.className = 'transfer-item';
            div.innerHTML = `
                <div class="transfer-header">
                    <span class="file-name">${item.icon} ${item.name}</span>
                    <span class="file-meta">${formatBytes(item.size)}</span>
                </div>
                <div class="progress-bar-bg">
                    <div class="progress-bar-fill" style="width: 100%; background: var(--success);"></div>
                </div>
                <div class="transfer-footer">
                    <span>✅ 완료 (${formatBytes(item.size)})</span>
                    <span>100%</span>
                </div>
            `;
            transferList.appendChild(div);
        });
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
