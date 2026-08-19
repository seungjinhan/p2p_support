/**
 * WebRTC P2P DataChannel Engine with Full Disconnection Resilience & Auto-Recovery
 * 
 * Features:
 * 1. Immediate Disconnection Detection: When either peer refreshes or closes, the remaining peer
 *    immediately resets its state, updates UI to "waiting", and broadcasts presence for instant auto-recovery.
 * 2. Instant Zero-Action Reconnection: When the refreshed peer returns, both peers reconnect in < 0.5s.
 * 3. File Transfer Interruption & 1-Click Retry: If a transfer is interrupted by a refresh,
 *    the sender retains the file in memory and shows a "🔄 다시 보내기" button.
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
let remoteClientId = null;
let remoteDeviceInfo = '상대 기기';
let isConnected = false;
let isDirectP2P = false;
let pingInterval = null;
let joinBroadcastInterval = null;
let candidateQueue = [];

// Active transfer tracker
let activeSendingFile = null; // Holds the File object if interrupted

// Incoming / Outgoing file tracking & early chunk buffering
const incomingTransfers = new Map();
const pendingEarlyChunks = new Map();
const cachedOutgoingFiles = new Map(); // fileId -> File object for retry

// History Persistence Keys
const STORAGE_KEY_CHAT = 'p2p_chat_history';
const STORAGE_KEY_FILES = 'p2p_files_history';

// Detect Device Information
function getDeviceDescription() {
    const ua = navigator.userAgent;
    let os = 'Unknown Device';
    if (/iPad/i.test(ua) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1)) os = 'iPad';
    else if (/iPhone/i.test(ua)) os = 'iPhone';
    else if (/Macintosh|Mac OS X/i.test(ua)) os = 'MacBook / Mac';
    else if (/Windows NT/i.test(ua)) os = 'Windows PC';
    else if (/Android/i.test(ua)) {
        os = /Tablet|SM-T|SM-X/i.test(ua) ? 'Galaxy Tab / Tablet' : 'Android Phone';
    } else if (/Linux/i.test(ua)) os = 'Linux PC';

    return os;
}

const myDeviceDesc = getDeviceDescription();

// UI Elements
const statusBadge = document.getElementById('statusBadge');
const statusText = document.getElementById('statusText');
const pingBadge = document.getElementById('pingBadge');
const roomCodeDisplay = document.getElementById('roomCodeDisplay');
const qrContainer = document.getElementById('qrContainer');
const myDeviceInfo = document.getElementById('myDeviceInfo');
const btnCopyLink = document.getElementById('btnCopyLink');
const joinRoomInput = document.getElementById('joinRoomInput');
const btnJoinRoom = document.getElementById('btnJoinRoom');
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

// Generate random 6-character room code
function generateRoomCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    for (let i = 0; i < 6; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

// Initialize Application
function initApp() {
    const params = new URLSearchParams(window.location.search);
    let roomParam = params.get('room');

    myDeviceInfo.textContent = `내 기기: ${myDeviceDesc}`;

    if (!roomParam || roomParam.trim() === '') {
        roomParam = generateRoomCode();
        const newUrl = `${window.location.pathname}?room=${roomParam}`;
        window.history.replaceState({}, '', newUrl);
    }

    currentRoomId = roomParam.trim().toUpperCase();
    roomCodeDisplay.textContent = currentRoomId;

    renderQRCode();
    restoreSessionHistory();
    connectSignaling(0);
}

function renderQRCode() {
    const fullURL = window.location.origin + window.location.pathname + `?room=${currentRoomId}`;
    if (window.QRCode && qrContainer) {
        new QRCode(qrContainer, fullURL, 180);
    }
}

// Copy invite link
btnCopyLink.addEventListener('click', async () => {
    const fullURL = window.location.origin + window.location.pathname + `?room=${currentRoomId}`;
    try {
        await navigator.clipboard.writeText(fullURL);
        showToast('📋 초대 링크가 클립보드에 복사되었습니다!');
    } catch (e) {
        prompt('초대 링크 복사:', fullURL);
    }
});

// Join other room
btnJoinRoom.addEventListener('click', () => {
    const code = joinRoomInput.value.trim().toUpperCase();
    if (code.length >= 4) {
        window.location.href = `${window.location.pathname}?room=${code}`;
    }
});

// -------------------------------------------------------------
// Session History Persistence (Chat & Files surviving Refresh)
// -------------------------------------------------------------
function saveChatToSession(text, direction) {
    try {
        const history = JSON.parse(sessionStorage.getItem(STORAGE_KEY_CHAT) || '[]');
        history.push({ text, direction, time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) });
        if (history.length > 50) history.shift();
        sessionStorage.setItem(STORAGE_KEY_CHAT, JSON.stringify(history));
    } catch (e) {}
}

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

function saveFileToSession(name, size, direction) {
    try {
        const history = JSON.parse(sessionStorage.getItem(STORAGE_KEY_FILES) || '[]');
        history.unshift({ name, size, icon: direction === 'incoming' ? '📥' : '📤' });
        if (history.length > 20) history.pop();
        sessionStorage.setItem(STORAGE_KEY_FILES, JSON.stringify(history));
    } catch (e) {}
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

    const roomTopic = `p2pshare/stable/${currentRoomId}/#`;

    mqttClient.on('connect', () => {
        console.log('[Signaling] Connected to Broker:', brokerUrl);
        if (!isConnected) {
            updateStatus('waiting', `상대방 대기 중 [방코드: ${currentRoomId}]`);
        }

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
        // Broadcast presence continuously so refreshed peers find us in < 1 second!
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
    const topic = `p2pshare/stable/${currentRoomId}/${type}`;
    const payload = JSON.stringify({
        type: type,
        sender: myClientId,
        device: myDeviceDesc,
        ...data
    });
    mqttClient.publish(topic, payload);
}

// -------------------------------------------------------------
// Unified Channel Send (Direct WebRTC with Instant MQTT Relay Fallback)
// -------------------------------------------------------------
function channelSend(arrayBuffer) {
    if (dataChannel && dataChannel.readyState === 'open') {
        dataChannel.send(arrayBuffer);
    } else if (mqttClient && mqttClient.connected && remoteClientId) {
        const topic = `p2pshare/stable/${currentRoomId}/data/${remoteClientId}`;
        const uint8 = new Uint8Array(arrayBuffer);
        mqttClient.publish(topic, uint8);
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

function handlePeerDisconnection() {
    console.log('[P2P] Peer disconnected, resetting state to waiting...');
    isConnected = false;
    isDirectP2P = false;
    cleanupPeerConnection();
    stopPing();

    updateStatus('waiting', `상대방 재연결 대기 중... [방: ${currentRoomId}]`);
    peerStatusLabel.textContent = '1:1 P2P';

    if (activeSendingFile) {
        showToast('⚠️ 상대방이 새로고침하여 전송이 중단되었습니다.');
        notifyInterruptedTransfer();
    } else {
        showToast('👋 상대방 연결이 끊어졌습니다. 자동 재연결 대기 중...');
    }

    // Ensure presence broadcast is active for instant reconnection
    startPresenceBroadcast();
}

function notifyInterruptedTransfer() {
    if (!activeSendingFile) return;
    const fileId = activeSendingFile.fileId;
    const fileObj = activeSendingFile.file;

    const item = document.getElementById(`transfer-${fileId}`);
    if (item) {
        const status = item.querySelector('.transfer-footer span:first-child');
        const speed = item.querySelector('.transfer-footer span:last-child');
        const bar = item.querySelector('.progress-bar-fill');

        if (bar) bar.style.background = 'var(--warning)';
        if (speed) speed.textContent = '중단됨';
        if (status) {
            status.innerHTML = `<span style="color: #f59e0b;">⚠️ 상대방 새로고침으로 중단됨</span> <button onclick="window.retryFileTransfer(${fileId})" style="margin-left: 8px; padding: 2px 8px; font-size: 0.75rem; background: var(--accent); color: #0f172a; border-radius: 4px; border:none; cursor:pointer;">🔄 다시 보내기</button>`;
        }
    }
    activeSendingFile = null;
}

window.retryFileTransfer = function(fileId) {
    const file = cachedOutgoingFiles.get(fileId);
    if (file) {
        if (!isConnected) {
            showToast('⚠️ 먼저 상대방 기기가 다시 연결될 때까지 잠시 기다려 주세요.');
            return;
        }
        sendFile(file);
    }
};

async function handleSignalingMessage(msg) {
    switch (msg.type) {
        case 'presence':
            // If peer refreshed or reconnected with new session ID, clean up old dead peer connection
            if (remoteClientId !== msg.sender || !peerConnection || peerConnection.signalingState === 'closed') {
                cleanupPeerConnection();
            }

            remoteClientId = msg.sender;
            remoteDeviceInfo = msg.device || '상대 기기';
            peerStatusLabel.textContent = `1:1 (${remoteDeviceInfo})`;

            // Reply immediately so newly joined peer knows we are online
            if (!msg.isReply) {
                announcePresence(true);
            }

            if (!isConnected) {
                isConnected = true;
                updateStatus('connected', `연결됨 (${remoteDeviceInfo})`);
                showToast(`🎉 ${remoteDeviceInfo}와 1:1 연결 완료!`);
                startPing();
            }

            // Initiate WebRTC upgrade
            if (myClientId < remoteClientId && !peerConnection) {
                createPeerConnection();
                createDataChannel();
                try {
                    const offer = await peerConnection.createOffer();
                    await peerConnection.setLocalDescription(offer);
                    publishSignal('offer', { sdp: offer, target: remoteClientId });
                } catch (e) {
                    console.warn('Offer error:', e);
                }
            }
            break;

        case 'offer':
            if (msg.target && msg.target !== myClientId) return;
            if (remoteClientId !== msg.sender) {
                cleanupPeerConnection();
            }

            remoteClientId = msg.sender;
            remoteDeviceInfo = msg.device || '상대 기기';

            isConnected = true;
            updateStatus('connected', `연결됨 (${remoteDeviceInfo})`);
            startPing();

            createPeerConnection();
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

function createPeerConnection() {
    if (peerConnection) return;

    peerConnection = new RTCPeerConnection(rtcConfig);

    peerConnection.onicecandidate = (event) => {
        if (event.candidate) {
            publishSignal('candidate', { candidate: event.candidate, target: remoteClientId });
        }
    };

    peerConnection.oniceconnectionstatechange = () => {
        console.log('[WebRTC] ICE State:', peerConnection.iceConnectionState);
        if (peerConnection.iceConnectionState === 'connected' || peerConnection.iceConnectionState === 'completed') {
            isDirectP2P = true;
            updateStatus('connected', `연결됨 (P2P 직통 • ${remoteDeviceInfo})`);
        } else if (peerConnection.iceConnectionState === 'disconnected' || peerConnection.iceConnectionState === 'closed' || peerConnection.iceConnectionState === 'failed') {
            handlePeerDisconnection();
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
        updateStatus('connected', `연결됨 (P2P 직통 • ${remoteDeviceInfo})`);
    };

    dataChannel.onclose = () => {
        console.log('[WebRTC] DataChannel closed');
        handlePeerDisconnection();
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
        if (isConnected) {
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
                if (isConnected) {
                    channelSend(ProtocolCodec.encodePong(packet.seqOrLen));
                }
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

    if (!isConnected) {
        showToast('⚠️ 기기가 아직 연결되지 않았습니다.');
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
            if (!isConnected) {
                showToast('⚠️ 기기가 아직 연결되지 않았습니다.');
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
    if (!isConnected) {
        showToast('⚠️ 먼저 상대방 기기를 연결해 주세요!');
        return;
    }

    for (let i = 0; i < files.length; i++) {
        sendFile(files[i]);
    }
}

async function sendFile(file) {
    const fileId = Math.floor(Math.random() * 0x7FFFFFFF);
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    
    // Cache for 1-click retry
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
        // Abort if peer disconnected
        if (!isConnected) {
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

function waitForBufferDrain() {
    return new Promise((resolve) => {
        dataChannel.onbufferedamountlow = () => {
            dataChannel.onbufferedamountlow = null;
            resolve();
        };
    });
}

// -------------------------------------------------------------
// Incoming File Handling with Early Chunk Queuing
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
        console.warn('Auto download error, fallback to click:', e);
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

// Start application safely
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        try { initApp(); } catch (e) { console.error('initApp error:', e); }
    });
} else {
    try { initApp(); } catch (e) { console.error('initApp error:', e); }
}
