/**
 * WebRTC P2P DataChannel Engine (High-Reliability Global MQTT Signaling)
 * 
 * Uses standard native RTCPeerConnection + global enterprise MQTT over WSS (EMQX & HiveMQ)
 * with robust ICE Candidate queuing and offer/answer state management.
 */

// Configuration
const CHUNK_SIZE = 32 * 1024; // 32KB chunks
const BUFFER_LOW_THRESHOLD = 128 * 1024; // 128KB buffer threshold
const BUFFER_HIGH_THRESHOLD = 512 * 1024; // 512KB pause threshold

// STUN servers for NAT Traversal
const rtcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' }
    ]
};

// Redundant Global MQTT WebSocket Brokers
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
let isInitiator = false;
let isNegotiating = false;
let isConnected = false;
let pingInterval = null;
let joinBroadcastInterval = null;
let candidateQueue = [];

// Incoming / Outgoing file tracking
const incomingTransfers = new Map();
const outgoingTransfers = new Map();

// Detect Device Information
function getDeviceDescription() {
    const ua = navigator.userAgent;
    let os = 'Unknown Device';
    if (/iPad/i.test(ua) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1)) os = 'iPad';
    else if (/iPhone/i.test(ua)) os = 'iPhone';
    else if (/Macintosh|Mac OS X/i.test(ua)) os = 'MacBook / Mac';
    else if (/Windows NT/i.test(ua)) os = 'Windows PC';
    else if (/Android/i.test(ua)) {
        os = /Tablet|SM-T|SM-X/i.test(ua) ? 'Galaxy Tab / Android Tablet' : 'Android Smartphone';
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
// Global MQTT-based WebRTC Signaling (Zero Server Infrastructure)
// -------------------------------------------------------------
function connectSignaling(brokerIndex) {
    if (isConnected) return;

    const brokerUrl = BROKER_URLS[brokerIndex % BROKER_URLS.length];
    updateStatus('connecting', 'P2P 중계망 접속 중...');

    try {
        mqttClient = mqtt.connect(brokerUrl, {
            clientId: 'p2p_' + myClientId,
            clean: true,
            connectTimeout: 6000,
            reconnectPeriod: 3000
        });
    } catch (e) {
        console.error('MQTT Connect error:', e);
        setTimeout(() => connectSignaling(brokerIndex + 1), 2000);
        return;
    }

    const roomTopic = `p2pshare/v3/${currentRoomId}/#`;

    mqttClient.on('connect', () => {
        console.log('[Signaling] Connected to MQTT Broker:', brokerUrl);
        updateStatus('waiting', '상대방 기기 연결 대기 중...');

        mqttClient.subscribe(roomTopic, (err) => {
            if (!err) {
                // Announce presence
                announcePresence();
                if (joinBroadcastInterval) clearInterval(joinBroadcastInterval);
                joinBroadcastInterval = setInterval(() => {
                    if (!isConnected && !isNegotiating) {
                        announcePresence();
                    } else if (isConnected) {
                        clearInterval(joinBroadcastInterval);
                    }
                }, 2000);
            }
        });
    });

    mqttClient.on('message', async (topic, payload) => {
        try {
            const msg = JSON.parse(payload.toString());
            if (msg.sender === myClientId) return; // Ignore self

            handleSignalingMessage(msg);
        } catch (err) {
            console.error('[Signaling] Message parse error:', err);
        }
    });

    mqttClient.on('error', (err) => {
        console.warn('[Signaling] Broker error, trying fallback...', err);
        mqttClient.end(true);
        setTimeout(() => connectSignaling(brokerIndex + 1), 1500);
    });
}

function announcePresence() {
    publishSignal('presence', {
        device: myDeviceDesc
    });
}

function publishSignal(type, data = {}) {
    if (!mqttClient || !mqttClient.connected) return;
    const topic = `p2pshare/v3/${currentRoomId}/${type}`;
    const payload = JSON.stringify({
        type: type,
        sender: myClientId,
        device: myDeviceDesc,
        ...data
    });
    mqttClient.publish(topic, payload);
}

// -------------------------------------------------------------
// WebRTC PeerConnection & State Management
// -------------------------------------------------------------
async function handleSignalingMessage(msg) {
    if (isConnected) return;

    switch (msg.type) {
        case 'presence':
            if (isNegotiating) return; // Ignore repeated presence while negotiating

            remoteClientId = msg.sender;
            remoteDeviceInfo = msg.device || '상대 기기';
            peerStatusLabel.textContent = `1:1 (${remoteDeviceInfo})`;

            // Deterministic Tie-Breaker: Smaller ClientId creates the Offer
            isInitiator = myClientId < remoteClientId;

            if (isInitiator) {
                console.log('[WebRTC] We are Initiator. Starting Offer...');
                isNegotiating = true;
                if (joinBroadcastInterval) clearInterval(joinBroadcastInterval);

                updateStatus('connecting', `${remoteDeviceInfo}와 P2P 연결 중...`);
                createPeerConnection();
                createDataChannel();

                try {
                    const offer = await peerConnection.createOffer();
                    await peerConnection.setLocalDescription(offer);
                    publishSignal('offer', { sdp: offer, target: remoteClientId });
                } catch (e) {
                    console.error('Create offer error:', e);
                    isNegotiating = false;
                }
            } else {
                console.log('[WebRTC] We are Receiver. Sending ready...');
                if (joinBroadcastInterval) clearInterval(joinBroadcastInterval);
                updateStatus('connecting', `${remoteDeviceInfo}와 연결 중...`);
                publishSignal('ready', { target: remoteClientId });
            }
            break;

        case 'ready':
            if (isInitiator && !peerConnection) {
                isNegotiating = true;
                updateStatus('connecting', `${remoteDeviceInfo}와 P2P 연결 중...`);
                createPeerConnection();
                createDataChannel();
                const offer = await peerConnection.createOffer();
                await peerConnection.setLocalDescription(offer);
                publishSignal('offer', { sdp: offer, target: remoteClientId });
            }
            break;

        case 'offer':
            if (msg.target && msg.target !== myClientId) return;
            console.log('[WebRTC] Received Offer');
            isNegotiating = true;
            if (joinBroadcastInterval) clearInterval(joinBroadcastInterval);

            remoteClientId = msg.sender;
            remoteDeviceInfo = msg.device || '상대 기기';
            updateStatus('connecting', `${remoteDeviceInfo}와 연결 중...`);

            createPeerConnection();
            try {
                await peerConnection.setRemoteDescription(new RTCSessionDescription(msg.sdp));
                await flushCandidateQueue();

                const answer = await peerConnection.createAnswer();
                await peerConnection.setLocalDescription(answer);
                publishSignal('answer', { sdp: answer, target: msg.sender });
            } catch (e) {
                console.error('Handle offer error:', e);
            }
            break;

        case 'answer':
            if (msg.target && msg.target !== myClientId) return;
            console.log('[WebRTC] Received Answer');
            if (peerConnection && peerConnection.signalingState !== 'stable') {
                try {
                    await peerConnection.setRemoteDescription(new RTCSessionDescription(msg.sdp));
                    await flushCandidateQueue();
                } catch (e) {
                    console.error('Handle answer error:', e);
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
            console.warn('[WebRTC] Add candidate warning:', e);
        }
    } else {
        // Queue candidates until remote description is set
        candidateQueue.push(candidate);
    }
}

async function flushCandidateQueue() {
    while (candidateQueue.length > 0) {
        const cand = candidateQueue.shift();
        try {
            await peerConnection.addIceCandidate(new RTCIceCandidate(cand));
        } catch (e) {
            console.warn('[WebRTC] Flush candidate warning:', e);
        }
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
            isConnected = true;
            isNegotiating = false;
        } else if (peerConnection.iceConnectionState === 'disconnected' || peerConnection.iceConnectionState === 'failed') {
            isConnected = false;
            isNegotiating = false;
            updateStatus('disconnected', 'P2P 연결 끊김 (재시도 중)');
            setTimeout(() => {
                if (!isConnected) {
                    cleanupConnection();
                    announcePresence();
                }
            }, 3000);
        }
    };

    peerConnection.ondatachannel = (event) => {
        console.log('[WebRTC] Receiver received DataChannel');
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
        console.log('[WebRTC] DataChannel OPENED!');
        isConnected = true;
        isNegotiating = false;
        updateStatus('connected', `연결됨 (${remoteDeviceInfo})`);
        peerStatusLabel.textContent = `1:1 (${remoteDeviceInfo})`;
        showToast(`🎉 ${remoteDeviceInfo}와 1:1 P2P 연결 완료!`);

        if (joinBroadcastInterval) clearInterval(joinBroadcastInterval);
        startPing();
    };

    dataChannel.onclose = () => {
        console.log('[WebRTC] DataChannel CLOSED');
        isConnected = false;
        isNegotiating = false;
        stopPing();
        updateStatus('waiting', '상대방 기기 연결 대기 중...');
        peerStatusLabel.textContent = '1:1 P2P';
        showToast('👋 상대방 기기 연결이 종료되었습니다.');
        cleanupConnection();
    };

    dataChannel.onmessage = (event) => {
        handleIncomingPacket(event.data);
    };

    dataChannel.onerror = (err) => {
        console.error('[WebRTC] Channel error:', err);
    };
}

function cleanupConnection() {
    stopPing();
    if (dataChannel) {
        dataChannel.close();
        dataChannel = null;
    }
    if (peerConnection) {
        peerConnection.close();
        peerConnection = null;
    }
    candidateQueue = [];
    isNegotiating = false;
    isConnected = false;
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
        if (dataChannel && dataChannel.readyState === 'open') {
            const now = Date.now() & 0xFFFFFFFF;
            dataChannel.send(ProtocolCodec.encodePing(now));
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
                if (dataChannel && dataChannel.readyState === 'open') {
                    dataChannel.send(ProtocolCodec.encodePong(packet.seqOrLen));
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

    if (!dataChannel || dataChannel.readyState !== 'open') {
        showToast('⚠️ 기기가 아직 연결되지 않았습니다.');
        return;
    }

    const packetBuf = ProtocolCodec.encodeChat(text);
    dataChannel.send(packetBuf);

    appendChatMessage(text, 'outgoing');
    chatInput.value = '';
});

// Clipboard Send
btnSendClipboard.addEventListener('click', async () => {
    try {
        const text = await navigator.clipboard.readText();
        if (text && text.trim()) {
            if (!dataChannel || dataChannel.readyState !== 'open') {
                showToast('⚠️ 기기가 아직 연결되지 않았습니다.');
                return;
            }
            const packetBuf = ProtocolCodec.encodeChat(text);
            dataChannel.send(packetBuf);
            appendChatMessage(`[클립보드 공유] ${text}`, 'outgoing');
            showToast('📋 클립보드 텍스트를 전송했습니다.');
        } else {
            showToast('⚠️ 클립보드에 텍스트가 없습니다.');
        }
    } catch (err) {
        showToast('⚠️ 클립보드 접근 권한이 필요합니다.');
    }
});

// -------------------------------------------------------------
// File Transfer (Streaming + Flow Control)
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
    if (!dataChannel || dataChannel.readyState !== 'open') {
        showToast('⚠️ 먼저 상대방 기기를 연결해 주세요!');
        return;
    }

    for (let i = 0; i < files.length; i++) {
        sendFile(files[i]);
    }
}

async function sendFile(file) {
    const fileId = Math.floor(Math.random() * 0xFFFFFFFF);
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    const transferItem = createTransferUI(fileId, file.name, file.size, 'outgoing');

    const meta = {
        name: file.name,
        size: file.size,
        totalChunks: totalChunks,
        type: file.type || 'application/octet-stream'
    };

    // 1. Send File Meta
    dataChannel.send(ProtocolCodec.encodeFileMeta(fileId, meta));

    // 2. Stream Chunks with Flow Control
    let offset = 0;
    let chunkIndex = 0;
    const startTime = Date.now();
    let lastTime = startTime;
    let lastBytes = 0;

    while (offset < file.size) {
        if (dataChannel.bufferedAmount > BUFFER_HIGH_THRESHOLD) {
            await waitForBufferDrain();
        }

        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const arrayBuffer = await slice.arrayBuffer();

        const packet = ProtocolCodec.encodeFileChunk(fileId, chunkIndex, arrayBuffer);
        dataChannel.send(packet);

        offset += arrayBuffer.byteLength;
        chunkIndex++;

        // Update progress UI
        const percent = Math.min(100, Math.floor((offset / file.size) * 100));
        const now = Date.now();
        const timeDiff = (now - lastTime) / 1000;
        if (timeDiff > 0.3 || offset === file.size) {
            const bytesDiff = offset - lastBytes;
            const speed = bytesDiff / timeDiff; // B/s
            updateTransferUI(transferItem, percent, offset, file.size, speed);
            lastTime = now;
            lastBytes = offset;
        }
    }

    updateTransferUI(transferItem, 100, file.size, file.size, 0, true);
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
}

function onReceiveFileChunk(fileId, chunkIndex, chunkData) {
    const item = incomingTransfers.get(fileId);
    if (!item) return;

    item.chunks[chunkIndex] = chunkData;
    item.chunksReceived++;
    item.receivedBytes += chunkData.byteLength;

    const percent = Math.min(100, Math.floor((item.receivedBytes / item.size) * 100));
    const now = Date.now();
    const timeDiff = (now - item.lastTime) / 1000;

    if (timeDiff > 0.3 || item.chunksReceived === item.totalChunks) {
        const bytesDiff = item.receivedBytes - item.lastBytes;
        const speed = bytesDiff / timeDiff;
        updateTransferUI(item.ui, percent, item.receivedBytes, item.size, speed);
        item.lastTime = now;
        item.lastBytes = item.receivedBytes;
    }

    // All chunks received -> Assemble & Auto Download
    if (item.chunksReceived >= item.totalChunks) {
        finalizeIncomingFile(fileId, item);
    }
}

function finalizeIncomingFile(fileId, item) {
    updateTransferUI(item.ui, 100, item.size, item.size, 0, true);

    const blob = new Blob(item.chunks, { type: item.type || 'application/octet-stream' });
    const downloadUrl = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = downloadUrl;
    a.download = item.name;
    document.body.appendChild(a);
    a.click();
    a.remove();

    showToast(`🎉 ${item.name} 다운로드 완료!`);
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
            <span id="pstatus-${fileId}">${directionText} (0%)</span>
            <span id="pspeed-${fileId}">-- MB/s</span>
        </div>
    `;

    transferList.prepend(div);
    return div;
}

function updateTransferUI(container, percent, bytesTransferred, totalBytes, speedBytesPerSec, isComplete = false) {
    if (!container) return;
    const bar = container.querySelector('.progress-bar-fill');
    const status = container.querySelector('.transfer-footer span:first-child');
    const speed = container.querySelector('.transfer-footer span:last-child');

    if (bar) bar.style.width = `${percent}%`;

    if (isComplete) {
        if (status) status.textContent = `완료 (${formatBytes(totalBytes)})`;
        if (speed) speed.textContent = '100%';
        if (bar) bar.style.background = 'var(--success)';
    } else {
        if (status) status.textContent = `${percent}% (${formatBytes(bytesTransferred)} / ${formatBytes(totalBytes)})`;
        if (speed) speed.textContent = `${formatBytes(speedBytesPerSec)}/s`;
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
