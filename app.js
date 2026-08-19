/**
 * WebRTC P2P DataChannel Application (GitHub Pages Serverless Edition)
 * High-performance file transfer & real-time messaging using 9-byte binary headers.
 * 
 * Auto-negotiating symmetric peer discovery:
 * Automatically determines Host vs Guest role without role mismatch errors.
 */

// Configuration
const CHUNK_SIZE = 32 * 1024; // 32KB chunks
const BUFFER_LOW_THRESHOLD = 128 * 1024; // 128KB buffer threshold
const BUFFER_HIGH_THRESHOLD = 512 * 1024; // 512KB pause threshold
const PEER_PREFIX = 'p2p-v2-';

const rtcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
        { urls: 'stun:stun.cloudflare.com:3478' },
        { urls: 'stun:global.stun.twilio.com:3478' }
    ]
};

// State
let peer = null;
let activeConnection = null;
let currentRoomId = '';
let isHost = false;
let pingInterval = null;
let remoteDeviceInfo = '상대방';
let retryCount = 0;
const MAX_RETRIES = 6;

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
    startP2PSession(currentRoomId);
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
// Symmetric P2P Auto-Discovery Engine
// -------------------------------------------------------------
function startP2PSession(roomId) {
    const hostPeerId = `${PEER_PREFIX}${roomId}-host`;
    const guestPeerId = `${PEER_PREFIX}${roomId}-guest-${Math.random().toString(36).substring(2, 7)}`;

    updateStatus('connecting', 'P2P 네트워크 준비 중...');

    // 1. Try to initialize as Host first
    try {
        peer = new Peer(hostPeerId, {
            config: rtcConfig,
            debug: 1
        });
    } catch (err) {
        console.error('Peer init error:', err);
        updateStatus('error', '초기화 실패');
        return;
    }

    peer.on('open', (id) => {
        console.log('[P2P] Registered as Room Host:', id);
        isHost = true;
        updateStatus('waiting', '상대방 연결 대기 중...');
    });

    peer.on('connection', (conn) => {
        console.log('[P2P Host] Guest connected!');
        setupDataConnection(conn);
    });

    peer.on('error', (err) => {
        console.warn('[P2P] Event error:', err.type, err);

        if (err.type === 'unavailable-id') {
            // Host is ALREADY active! We become the Guest!
            console.log('[P2P] Host exists, switching to Guest mode...');
            if (peer) {
                peer.destroy();
                peer = null;
            }
            connectAsGuest(guestPeerId, hostPeerId);
        } else if (err.type === 'peer-unavailable') {
            // Target host not ready yet -> retry
            if (retryCount < MAX_RETRIES) {
                retryCount++;
                updateStatus('connecting', `상대방 검색 중 (${retryCount}/${MAX_RETRIES})...`);
                setTimeout(() => {
                    if (!activeConnection || !activeConnection.open) {
                        connectAsGuest(guestPeerId, hostPeerId);
                    }
                }, 1500);
            } else {
                updateStatus('error', '상대방 기기를 찾을 수 없습니다. PC 화면을 켜둔 상태에서 다시 시도해 주세요.');
            }
        } else {
            updateStatus('error', '연결 오류: ' + (err.message || err.type));
        }
    });
}

function connectAsGuest(guestPeerId, hostPeerId) {
    updateStatus('connecting', '호스트 기기에 연결 중...');

    try {
        peer = new Peer(guestPeerId, {
            config: rtcConfig,
            debug: 1
        });
    } catch (err) {
        console.error('Guest peer init error:', err);
        return;
    }

    peer.on('open', (id) => {
        console.log('[P2P Guest] Registered with ID:', id, 'Connecting to:', hostPeerId);
        const conn = peer.connect(hostPeerId, {
            metadata: { device: myDeviceDesc },
            reliable: true
        });
        setupDataConnection(conn);
    });

    peer.on('error', (err) => {
        console.warn('[P2P Guest] Error:', err.type, err);
        if (err.type === 'peer-unavailable' && retryCount < MAX_RETRIES) {
            retryCount++;
            updateStatus('connecting', `연결 시도 중 (${retryCount}/${MAX_RETRIES})...`);
            setTimeout(() => {
                if (!activeConnection || !activeConnection.open) {
                    if (peer) peer.destroy();
                    connectAsGuest(guestPeerId, hostPeerId);
                }
            }, 1500);
        } else {
            updateStatus('error', '호스트 연결 실패. PC 화면의 QR 코드가 켜져 있는지 확인해 주세요.');
        }
    });
}

function setupDataConnection(conn) {
    activeConnection = conn;

    conn.on('open', () => {
        console.log('[WebRTC] DataChannel OPENED!');
        if (conn.dataChannel) {
            conn.dataChannel.binaryType = 'arraybuffer';
            conn.dataChannel.bufferedAmountLowThreshold = BUFFER_LOW_THRESHOLD;
        }

        remoteDeviceInfo = (conn.metadata && conn.metadata.device) ? conn.metadata.device : '상대 기기';
        updateStatus('connected', `연결됨 (${remoteDeviceInfo})`);
        peerStatusLabel.textContent = `1:1 (${remoteDeviceInfo})`;
        showToast(`🎉 ${remoteDeviceInfo}와 1:1 P2P 연결 완료!`);

        // Send our device info greeting
        conn.send(ProtocolCodec.encodeChat(`[시스템] ${myDeviceDesc} 연결 완료`));
        startPing();
    });

    conn.on('data', (data) => {
        if (data instanceof ArrayBuffer) {
            handleIncomingPacket(data);
        } else if (typeof data === 'string') {
            appendChatMessage(data, 'incoming');
        }
    });

    conn.on('close', () => {
        console.log('[WebRTC] DataChannel CLOSED');
        stopPing();
        updateStatus('waiting', '상대방 연결 대기 중...');
        peerStatusLabel.textContent = '1:1 P2P';
        showToast('👋 상대방 연결이 종료되었습니다.');
    });

    conn.on('error', (err) => {
        console.error('[WebRTC] Channel error:', err);
    });
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
        if (activeConnection && activeConnection.open) {
            const now = Date.now() & 0xFFFFFFFF;
            activeConnection.send(ProtocolCodec.encodePing(now));
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
                if (activeConnection && activeConnection.open) {
                    activeConnection.send(ProtocolCodec.encodePong(packet.seqOrLen));
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

    if (!activeConnection || !activeConnection.open) {
        showToast('⚠️ 기기가 아직 연결되지 않았습니다.');
        return;
    }

    const packetBuf = ProtocolCodec.encodeChat(text);
    activeConnection.send(packetBuf);

    appendChatMessage(text, 'outgoing');
    chatInput.value = '';
});

// Clipboard Send
btnSendClipboard.addEventListener('click', async () => {
    try {
        const text = await navigator.clipboard.readText();
        if (text && text.trim()) {
            if (!activeConnection || !activeConnection.open) {
                showToast('⚠️ 기기가 아직 연결되지 않았습니다.');
                return;
            }
            const packetBuf = ProtocolCodec.encodeChat(text);
            activeConnection.send(packetBuf);
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
    if (!activeConnection || !activeConnection.open) {
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
    activeConnection.send(ProtocolCodec.encodeFileMeta(fileId, meta));

    // 2. Stream Chunks with Flow Control
    let offset = 0;
    let chunkIndex = 0;
    const startTime = Date.now();
    let lastTime = startTime;
    let lastBytes = 0;

    const dc = activeConnection.dataChannel;

    while (offset < file.size) {
        // Flow Control: Pause if WebRTC buffer is high
        if (dc && dc.bufferedAmount > BUFFER_HIGH_THRESHOLD) {
            await waitForBufferDrain(dc);
        }

        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const arrayBuffer = await slice.arrayBuffer();

        const packet = ProtocolCodec.encodeFileChunk(fileId, chunkIndex, arrayBuffer);
        activeConnection.send(packet);

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

function waitForBufferDrain(dc) {
    return new Promise((resolve) => {
        dc.onbufferedamountlow = () => {
            dc.onbufferedamountlow = null;
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
