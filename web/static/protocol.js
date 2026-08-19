/**
 * 9-Byte Binary Protocol Codec for WebRTC DataChannel
 * 
 * Header layout (9 bytes, Big-Endian):
 * [0]   : Type (uint8)
 * [1:5] : FileID (uint32)
 * [5:9] : SeqOrLen (uint32)
 * [9:]  : Payload (Uint8Array)
 */

const PacketType = {
    CHAT: 0x01,        // 텍스트 채팅 메시지
    FILE_META: 0x02,   // 파일 메타데이터 (JSON: name, size, totalChunks, checksum)
    FILE_CHUNK: 0x03,  // 파일 바이너리 청크
    FILE_ACK: 0x04,    // 청크 수신 확인 (ACK)
    FILE_CANCEL: 0x05, // 파일 전송 취소
    PING: 0x06,        // Ping 레이턴시 측정
    PONG: 0x07         // Pong 응답
};

const HEADER_SIZE = 9;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

class ProtocolCodec {
    /**
     * Decode an ArrayBuffer into a structured Packet object
     * @param {ArrayBuffer} buffer 
     * @returns {{type: number, fileId: number, seqOrLen: number, payload: Uint8Array}}
     */
    static decode(buffer) {
        if (!buffer || buffer.byteLength < HEADER_SIZE) {
            throw new Error(`Buffer too short for header: ${buffer ? buffer.byteLength : 0} bytes`);
        }

        const view = new DataView(buffer);
        const type = view.getUint8(0);
        const fileId = view.getUint32(1, false); // Big-Endian
        const seqOrLen = view.getUint32(5, false); // Big-Endian

        const payload = new Uint8Array(buffer, HEADER_SIZE);

        return {
            type,
            fileId,
            seqOrLen,
            payload
        };
    }

    /**
     * Encode text chat message
     */
    static encodeChat(text) {
        const payload = textEncoder.encode(text);
        const buffer = new ArrayBuffer(HEADER_SIZE + payload.byteLength);
        const view = new DataView(buffer);

        view.setUint8(0, PacketType.CHAT);
        view.setUint32(1, 0, false);
        view.setUint32(5, payload.byteLength, false);

        new Uint8Array(buffer, HEADER_SIZE).set(payload);
        return buffer;
    }

    /**
     * Encode file metadata object
     */
    static encodeFileMeta(fileId, metaObj) {
        const jsonStr = JSON.stringify(metaObj);
        const payload = textEncoder.encode(jsonStr);
        const buffer = new ArrayBuffer(HEADER_SIZE + payload.byteLength);
        const view = new DataView(buffer);

        view.setUint8(0, PacketType.FILE_META);
        view.setUint32(1, fileId, false);
        view.setUint32(5, payload.byteLength, false);

        new Uint8Array(buffer, HEADER_SIZE).set(payload);
        return buffer;
    }

    /**
     * Encode file chunk slice
     */
    static encodeFileChunk(fileId, chunkIndex, chunkData) {
        const chunkBytes = chunkData instanceof Uint8Array ? chunkData : new Uint8Array(chunkData);
        const buffer = new ArrayBuffer(HEADER_SIZE + chunkBytes.byteLength);
        const view = new DataView(buffer);

        view.setUint8(0, PacketType.FILE_CHUNK);
        view.setUint32(1, fileId, false);
        view.setUint32(5, chunkIndex, false);

        new Uint8Array(buffer, HEADER_SIZE).set(chunkBytes);
        return buffer;
    }

    /**
     * Encode File ACK
     */
    static encodeFileAck(fileId, chunkIndex) {
        const buffer = new ArrayBuffer(HEADER_SIZE);
        const view = new DataView(buffer);

        view.setUint8(0, PacketType.FILE_ACK);
        view.setUint32(1, fileId, false);
        view.setUint32(5, chunkIndex, false);

        return buffer;
    }

    /**
     * Encode File Cancel
     */
    static encodeFileCancel(fileId) {
        const buffer = new ArrayBuffer(HEADER_SIZE);
        const view = new DataView(buffer);

        view.setUint8(0, PacketType.FILE_CANCEL);
        view.setUint32(1, fileId, false);
        view.setUint32(5, 0, false);

        return buffer;
    }

    /**
     * Encode Ping
     */
    static encodePing(timestamp) {
        const buffer = new ArrayBuffer(HEADER_SIZE);
        const view = new DataView(buffer);

        view.setUint8(0, PacketType.PING);
        view.setUint32(1, 0, false);
        view.setUint32(5, timestamp & 0xFFFFFFFF, false);

        return buffer;
    }

    /**
     * Encode Pong
     */
    static encodePong(timestamp) {
        const buffer = new ArrayBuffer(HEADER_SIZE);
        const view = new DataView(buffer);

        view.setUint8(0, PacketType.PONG);
        view.setUint32(1, 0, false);
        view.setUint32(5, timestamp & 0xFFFFFFFF, false);

        return buffer;
    }

    /**
     * Helper to decode text payload
     */
    static decodeText(payload) {
        return textDecoder.decode(payload);
    }

    /**
     * Helper to decode JSON payload
     */
    static decodeJSON(payload) {
        const str = textDecoder.decode(payload);
        return JSON.parse(str);
    }
}

// Export for module/browser
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { PacketType, ProtocolCodec };
}
