class BetterStruct():
    """!
    @brief Minimal binary reader/writer for WSMessage frames
    """

    def __init__(self, buffer=None):
        self._buffer = buffer if (buffer is not None) else b""
        self._pos = 0

    def _ensure(self, size: int):
        if (self._pos + size > len(self._buffer)):
            raise ValueError("Malformed frame")

    def add_string(self, string: str):
        encoded = str(string).encode("utf-8")
        self._buffer += len(encoded).to_bytes(4, "little", signed=False)
        self._buffer += encoded
        self._pos += 4 + len(encoded)

    def get_string(self) -> str:
        self._ensure(4)
        length = int.from_bytes(self._buffer[self._pos:self._pos + 4], "little", signed=False)
        self._pos += 4
        self._ensure(length)
        result = self._buffer[self._pos:self._pos + length].decode("utf-8")
        self._pos += length
        return result

    def add_bytes(self, data: bytes):
        if (type(data) not in (bytes, bytearray)):
            raise TypeError("payload must be bytes")
        data = bytes(data)
        self._buffer += len(data).to_bytes(4, "little", signed=False)
        self._buffer += data
        self._pos += 4 + len(data)

    def get_bytes(self) -> bytes:
        self._ensure(4)
        length = int.from_bytes(self._buffer[self._pos:self._pos + 4], "little", signed=False)
        self._pos += 4
        self._ensure(length)
        result = self._buffer[self._pos:self._pos + length]
        self._pos += length
        return result

    def add_integer(self, integer: int):
        integer = int(integer)
        if (integer < 0 or integer > 0xFFFFFFFF):
            raise ValueError("Integer out of range")
        self._buffer += integer.to_bytes(4, "little", signed=False)
        self._pos += 4

    def get_integer(self) -> int:
        self._ensure(4)
        result = int.from_bytes(self._buffer[self._pos:self._pos + 4], "little", signed=False)
        self._pos += 4
        return result

    def add_big_integer(self, integer: int):
        integer = int(integer)
        if (integer < 0 or integer > 0xFFFFFFFFFFFFFFFF):
            raise ValueError("Big integer out of range")
        self._buffer += integer.to_bytes(8, "little", signed=False)
        self._pos += 8

    def get_big_integer(self) -> int:
        self._ensure(8)
        result = int.from_bytes(self._buffer[self._pos:self._pos + 8], "little", signed=False)
        self._pos += 8
        return result

    def get_buffer(self) -> bytes:
        return self._buffer

    def add_byte(self, integer: int):
        integer = int(integer)
        if (integer < 0 or integer > 0xFF):
            raise ValueError("Byte out of range")
        self._buffer += integer.to_bytes(1, "little", signed=False)
        self._pos += 1

    def get_byte(self) -> int:
        self._ensure(1)
        result = self._buffer[self._pos]
        self._pos += 1
        return result

    def remaining(self) -> int:
        return len(self._buffer) - self._pos


class WSMessageType():
    REGISTER = 0
    UPLOAD_SUBCHUNK = 1
    GET_CHUNKS = 2
    DETACH_CHUNK = 3

    REGISTER_RESPONSE = 128
    CHUNK_RESPONSE = 129
    ERROR_RESPONSE = 130
    OK_RESPONSE = 131


class WSMessage():
    """!
    @brief The WSMessage object represents a message over the websocket to/from a worker
    """

    _KNOWN_TYPES = {
        WSMessageType.REGISTER,
        WSMessageType.UPLOAD_SUBCHUNK,
        WSMessageType.GET_CHUNKS,
        WSMessageType.DETACH_CHUNK,
        WSMessageType.REGISTER_RESPONSE,
        WSMessageType.CHUNK_RESPONSE,
        WSMessageType.ERROR_RESPONSE,
        WSMessageType.OK_RESPONSE,
    }

    def __init__(self, type: int, payload: dict):
        if (type not in self._KNOWN_TYPES):
            raise ValueError("Unknown message type")
        if (type(payload) != dict):
            raise TypeError("Payload must be a dict")
        self._type = type
        self._payload = payload

    def get_type(self):
        return self._type

    def get_payload(self):
        return self._payload

    def encode(self) -> bytes:
        encoded = BetterStruct()
        encoded.add_byte(self._type)

        if (self._type == WSMessageType.REGISTER):
            encoded.add_integer(self._payload["version"])
            encoded.add_integer(self._payload["max_concurrent"])
            encoded.add_string(self._payload.get("access_token", ""))
        elif (self._type == WSMessageType.UPLOAD_SUBCHUNK):
            encoded.add_string(self._payload["chunk_id"])
            encoded.add_string(self._payload["file_id"])
            encoded.add_bytes(self._payload["payload"])
        elif (self._type == WSMessageType.GET_CHUNKS):
            encoded.add_integer(self._payload["count"])
        elif (self._type == WSMessageType.DETACH_CHUNK):
            encoded.add_string(self._payload["chunk_id"])
        elif (self._type == WSMessageType.REGISTER_RESPONSE):
            encoded.add_string(self._payload["worker_id"])
        elif (self._type == WSMessageType.CHUNK_RESPONSE):
            encoded.add_integer(len(self._payload))
            for chunk_id, info in self._payload.items():
                encoded.add_string(chunk_id)
                encoded.add_string(info["file_id"])
                encoded.add_string(info["url"])
                encoded.add_big_integer(info["range"][0])
                encoded.add_big_integer(info["range"][1])
        else:  # ERROR_RESPONSE / OK_RESPONSE
            encoded.add_integer(len(self._payload))
            for key, value in self._payload.items():
                encoded.add_string(str(key))
                encoded.add_string(str(value))

        return encoded.get_buffer()

    @staticmethod
    def decode(encoded: bytes):
        if (type(encoded) not in (bytes, bytearray) or len(encoded) == 0):
            raise ValueError("Empty frame")

        struct = BetterStruct(bytes(encoded))
        type = struct.get_byte()
        if (type not in WSMessage._KNOWN_TYPES):
            raise ValueError("Unknown message type")

        payload = {}
        if (type == WSMessageType.REGISTER):
            payload["version"] = struct.get_integer()
            payload["max_concurrent"] = struct.get_integer()
            payload["access_token"] = struct.get_string()
        elif (type == WSMessageType.UPLOAD_SUBCHUNK):
            payload["chunk_id"] = struct.get_string()
            payload["file_id"] = struct.get_string()
            payload["payload"] = struct.get_bytes()
        elif (type == WSMessageType.GET_CHUNKS):
            payload["count"] = struct.get_integer()
        elif (type == WSMessageType.DETACH_CHUNK):
            payload["chunk_id"] = struct.get_string()
        elif (type == WSMessageType.REGISTER_RESPONSE):
            payload["worker_id"] = struct.get_string()
        elif (type == WSMessageType.CHUNK_RESPONSE):
            payload_length = struct.get_integer()
            for _ in range(payload_length):
                chunk_id = struct.get_string()
                file_id = struct.get_string()
                url = struct.get_string()
                start = struct.get_big_integer()
                end = struct.get_big_integer()
                payload[chunk_id] = {
                    "file_id": file_id,
                    "url": url,
                    "range": [start, end],
                }
        else:  # ERROR_RESPONSE / OK_RESPONSE
            payload_length = struct.get_integer()
            for _ in range(payload_length):
                key = struct.get_string()
                value = struct.get_string()
                payload[key] = value

        if (struct.remaining() != 0):
            raise ValueError("Trailing bytes in frame")

        return WSMessage(type, payload)
