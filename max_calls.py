from __future__ import annotations

import argparse
import base64
import json
import queue
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import lz4.frame
import websocket


ONEME_ENDPOINT = "wss://ws-api.oneme.ru/websocket"
ONEME_ORIGIN = "https://web.max.ru"

CLIENT_HELLO_OPCODE = 6
VERIFICATION_REQUEST_OPCODE = 17
CODE_ENTER_OPCODE = 18
CHAT_SYNC_REQUEST_OPCODE = 19
INCOMING_CALL_OPCODE = 137
CALL_TOKEN_REQUEST_OPCODE = 158

PROTOCOL_VERSION = 11
CLIENT_SIDE = 0
CHATS_COUNT = 40
CHATS_SYNC = 0
CONTACTS_SYNC = 0
PRESENCE_SYNC = 0
DRAFTS_SYNC = 0
SESSION_DATA_VERSION = 3
ACCEPT_CALL_SEQUENCE = 1
INITIAL_TRANSMIT_SEQUENCE = 2
QUEUE_TIMEOUT = 0.1
MESSAGE_INTERVAL = 5


@dataclass(frozen=True)
class OneMeError(RuntimeError):
    message: str
    raw_payload: Dict[str, Any]

    def __str__(self) -> str:
        return f"{self.message}: {self.raw_payload}"


def _new_user_agent() -> Dict[str, Any]:
    return {
        "deviceType": "WEB",
        "locale": "ru",
        "deviceLocale": "ru",
        "osVersion": "Windows",
        "deviceName": "Chrome",
        "headerUserAgent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
        ),
        "appVersion": "25.11.2",
        "screen": "1080x1920 1.0x",
        "timezone": "Europe/Moscow",
    }


class OneMeClient:
    def __init__(self) -> None:
        self._conn = websocket.create_connection(
            ONEME_ENDPOINT,
            header=[f"Origin: {ONEME_ORIGIN}"],
        )
        self._seq = 0
        self._pending: List[Dict[str, Any]] = []

    def close(self) -> None:
        try:
            self._conn.close()
        finally:
            self._pending.clear()

    def __enter__(self) -> "OneMeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _send_message(self, opcode: int, payload: Dict[str, Any]) -> int:
        seq = self._seq
        message = {
            "seq": seq,
            "opcode": opcode,
            "payload": payload,
            "ver": PROTOCOL_VERSION,
            "cmd": CLIENT_SIDE,
        }
        self._conn.send(json.dumps(message))
        self._seq += 1
        return seq

    def _receive_raw(self) -> Dict[str, Any]:
        if self._pending:
            return self._pending.pop(0)
        data = self._conn.recv()
        return json.loads(data)

    def _receive_message(self, expected_seq: Optional[int]) -> Dict[str, Any]:
        if expected_seq is not None:
            for idx, msg in enumerate(self._pending):
                if int(msg.get("seq", -1)) == expected_seq:
                    payload = self._pending.pop(idx).get("payload", {})
                    return self._validate_payload(payload)

        while True:
            msg = self._receive_raw()
            seq_val = msg.get("seq")
            if expected_seq is None or int(seq_val) == expected_seq:
                payload = msg.get("payload", {})
                return self._validate_payload(payload)

            self._pending.append(msg)

    @staticmethod
    def _validate_payload(payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise OneMeError("invalid payload", {"payload": payload})
        if "error" in payload:
            raise OneMeError(str(payload["error"]), payload)
        return payload

    def do_client_hello(self) -> None:
        seq = self._send_message(
            CLIENT_HELLO_OPCODE,
            {
                "userAgent": _new_user_agent(),
                "deviceId": str(uuid.uuid4()),
            },
        )
        self._receive_message(seq)

    def do_verification_request(self, phone: str) -> Dict[str, Any]:
        seq = self._send_message(
            VERIFICATION_REQUEST_OPCODE,
            {
                "phone": phone,
                "type": "START_AUTH",
                "language": "ru",
            },
        )
        return self._receive_message(seq)

    def do_code_enter(self, token: str, verify_code: str) -> Dict[str, Any]:
        seq = self._send_message(
            CODE_ENTER_OPCODE,
            {
                "token": token,
                "verifyCode": verify_code,
                "authTokenType": "CHECK_CODE",
            },
        )
        return self._receive_message(seq)

    def do_chat_sync(self, token: str) -> Dict[str, Any]:
        seq = self._send_message(
            CHAT_SYNC_REQUEST_OPCODE,
            {
                "token": token,
                "interactive": False,
                "chatsCount": 40,
                "chatsSync": 0,
                "contactsSync": 0,
                "presenceSync": 0,
                "draftsSync": 0,
            },
        )
        return self._receive_message(seq)

    def do_call_token_request(self) -> str:
        seq = self._send_message(CALL_TOKEN_REQUEST_OPCODE, {})
        payload = self._receive_message(seq)
        token = payload.get("token")
        if not isinstance(token, str):
            raise RuntimeError(f"Unexpected call token payload: {payload!r}")
        return token

    def wait_for_incoming_call(self) -> Dict[str, Any]:
        while True:
            msg = self._receive_raw()
            opcode = msg.get("opcode")
            if isinstance(opcode, float):
                opcode = int(opcode)
            if opcode != INCOMING_CALL_OPCODE:
                self._pending.append(msg)
                continue
            payload = msg.get("payload", {})
            if isinstance(payload, dict):
                return payload


###############################################################################
# Call helpers
###############################################################################

CALLS_ENDPOINT = "https://calls.okcdn.ru/fb.do"
CALLS_APPLICATION_KEY = "CNHIJPLGDIHBABABA"


class CallsClient:
    def run_method(self, method: str, params: Dict[str, str]) -> Dict[str, Any]:
        form = {
            "method": method,
            "format": "JSON",
            "application_key": CALLS_APPLICATION_KEY,
            **params,
        }
        data = urlencode(form).encode("utf-8")
        req = Request(CALLS_ENDPOINT, data=data, method="POST")
        with urlopen(req) as resp:
            return json.loads(resp.read())

    def login(self, token: str) -> Dict[str, Any]:
        session_data = {
            "auth_token": token,
            "client_type": "SDK_JS",
            "client_version": "1.1",
            "device_id": str(uuid.uuid4()),
            "version": 3,
        }
        return self.run_method(
            "auth.anonymLogin",
            {"session_data": json.dumps(session_data)},
        )

    def start_conversation(
        self,
        session_key: str,
        calltaker_external_id: str,
        conversation_id: str,
    ) -> Dict[str, Any]:
        payload = {"is_video": False}
        return self.run_method(
            "vchat.startConversation",
            {
                "conversationId": conversation_id,
                "isVideo": "false",
                "protocolVersion": "5",
                "payload": json.dumps(payload),
                "externalIds": calltaker_external_id,
                "session_key": session_key,
            },
        )


###############################################################################
# Signaling
###############################################################################

SIGNALING_ORIGIN = "https://web.max.ru"


def _find_user_id_by_external_id(server_hello: Dict[str, Any], external_id: str) -> int:
    participants = server_hello.get("conversation", {}).get("participants", [])
    for participant in participants:
        ext = participant.get("externalId", {}).get("id")
        if ext == external_id:
            return int(participant["id"])
    raise ValueError(f"User id not found for external id {external_id!r}")


class SignalingSession:
    def __init__(self, endpoint: str, target_external_id: str, accept_call: bool) -> None:
        query = {
            "platform": "WEB",
            "appVersion": "1.1",
            "version": "5",
            "device": "browser",
            "capabilities": "603F",
            "clientType": "ONE_ME",
            "tgt": "start",
        }
        separator = "&" if "?" in endpoint else "?"
        url = f"{endpoint}{separator}{urlencode(query)}"

        self._conn = websocket.create_connection(
            url,
            header=[f"Origin: {SIGNALING_ORIGIN}"],
        )
        self._running = True
        self._send_queue: "queue.Queue[Any]" = queue.Queue()
        self._receive_queue: "queue.Queue[Any]" = queue.Queue()
        self._raw_queue: "queue.Queue[Any]" = queue.Queue()

        self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._send_thread.start()
        self._recv_thread.start()

        server_hello_raw = self._raw_queue.get()
        if isinstance(server_hello_raw, bytes):
            server_hello_raw = server_hello_raw.decode("utf-8")
        server_hello = json.loads(server_hello_raw)
        self._participant_id = _find_user_id_by_external_id(server_hello, target_external_id)

        self._sequence = INITIAL_TRANSMIT_SEQUENCE

        if accept_call:
            accept_payload = {
                "command": "accept-call",
                "sequence": ACCEPT_CALL_SEQUENCE,
                "mediaSettings": {
                    "isAudioEnabled": True,
                    "isVideoEnabled": False,
                    "isScreenSharingEnabled": False,
                    "isFastScreenSharingEnabled": False,
                    "isAudioSharingEnabled": False,
                    "isAnimojiEnabled": False,
                },
            }
            self._conn.send(json.dumps(accept_payload))

        self._converter_thread = threading.Thread(target=self._channel_converter, daemon=True)
        self._converter_thread.start()

    def _receive_loop(self) -> None:
        while self._running:
            try:
                msg = self._conn.recv()
                self._raw_queue.put(msg)
            except Exception:
                self._running = False
                break

    def _send_loop(self) -> None:
        while self._running:
            try:
                msg = self._send_queue.get(timeout=QUEUE_TIMEOUT)
            except queue.Empty:
                continue
            try:
                self._conn.send(msg)
            except Exception:
                self._running = False
                break

    def _channel_converter(self) -> None:
        while self._running:
            # Outgoing messages
            try:
                payload = self._receive_queue.get_nowait()
            except queue.Empty:
                payload = None

            if payload is not None:
                message = {
                    "command": "transmit-data",
                    "sequence": self._sequence,
                    "participantId": self._participant_id,
                    "data": payload,
                    "participantType": "USER",
                }
                self._sequence += 1
                self._send_queue.put(json.dumps(message))

            # Incoming messages
            try:
                raw = self._raw_queue.get(timeout=QUEUE_TIMEOUT)
            except queue.Empty:
                continue

            if raw in ("ping", b"ping"):
                self._send_queue.put("pong")
                continue

            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if decoded.get("type") != "notification":
                continue
            if decoded.get("notification") != "transmitted-data":
                continue

            self._receive_queue.put(decoded.get("data"))

    def send(self, message: Any) -> None:
        self._receive_queue.put(message)

    def receive(self, timeout: Optional[float] = None) -> Any:
        return self._receive_queue.get(timeout=timeout)

    def close(self) -> None:
        self._running = False
        try:
            self._conn.close()
        finally:
            pass


def decode_vcp(vcp: str) -> Dict[str, Any]:
    parts = vcp.split(":", 1)
    if len(parts) != 2:
        raise ValueError("Invalid VCP data")
    size_str, compressed_b64 = parts
    compressed = base64.b64decode(compressed_b64)
    size = int(size_str)
    decompressed = lz4.frame.decompress(compressed, uncompressed_size=size)
    return json.loads(decompressed)


def parse_incoming_call(raw: Dict[str, Any]) -> Dict[str, Any]:
    decoded_vcp = decode_vcp(raw.get("vcp", ""))
    return {
        "callerId": raw.get("callerId"),
        "conversationId": raw.get("conversationId"),
        "signaling": {
            "token": decoded_vcp.get("tkn", ""),
            "url": decoded_vcp.get("wse", ""),
        },
        "stun": decoded_vcp.get("stne", ""),
        "turn": {
            "servers": (decoded_vcp.get("trne", "") or "").split(","),
            "user": decoded_vcp.get("trnu", ""),
            "password": decoded_vcp.get("trnp", ""),
        },
    }


def get_oneme_token(phone: Optional[str] = None, token: Optional[str] = None, token_file: Optional[str] = None) -> str:
    if token:
        return token
    if token_file and Path(token_file).exists():
        return Path(token_file).read_text(encoding="utf-8").strip()
    if phone:
        print(f"Authenticating phone: {phone}")
        with OneMeClient() as client:
            client.do_client_hello()
            verification = client.do_verification_request(phone)
            ver_token = verification.get("token")
            if not ver_token:
                raise RuntimeError(f"No verification token: {verification}")
            code = input("SMS code: ").strip()
            login = client.do_code_enter(ver_token, code)
            token_attrs = login.get("tokenAttrs", {})
            login_token = token_attrs.get("LOGIN", {}).get("token")
            if not login_token:
                raise RuntimeError(f"No login token: {login}")
            print(f"Got token: {login_token[:20]}...")
            return login_token
    raise ValueError("Need --token, --token-file, or --phone")


def prepare_calls_session(token: str) -> tuple[Dict[str, Any], CallsClient]:
    print("Getting Calls token...")
    with OneMeClient() as client:
        client.do_client_hello()
        client.do_chat_sync(token)
        calls_token = client.do_call_token_request()
    
    print("Logging into Calls API...")
    calls_client = CallsClient()
    login_data = calls_client.login(calls_token)
    print(f"Logged in as: {login_data['external_user_id']}")
    return login_data, calls_client


def run_calltaker(args: argparse.Namespace) -> None:
    token = get_oneme_token(args.phone, args.token, args.token_file)
    login_data, calls_client = prepare_calls_session(token)
    
    print(f"Call taker ID: {login_data['external_user_id']}")
    print("Waiting for incoming calls...")
    
    with OneMeClient() as client:
        client.do_client_hello()
        client.do_chat_sync(token)
        incoming_raw = client.wait_for_incoming_call()
        incoming_call = parse_incoming_call(incoming_raw)
    
    signaling = SignalingSession(
        endpoint=f"{incoming_call['signaling']['url']}?"
        f"userId={login_data['uid']}&entityType=USER&conversationId={incoming_call['conversationId']}"
        f"&token={incoming_call['signaling']['token']}",
        target_external_id=str(incoming_call["callerId"]),
        accept_call=True,
    )
    
    try:
        while True:
            message = "Hello caller!"
            print(f"[Signaling.Calltaker] {message}")
            signaling.send(message)
            time.sleep(MESSAGE_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping calltaker...")
    finally:
        signaling.close()


def run_caller(args: argparse.Namespace) -> None:
    token = get_oneme_token(args.phone, args.token, args.token_file)
    login_data, calls_client = prepare_calls_session(token)
    
    calltaker_external_id = args.calltaker_id or input("Call taker ID: ").strip()
    
    print(f"Starting conversation with {calltaker_external_id}...")
    conversation_id = str(uuid.uuid4())
    started = calls_client.start_conversation(
        login_data["session_key"],
        calltaker_external_id,
        conversation_id,
    )
    
    signaling = SignalingSession(
        endpoint=started["endpoint"],
        target_external_id=calltaker_external_id,
        accept_call=False,
    )
    
    try:
        while True:
            message = "Hello calltaker!"
            print(f"[Signaling.Caller] {message}")
            signaling.send(message)
            time.sleep(MESSAGE_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping caller...")
    finally:
        signaling.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MAX Calls - automatic setup and call handling")
    subparsers = parser.add_subparsers(dest="command", required=True)

    calltaker_parser = subparsers.add_parser("calltaker", help="Run as calltaker (receive calls)")
    calltaker_parser.add_argument("--token", help="OneMe token (if you have it)")
    calltaker_parser.add_argument("--token-file", help="Path to OneMe token file")
    calltaker_parser.add_argument("--phone", help="Phone number (+7XXXXXXXXXX) - will authenticate automatically")
    calltaker_parser.set_defaults(func=run_calltaker)

    caller_parser = subparsers.add_parser("caller", help="Run as caller (make calls)")
    caller_parser.add_argument("--token", help="OneMe token (if you have it)")
    caller_parser.add_argument("--token-file", help="Path to OneMe token file")
    caller_parser.add_argument("--phone", help="Phone number (+7XXXXXXXXXX) - will authenticate automatically")
    caller_parser.add_argument("--calltaker-id", help="Call taker ID to call")
    caller_parser.set_defaults(func=run_caller)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
        return 0
    except OneMeError as exc:
        print(f"OneMe error: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())

