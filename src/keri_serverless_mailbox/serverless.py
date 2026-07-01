"""ServerlessStrategy transport: WebSocket notify-and-fetch, hio-native.

Two channels, two transports:

  * NUDGE channel = WebSocket (the ``wss://`` loc for the mailbox EID, discovered KEL-native
    via ``hab.fetchUrl(eid, scheme=Schemes.wss)``). One idle socket; the server pushes tiny
    JSON nudges ``{"type":"mailbox.nudge","pre":...,"topic":...}``. The nudge carries NO CESR.
  * FETCH channel = ordinary HTTP (``agenting.httpClient(hab, eid)`` — the SAME path
    ``run_standard`` uses). On a nudge (or the infrequent safety-net timer) the client does ONE
    signed ``qry r=/mbx`` fetch and reads the drain the server produces (drain-and-close).

**hio-native, no thread.** The whole client runs on the host hio Doist: ``run_serverless`` is a
doer generator serviced by the SAME Doist as ``fetch_once`` (the HTTP drain). The WebSocket I/O
is ALSO serviced by that Doist — there is NO background thread and NO asyncio. hio ships no WS
client, so ``WsClient`` builds a minimal RFC-6455 client over ``hio.core.tcp.clienting.ClientTls``:
the underlying ``ClientTls`` is wrapped in a stock ``ClientDoer`` and scheduled on the owning
DoDoer (``scheduler.extend([clientDoer])``) exactly like ``httpClient``'s clientDoer, so the host
Doist drives connect/TLS/tx/rx; ``run_serverless`` pumps the WS state machine (handshake, subscribe,
frame parse) once per tick against ``client.rxbs`` / ``client.tx``.

Testability: ALL scheduling / fetch LOGIC lives in the ``run_serverless`` generator (the tested
unit); the WS source is an INJECTABLE ``ws_factory``. Tests inject a fake WS source whose nudge
queue is fed synchronously, so the generator is driven deterministically with no real socket.

Stock keripy / hio + stdlib only (hashlib / base64 / os / struct). NO ``websockets`` dependency.
Host-agnostic (no Locksmith, no Qt)."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import queue
import struct
import time
from urllib.parse import urlparse

from hio.core.tcp import clienting as tcp_clienting

from keri import help
from keri.kering import Schemes

from .fetch import build_qtopics, fetch_once

logger = help.ogler.getLogger(__name__)

# Indirected through the module so tests can monkeypatch a deterministic clock.
_monotonic = time.monotonic

# Defaults (ms). Keep-alive ping must be more frequent than the API-GW 10-min idle timeout;
# the safety-net fetch is a RARE backstop for missed nudges (not the old 30s treadmill).
DEFAULT_PING_MS = 4 * 60 * 1000          # 4 min < 10-min idle timeout
DEFAULT_SAFETY_NET_MS = 5 * 60 * 1000    # 5 min backstop
_RECONNECT_CAP_MS = 30 * 1000            # exponential backoff cap

# Liveness bound: max wall-clock a CONNECTING (TLS) or HANDSHAKE (awaiting 101) phase may hang
# before we teardown + reconnect. Restores the bound the old thread pump had via recv timeout;
# guards against a server that finishes TLS but never sends 101, or a half-open socket.
_PHASE_TIMEOUT_MS = 12 * 1000

# Nudges are tiny JSON; a frame advertising a huge length is bogus/hostile. Cap the accepted
# payload length — a frame over this triggers teardown+reconnect rather than buffering forever
# (which would otherwise collapse into the phase-timeout stall on an already-OPEN socket).
_MAX_FRAME = 1 << 20                      # 1 MiB

# Bound the non-JSON frame log so a garbage/huge payload can't spam the log with its full body.
_LOG_PAYLOAD_CAP = 120

# RFC 6455 GUID for the Sec-WebSocket-Accept handshake proof.
_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# WebSocket opcodes.
_OP_CONT = 0x0
_OP_TEXT = 0x1
_OP_BINARY = 0x2
_OP_CLOSE = 0x8
_OP_PING = 0x9
_OP_PONG = 0xA


def run_serverless(*, hab, eid, topics, on_message, cursor_store, retry_ms=1000, scheduler,
                   ws_factory=None, safety_net_ms=DEFAULT_SAFETY_NET_MS, ping_ms=DEFAULT_PING_MS):
    """hio doer generator. Opens one idle WS (hio-native, serviced by ``scheduler``'s Doist),
    then on each nudge (or the safety-net cadence) performs ONE signed-qry HTTP fetch through
    the shared fetch core.

    ``scheduler`` is the owning hio DoDoer; both the WS client's clientDoer and each fetch's
    clientDoer are scheduled on it (extend/remove) exactly like ``run_standard`` — otherwise
    nothing flushes over the wire.

    ``ws_factory(**kwargs) -> ws`` is injectable. The default builds a real hio-native
    ``WsClient`` over ``ClientTls`` and schedules its clientDoer on ``scheduler``; tests inject a
    fake WS source whose nudge queue they feed synchronously. A WS source exposes:
    ``.pump()`` (serviced each tick — drive handshake/parse; the default no-ops for fakes),
    ``.stop()``, and ``.nudges`` (a ``queue.Queue``)."""
    tock = 0.0
    _ = (yield tock)

    if ws_factory is None:
        ws_factory = _default_ws_factory

    url = hab.fetchUrl(eid, scheme=Schemes.wss)
    if not url:
        # No wss loc: nothing to connect to. Log and idle rather than crash (mirrors client.py).
        logger.info(f"no wss loc for mailbox {eid}; serverless client idles")
        return

    def _subscribe_builder():
        """Build the subscribe envelope with the CURRENT cursors, evaluated at (re)connect."""
        q_topics = build_qtopics(eid, topics, cursor_store)
        mhab = getattr(hab, "mhab", None)          # GroupHab: subscribe via the member hab
        querier = mhab if mhab is not None else hab
        msg = querier.query(pre=hab.pre, src=eid, route="mbx", query=dict(pre=hab.pre, topics=q_topics))
        qry_b64 = base64.b64encode(bytes(msg)).decode("ascii")
        return {"action": "subscribe", "qry": qry_b64}

    ws = ws_factory(hab=hab, eid=eid, url=url, subscribe_builder=_subscribe_builder,
                    retry_ms=retry_ms, ping_ms=ping_ms, scheduler=scheduler)

    last_safety = _monotonic()
    try:
        while True:
            nudge_received = False

            # Service the WS I/O this tick (connect/TLS handshake progress, WS upgrade,
            # subscribe, inbound frame parse, ping/pong keep-alive, reconnect on drop).
            # For the real client this drives the ClientTls the host Doist already services;
            # for the injected fake it is a no-op.
            ws.pump()

            # Drain any nudges the WS pushed onto its queue.
            # NOTE: the nudge topic is advisory — the server sends the bare /fwd modifier form
            # (e.g. "credential") while the client's subscribed topics are slash-prefixed
            # ("/credential"). Using the nudge's topic string to narrow the fetch would build
            # the wrong cursor key and the wrong qry topic (no slash => no server match).
            # So the nudge is treated as a pure "wake-and-fetch" signal: on ANY nudge we
            # re-drain ALL of the client's subscribed (slash-prefixed) topics past their cursors.
            while True:
                try:
                    ws.nudges.get_nowait()
                except queue.Empty:
                    break
                nudge_received = True   # bare vs. slash form doesn't matter; wake → full drain

            # Safety-net: an infrequent backstop to catch a missed nudge.
            should_fetch = nudge_received
            now = _monotonic()
            if (now - last_safety) * 1000.0 >= safety_net_ms:
                last_safety = now
                should_fetch = True

            if should_fetch:
                # ONE fetch per wake, covering ALL subscribed (slash-prefixed) topics.
                yield from fetch_once(hab=hab, eid=eid, topics=topics,
                                      on_message=on_message, cursor_store=cursor_store,
                                      scheduler=scheduler)

            yield tock
    finally:
        ws.stop()


# --------------------------------------------------------------------------------------------
# hio-native WS client: a minimal RFC-6455 client over ClientTls, serviced by the host Doist.
# NOT exercised by the run_serverless unit tests (they inject a fake ws source); its framing /
# handshake are unit-tested directly below (test_serverless_strategy.py) with a fake ClientTls.

# Connection lifecycle states.
_ST_CONNECTING = "connecting"    # ClientTls establishing TCP+TLS
_ST_HANDSHAKE = "handshake"      # HTTP/1.1 Upgrade sent, awaiting 101
_ST_OPEN = "open"                # WS open, subscribe sent, streaming frames
_ST_CLOSED = "closed"            # dropped/errored; awaiting backoff before reconnect


def _ws_frame(opcode, payload=b""):
    """Encode a client->server WebSocket frame (FIN=1, masked per RFC 6455). ``payload`` is
    bytes. Handles the 7-bit / 16-bit / 64-bit length forms."""
    b0 = 0x80 | (opcode & 0x0F)                 # FIN=1
    n = len(payload)
    header = bytearray([b0])
    if n < 126:
        header.append(0x80 | n)                 # mask bit set + 7-bit length
    elif n < (1 << 16):
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", n))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", n))
    mask = os.urandom(4)
    header.extend(mask)
    masked = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
    return bytes(header) + masked


class _OversizeFrame(Exception):
    """A frame advertised a payload length beyond ``_MAX_FRAME``; caller must reconnect rather
    than wait to buffer it."""


def _parse_ws_frames(buf):
    """Parse as many complete server->client frames from ``buf`` (a bytearray) as are present.
    Server frames are UNMASKED. Returns a list of ``(fin, opcode, payload_bytes)`` and consumes
    the parsed bytes from ``buf`` in place; a partial trailing frame is left for the next call.

    Raises ``_OversizeFrame`` (without consuming) if a frame advertises a length > ``_MAX_FRAME``
    — the header is enough to decide, so we needn't buffer the whole (bogus) body first."""
    frames = []
    while True:
        if len(buf) < 2:
            break
        b0, b1 = buf[0], buf[1]
        fin = (b0 & 0x80) != 0
        opcode = b0 & 0x0F
        masked = (b1 & 0x80) != 0
        length = b1 & 0x7F
        offset = 2
        if length == 126:
            if len(buf) < offset + 2:
                break
            length = struct.unpack("!H", bytes(buf[offset:offset + 2]))[0]
            offset += 2
        elif length == 127:
            if len(buf) < offset + 8:
                break
            length = struct.unpack("!Q", bytes(buf[offset:offset + 8]))[0]
            offset += 8
        if length > _MAX_FRAME:                 # decided from the header alone — don't buffer it
            raise _OversizeFrame(f"ws frame length {length} exceeds cap {_MAX_FRAME}")
        mask = b""
        if masked:                              # servers shouldn't mask, but handle defensively
            if len(buf) < offset + 4:
                break
            mask = bytes(buf[offset:offset + 4])
            offset += 4
        if len(buf) < offset + length:
            break                               # partial frame; wait for more bytes
        payload = bytes(buf[offset:offset + length])
        if masked:
            payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
        del buf[:offset + length]
        frames.append((fin, opcode, payload))
    return frames


class WsClient:
    """Minimal hio-native RFC-6455 WebSocket client over ``ClientTls``.

    On start it schedules a stock ``ClientDoer`` (over the ClientTls) on the host ``scheduler``,
    so the host Doist services the socket (connect/TLS/tx/rx). ``pump()`` — called once per tick
    by ``run_serverless`` on the same Doist — advances the WS state machine: after TLS connects it
    sends the HTTP Upgrade, verifies the ``101`` + ``Sec-WebSocket-Accept``, sends the subscribe
    envelope (built by ``subscribe_builder`` with current cursors), parses inbound frames from
    ``client.rxbs`` pushing each ``mailbox.nudge`` onto ``self.nudges``, replies PONG to PING, and
    sends periodic PING keep-alives. On close/drop/error it tears the socket down and reconnects
    with exponential backoff, RE-SUBSCRIBING with the (advanced) cursors.

    ``client_factory(host, port) -> ClientTls`` is injectable so the framing/handshake is testable
    with a fake ClientTls; the default builds a real ``ClientTls`` on port 443."""

    def __init__(self, *, hab, eid, url, subscribe_builder, scheduler, retry_ms=1000,
                 ping_ms=DEFAULT_PING_MS, client_factory=None):
        self.hab = hab
        self.eid = eid
        self.url = url
        self.subscribe_builder = subscribe_builder
        self.scheduler = scheduler
        self.retry_ms = max(retry_ms, 1)
        self.ping_ms = ping_ms
        self.nudges: queue.Queue = queue.Queue()
        self._client_factory = client_factory or _default_client_tls

        up = urlparse(url)
        self.host = up.hostname
        self.port = up.port or 443
        self.path = up.path or "/"
        if up.query:
            self.path = f"{self.path}?{up.query}"

        self.client = None
        self.clientDoer = None
        self._state = _ST_CLOSED
        self._sec_key = ""            # base64 Sec-WebSocket-Key of the in-flight handshake
        self._backoff_s = self.retry_ms / 1000.0
        self._reconnect_at = 0.0      # monotonic time we may reconnect (backoff gate)
        self._last_ping = 0.0
        self._phase_deadline = 0.0    # monotonic deadline for the CONNECTING/HANDSHAKE phase
        # Fragmentation reassembly: opcode of the in-flight data message + accumulated payload.
        self._frag_opcode = None
        self._frag_buf = bytearray()
        # Start disconnected; the first pump() opens the socket.
        self._connect()

    # -- connection lifecycle ----------------------------------------------------------------

    def _connect(self):
        """Build a fresh ClientTls + ClientDoer and schedule the doer on the host scheduler."""
        try:
            self.client = self._client_factory(self.host, self.port)
            self.clientDoer = tcp_clienting.ClientDoer(client=self.client)
            self.scheduler.extend([self.clientDoer])
        except Exception as ex:   # noqa: BLE001 -- any connect setup failure -> backoff+retry
            logger.error(f"ws connect setup to {self.host}:{self.port} failed: {ex}; backing off")
            self._teardown()
            self._schedule_reconnect()
            return
        self._state = _ST_CONNECTING
        self._last_ping = _monotonic()
        self._phase_deadline = _monotonic() + _PHASE_TIMEOUT_MS / 1000.0

    def _teardown(self):
        """Remove the clientDoer from the scheduler and drop the socket."""
        if self.clientDoer is not None:
            try:
                self.scheduler.remove([self.clientDoer])
            except Exception:   # noqa: BLE001
                pass
        if self.client is not None:
            try:
                self.client.close()
            except Exception:   # noqa: BLE001
                pass
        self.client = None
        self.clientDoer = None
        self._state = _ST_CLOSED
        # Drop any half-assembled fragmented message; a fresh connection restarts framing.
        self._frag_opcode = None
        self._frag_buf = bytearray()

    def _schedule_reconnect(self):
        self._reconnect_at = _monotonic() + self._backoff_s
        self._backoff_s = min(self._backoff_s * 2, _RECONNECT_CAP_MS / 1000.0)

    def stop(self):
        self._teardown()

    # -- per-tick state machine --------------------------------------------------------------

    def pump(self):
        """Advance the WS state machine one tick. Never raises (any error -> reconnect)."""
        try:
            self._pump()
        except Exception as ex:   # noqa: BLE001 -- any WS-loop error -> reconnect+resubscribe
            logger.info(f"ws pump error for {self.url}: {ex}; reconnecting")
            self._teardown()
            self._schedule_reconnect()

    def _pump(self):
        if self._state == _ST_CLOSED:
            if _monotonic() >= self._reconnect_at:
                self._connect()
            return

        client = self.client
        if client is None:
            self._state = _ST_CLOSED
            return

        # Detect a dropped socket (hio flags .cutoff on close/reset).
        if getattr(client, "cutoff", False):
            logger.info(f"ws socket cutoff for {self.url}; reconnecting")
            self._teardown()
            self._schedule_reconnect()
            return

        # Liveness bound for the pre-OPEN phases: a server that finishes TLS but never sends 101
        # (or a half-open socket) must not hang the client forever.
        if self._state in (_ST_CONNECTING, _ST_HANDSHAKE) and _monotonic() >= self._phase_deadline:
            logger.info(f"ws {self._state} phase timed out for {self.url}; reconnecting")
            self._teardown()
            self._schedule_reconnect()
            return

        if self._state == _ST_CONNECTING:
            if client.connected:
                self._send_upgrade()
                self._state = _ST_HANDSHAKE
                # Reset the deadline for the fresh HANDSHAKE phase (awaiting the 101).
                self._phase_deadline = _monotonic() + _PHASE_TIMEOUT_MS / 1000.0
            return

        if self._state == _ST_HANDSHAKE:
            self._try_complete_handshake()
            return

        if self._state == _ST_OPEN:
            self._read_frames()
            # Periodic keep-alive ping (free control frame; server replies pong).
            if (_monotonic() - self._last_ping) * 1000.0 >= self.ping_ms:
                client.tx(_ws_frame(_OP_PING))
                self._last_ping = _monotonic()

    def _send_upgrade(self):
        """Send the HTTP/1.1 Upgrade request that opens the WebSocket."""
        self._sec_key = base64.b64encode(os.urandom(16)).decode("ascii")
        host_hdr = self.host if self.port == 443 else f"{self.host}:{self.port}"
        req = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {host_hdr}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {self._sec_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"\r\n"
        )
        self.client.tx(req.encode("ascii"))

    def _try_complete_handshake(self):
        """Read the 101 response from client.rxbs; verify Sec-WebSocket-Accept; go OPEN."""
        rxbs = self.client.rxbs
        end = rxbs.find(b"\r\n\r\n")
        if end < 0:
            return                              # headers not fully received yet
        head = bytes(rxbs[:end]).decode("latin-1")
        del rxbs[:end + 4]                      # consume the header block; body (if any) stays
        lines = head.split("\r\n")
        status_line = lines[0] if lines else ""
        if "101" not in status_line:
            raise ValueError(f"ws upgrade failed: {status_line!r}")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        accept = headers.get("sec-websocket-accept", "")
        expected = base64.b64encode(
            hashlib.sha1((self._sec_key + _WS_GUID).encode("ascii")).digest()
        ).decode("ascii")
        if accept != expected:
            raise ValueError("ws upgrade Sec-WebSocket-Accept mismatch")
        # Handshake OK: reset backoff, subscribe with current cursors, start streaming.
        self._backoff_s = self.retry_ms / 1000.0
        self.client.tx(_ws_frame(_OP_TEXT, json.dumps(self.subscribe_builder()).encode("utf-8")))
        self._last_ping = _monotonic()
        self._state = _ST_OPEN
        # Any bytes already buffered past the header may be the first frame(s).
        self._read_frames()

    def _read_frames(self):
        """Parse WS frames from client.rxbs and dispatch. Data frames (TEXT/CONT/BINARY) are
        reassembled by FIN before dispatch — a fragmented TEXT(FIN=0)+CONT(FIN=1) message is one
        logical nudge. Control frames (ping/pong/close) are never fragmented and are handled
        immediately, even if they interleave between data fragments (RFC 6455 §5.4)."""
        try:
            frames = _parse_ws_frames(self.client.rxbs)
        except _OversizeFrame as ex:
            logger.info(f"ws oversize frame for {self.url}: {ex}; reconnecting")
            self._teardown()
            self._schedule_reconnect()
            return
        for fin, opcode, payload in frames:
            if opcode == _OP_PING:
                self.client.tx(_ws_frame(_OP_PONG, payload))
            elif opcode == _OP_PONG:
                pass                            # keep-alive acknowledged
            elif opcode == _OP_CLOSE:
                logger.info(f"ws close frame for {self.url}; reconnecting")
                self._teardown()
                self._schedule_reconnect()
                return
            elif opcode == _OP_CONT:
                # Continuation of an in-flight data message.
                if self._frag_opcode is None:
                    logger.info("ws continuation frame with no message in progress; ignored")
                    continue
                self._frag_buf.extend(payload)
                if fin:
                    self._dispatch_message(self._frag_opcode, bytes(self._frag_buf))
                    self._frag_opcode = None
                    self._frag_buf = bytearray()
            elif opcode in (_OP_TEXT, _OP_BINARY):
                # Start of a data message.
                if fin:
                    self._dispatch_message(opcode, payload)   # unfragmented
                else:
                    self._frag_opcode = opcode                # begin reassembly
                    self._frag_buf = bytearray(payload)
            # any other (reserved) opcode: ignore

    def _dispatch_message(self, opcode, payload):
        """Dispatch a COMPLETE (reassembled) data message. Only TEXT (0x1) can be a nudge;
        BINARY (0x2) is not part of the nudge protocol and is dropped."""
        if opcode != _OP_TEXT:                  # drop BINARY rather than force-JSON it (M-1)
            logger.info(f"ws non-text data frame (opcode {opcode:#x}) dropped")
            return
        try:
            obj = json.loads(payload.decode("utf-8"))
        except (ValueError, TypeError, UnicodeDecodeError):
            head = payload[:_LOG_PAYLOAD_CAP]
            suffix = "..." if len(payload) > _LOG_PAYLOAD_CAP else ""
            logger.info(f"non-JSON ws frame ignored ({len(payload)} bytes): {head!r}{suffix}")
            return
        if isinstance(obj, dict) and obj.get("type") == "mailbox.nudge":
            self.nudges.put(obj)


def _default_client_tls(host, port):
    """Build a real non-blocking ClientTls for the given host:port (server-auth TLS)."""
    return tcp_clienting.ClientTls(host=host, port=port)


def _default_ws_factory(*, hab, eid, url, subscribe_builder, retry_ms, ping_ms, scheduler):
    return WsClient(hab=hab, eid=eid, url=url, subscribe_builder=subscribe_builder,
                    scheduler=scheduler, retry_ms=retry_ms, ping_ms=ping_ms)
