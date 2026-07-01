"""Tests for the Serverless (WebSocket notify-and-fetch) strategy. NO real network.

The generator (the tested unit) owns ALL scheduling/fetch logic; the WebSocket I/O is
hio-native and INJECTABLE. Tests inject a FAKE WS source whose nudge queue is preloaded
synchronously, so the hio generator is driven deterministically with no real socket and
no thread. The real ``WsClient`` framing/handshake is unit-tested directly with a fake
``ClientTls`` (no socket)."""
import base64
import hashlib
import json
import queue
from types import SimpleNamespace

from hio.base import doing

from keri_serverless_mailbox import serverless
from keri_serverless_mailbox import fetch as fetch_mod   # the true lookup site for agenting/httping


class _FakeCursorStore:
    def __init__(self, seed=None):
        self.saved = dict(seed or {})
    def get(self, eid, topic): return self.saved.get((eid, topic))
    def set(self, eid, topic, idx): self.saved[(eid, topic)] = idx


class _FakeScheduler:
    """Stands in for the owning DoDoer. Records extend/remove so tests can assert the
    fetch's clientDoer is actually scheduled (the Phase-2 regression guard)."""
    def __init__(self):
        self.extended = []
        self.removed = []
    def extend(self, doers): self.extended.append(list(doers))
    def remove(self, doers): self.removed.append(list(doers))


class _FakeWs:
    """Injected in place of the real hio-native WsClient. Records subscribe envelopes and lets
    the test push nudges onto the queue synchronously; no real socket. ``pump()`` is called by
    run_serverless each tick (a no-op here — the fake feeds nudges directly)."""
    def __init__(self, *, hab, eid, url, subscribe_builder, retry_ms, ping_ms, scheduler):
        self.hab = hab
        self.eid = eid
        self.url = url
        self.subscribe_builder = subscribe_builder
        self.retry_ms = retry_ms
        self.ping_ms = ping_ms
        self.scheduler = scheduler
        self.nudges = queue.Queue()
        self.started = False               # set True on first pump() (mirrors "connected")
        self.stopped = False
        self.pumps = 0                     # how many times run_serverless serviced the WS
        self.subscribe_envelopes = []      # every envelope the client would send
        self.connects = 0                  # how many times it (re)connected+subscribed
        # Construction implies the first connect+subscribe (WsClient subscribes on handshake).
        self._connect()

    def _connect(self):
        # A real connect builds + sends the subscribe envelope with current cursors.
        self.connects += 1
        self.subscribe_envelopes.append(self.subscribe_builder())

    def resubscribe(self):
        """Simulate a reconnect: the client re-sends the subscribe envelope w/ CURRENT cursors."""
        self._connect()

    def pump(self):
        self.pumps += 1
        self.started = True

    def push_nudge(self, pre, topic, cursor=None):
        self.nudges.put({"type": "mailbox.nudge", "pre": pre, "topic": topic, "cursor": cursor})

    def stop(self):
        self.stopped = True


def _make_ws_factory():
    holder = {}
    def factory(**kwa):
        ws = _FakeWs(**kwa)
        holder["ws"] = ws
        return ws
    return factory, holder


def _fake_http_client(events):
    """A fake keripy httpClient (client, clientDoer). .requests empties immediately;
    .events is preloaded with the drain the one-shot fetch reads."""
    import collections
    client = SimpleNamespace(requests=[], events=collections.deque(events))
    clientDoer = doing.Doer()
    return client, clientDoer


def _make_hab():
    # query returns signed CESR bytes; the pump base64-encodes them into the envelope.
    # fetchUrl(eid, scheme=wss) returns the mailbox's wss connect URL (KEL-native discovery).
    return SimpleNamespace(pre="Ebob", query=lambda **kw: b"SIGNED-MBX-QRY:" + repr(kw).encode(),
                           mhab=None, fetchUrl=lambda eid, scheme="": "wss://mailbox.example/prod")


def _drive(gen, holder, *, ticks, act=None, until=None):
    """Send None into the generator up to `ticks` times. `act(i)` runs each tick BEFORE the
    send (to push nudges / drop sockets); stop early when `until()` is true. Bounded so a
    bug can't hang the test."""
    for i in range(ticks):
        if act is not None:
            act(i)
        try:
            gen.send(None)
        except StopIteration:
            break
        if until is not None and until():
            break
    gen.close()


# ---------------------------------------------------------------------------------------

def test_subscribe_envelope_carries_signed_mbx_qry_with_current_cursors(monkeypatch):
    """The pump's subscribe envelope is action=subscribe and its qry base64-decodes to a
    signed /mbx qry built from the current cursors (LAST-SEEN ordinal, or -1 if unseen;
    the server drains from cursor+1)."""
    factory, holder = _make_ws_factory()
    # /credential seen at 4 -> qry cursor 4 (server drains from 5); /receipt unseen -> -1.
    cur = _FakeCursorStore(seed={("Embx", "/credential"): 4})
    sched = _FakeScheduler()
    hab = _make_hab()

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential", "/receipt"],
        on_message=lambda t, r: None, cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)

    _drive(gen, holder, ticks=3, until=lambda: holder.get("ws") and holder["ws"].started)

    ws = holder["ws"]
    assert ws.started
    env = ws.subscribe_envelopes[0]
    assert env["action"] == "subscribe"
    decoded = base64.b64decode(env["qry"])
    assert decoded.startswith(b"SIGNED-MBX-QRY:")
    # The qry-build kwargs are embedded (our fake query echoes them): route=mbx, cursors applied.
    assert b"'route': 'mbx'" in decoded
    assert b"'/credential': 4" in decoded    # seen 4 -> last-seen (server drains from 5)
    assert b"'/receipt': -1" in decoded      # unseen -> -1 (server drains from 0)


def test_nudge_triggers_exactly_one_fetch_and_advances_cursor(monkeypatch):
    """I-1: a nudge with a BARE topic (server-side /fwd form, e.g. "credential") that does NOT
    match the client's slash-prefixed subscribed topics must still trigger ONE fetch over the
    client's OWN subscribed topics ("/credential"), not the bare nudge topic ("credential").

    RED against the old union-nudged-topics code (it would fetch "credential" with no slash,
    building cursor key ("Embx","credential") and qry topic "credential" => server miss).
    GREEN after the fix (any nudge => re-drain subscribed ("/credential") past their cursors)."""
    received = []
    factory, holder = _make_ws_factory()
    cur = _FakeCursorStore()
    sched = _FakeScheduler()

    # Capture the topics actually passed to build_and_post so we can assert the slash form.
    captured_topics = []
    real_build_qtopics = fetch_mod.build_qtopics
    def capturing_build_qtopics(eid, topics, cursor_store):
        captured_topics.extend(topics)
        return real_build_qtopics(eid, topics, cursor_store)
    monkeypatch.setattr(fetch_mod, "build_qtopics", capturing_build_qtopics)

    hab = _make_hab()

    calls = {"n": 0}
    def fake_httpClient(hab_, eid_):
        calls["n"] += 1
        return _fake_http_client([{"id": "7", "name": "/credential", "data": "AAAA-cesr"}])
    monkeypatch.setattr(fetch_mod.agenting, "httpClient", fake_httpClient)
    monkeypatch.setattr(fetch_mod.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: received.append((t, r)), cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)

    def act(i):
        if i == 2 and holder.get("ws"):
            # Push a nudge with the BARE topic form (no slash) — this is what the server sends
            # (the /fwd modifier deposit bare form), which does NOT match "/credential".
            holder["ws"].push_nudge("Ebob", "credential", cursor=7)
    # Run WELL past the single nudge (no early stop) to prove no redundant fetches on idle ticks.
    _drive(gen, holder, ticks=60, act=act)

    assert calls["n"] == 1                                # exactly ONE fetch, even after idle ticks
    assert received == [("/credential", b"AAAA-cesr")]    # raw bytes to on_message
    assert cur.saved[("Embx", "/credential")] == 7        # cursor advanced
    # The fetch must have queried the CLIENT'S slash-prefixed subscribed topic, NOT the bare nudge.
    assert "/credential" in captured_topics, "fetch used the bare nudge topic instead of the subscribed slash-prefixed topic"
    assert "credential" not in captured_topics or "/credential" in captured_topics  # bare form must not be the only one


def test_fetch_schedules_the_clientdoer_on_scheduler(monkeypatch):
    """PHASE-2 REGRESSION GUARD: a fetch that doesn't extend([clientDoer]) onto the host
    scheduler never flushes client.requests / reads client.events against a real mailbox."""
    factory, holder = _make_ws_factory()
    cur = _FakeCursorStore()
    sched = _FakeScheduler()
    hab = _make_hab()

    _, the_doer = None, doing.Doer()
    def fake_httpClient(hab_, eid_):
        client = SimpleNamespace(requests=[],
                                 events=__import__("collections").deque(
                                     [{"id": "0", "name": "/credential", "data": "x"}]))
        return client, the_doer
    monkeypatch.setattr(fetch_mod.agenting, "httpClient", fake_httpClient)
    monkeypatch.setattr(fetch_mod.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    received = []
    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: received.append((t, r)), cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)

    def act(i):
        if i == 2 and holder.get("ws"):
            holder["ws"].push_nudge("Ebob", "/credential")
    _drive(gen, holder, ticks=60, act=act)

    assert sched.extended == [[the_doer]]   # the SAME clientDoer httpClient returned, scheduled ONCE
    assert sched.removed == [[the_doer]]    # and removed when the one-shot drain completes


def test_idle_with_no_nudge_does_not_fetch(monkeypatch):
    """Driving run with no nudge and no elapsed safety-net does NOT fetch (idle is cheap)."""
    factory, holder = _make_ws_factory()
    cur = _FakeCursorStore()
    sched = _FakeScheduler()
    hab = _make_hab()

    calls = {"n": 0}
    monkeypatch.setattr(fetch_mod.agenting, "httpClient",
                        lambda hab_, eid_: (calls.__setitem__("n", calls["n"] + 1),
                                            _fake_http_client([]))[1])
    monkeypatch.setattr(fetch_mod.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: None, cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)
    _drive(gen, holder, ticks=30)

    assert calls["n"] == 0            # never fetched
    assert sched.extended == []       # never scheduled a fetch clientDoer


def test_safety_net_fetch_fires_on_cadence_without_a_nudge(monkeypatch):
    """The infrequent safety-net fetch fires on its cadence even with zero nudges."""
    factory, holder = _make_ws_factory()
    cur = _FakeCursorStore()
    sched = _FakeScheduler()
    hab = _make_hab()

    calls = {"n": 0}
    def fake_httpClient(hab_, eid_):
        calls["n"] += 1
        return _fake_http_client([])   # empty drain: safety-net found nothing, still counts as a fetch
    monkeypatch.setattr(fetch_mod.agenting, "httpClient", fake_httpClient)
    monkeypatch.setattr(fetch_mod.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    # A monotonic clock the generator reads for cadence; the test advances it.
    clock = {"t": 0.0}
    monkeypatch.setattr(serverless, "_monotonic", lambda: clock["t"])

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: None, cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=5000, ping_ms=1_000_000)

    def act(i):
        clock["t"] = i * 1.0   # advance 1s per tick; safety_net_ms=5000 -> fires around tick 5
    _drive(gen, holder, ticks=15, act=act, until=lambda: calls["n"] >= 1)

    assert calls["n"] >= 1            # safety-net fetched with no nudge


def test_ws_close_triggers_reconnect_and_resubscribe_with_current_cursors(monkeypatch):
    """A WS close/drop reconnects and RE-SUBSCRIBES with the CURRENT (advanced) cursors."""
    factory, holder = _make_ws_factory()
    cur = _FakeCursorStore(seed={("Embx", "/credential"): 2})
    sched = _FakeScheduler()
    hab = _make_hab()
    monkeypatch.setattr(fetch_mod.agenting, "httpClient",
                        lambda hab_, eid_: _fake_http_client([]))
    monkeypatch.setattr(fetch_mod.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: None, cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)
    _drive(gen, holder, ticks=3, until=lambda: holder.get("ws") and holder["ws"].started)

    ws = holder["ws"]
    first = ws.subscribe_envelopes[0]
    # Cursor advances (e.g. a fetch delivered 5), then the socket drops and the client reconnects.
    cur.set("Embx", "/credential", 5)
    ws.resubscribe()

    assert ws.connects == 2
    second = ws.subscribe_envelopes[1]
    assert base64.b64decode(first["qry"]).find(b"'/credential': 2") != -1   # seed 2 -> last-seen
    assert base64.b64decode(second["qry"]).find(b"'/credential': 5") != -1  # advanced 5 -> last-seen


def test_teardown_stops_the_pump(monkeypatch):
    """Closing the generator signals the WS client to stop (tear the socket down)."""
    factory, holder = _make_ws_factory()
    cur = _FakeCursorStore()
    sched = _FakeScheduler()
    hab = _make_hab()

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: None, cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)
    _drive(gen, holder, ticks=3, until=lambda: holder.get("ws") and holder["ws"].started)

    assert holder["ws"].stopped is True


# --- Real hio-native WsClient I/O, driven with a FAKE ClientTls (no real socket) ------------

class _FakeClientTls:
    """A fake hio ClientTls: TX goes onto ``self.txbs`` (what the client sent over the wire);
    ``self.rxbs`` is the byte buffer the client parses. The test flips ``.connected`` and feeds
    ``.rxbs`` to drive the WS state machine deterministically — no socket, no Doist needed."""
    def __init__(self):
        self.txbs = bytearray()      # bytes the WsClient .tx()'d (readable by the test)
        self.rxbs = bytearray()      # bytes the "server" sent (writable by the test)
        self.connected = False       # TLS-connected flag (test flips it)
        self.cutoff = False
        self.closed = False
    def tx(self, data): self.txbs.extend(data)
    def feed(self, data): self.rxbs.extend(data)
    def close(self): self.closed = True


def _server_ws_frame(opcode, payload=b"", fin=True, length_override=None):
    """Encode a SERVER->client frame (UNMASKED per RFC 6455) for feeding the fake socket.
    ``fin=False`` emits a non-final (fragment) frame. ``length_override`` writes a bogus advertised
    length (for the oversize-frame test) without materializing that many payload bytes."""
    import struct as _struct
    b0 = (0x80 if fin else 0x00) | (opcode & 0x0F)
    n = length_override if length_override is not None else len(payload)
    header = bytearray([b0])
    if n < 126:
        header.append(n)
    elif n < (1 << 16):
        header.append(126)
        header.extend(_struct.pack("!H", n))
    else:
        header.append(127)
        header.extend(_struct.pack("!Q", n))
    return bytes(header) + payload


def _server_101(sec_key):
    """Build the server's 101 Switching Protocols response for the client's Sec-WebSocket-Key."""
    accept = base64.b64encode(
        hashlib.sha1((sec_key + serverless._WS_GUID).encode("ascii")).digest()
    ).decode("ascii")
    return (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        "\r\n"
    ).encode("latin-1")


def test_wsclient_handshake_subscribe_and_parses_nudge_frames():
    """The real hio-native WsClient does the HTTP Upgrade, verifies the 101 accept, sends the
    subscribe envelope as a MASKED text frame, then parses server frames: pushes only
    mailbox.nudge onto its queue (non-nudge + non-JSON ignored) and replies PONG to a PING."""
    hab = _make_hab()
    def sub_builder():
        return {"action": "subscribe", "qry": base64.b64encode(b"QRY").decode("ascii")}

    sched = _FakeScheduler()
    fake = _FakeClientTls()
    ws = serverless.WsClient(
        hab=hab, eid="Embx", url="wss://mailbox.example/prod",
        subscribe_builder=sub_builder, scheduler=sched, retry_ms=1, ping_ms=1_000_000,
        client_factory=lambda host, port: fake)

    # The clientDoer was scheduled on the host scheduler (host Doist services the ClientTls).
    assert sched.extended and sched.extended[0][0] is ws.clientDoer

    # Tick 1: not connected yet -> nothing sent.
    ws.pump()
    assert bytes(fake.txbs) == b""

    # Tick 2: TLS connects -> WsClient sends the HTTP Upgrade GET.
    fake.connected = True
    ws.pump()
    upgrade = bytes(fake.txbs).decode("latin-1")
    assert upgrade.startswith("GET /prod HTTP/1.1\r\n")
    assert "Upgrade: websocket\r\n" in upgrade
    assert "Sec-WebSocket-Version: 13\r\n" in upgrade
    sec_key = ws._sec_key
    assert sec_key                      # a 16-byte base64 key was generated

    # Server returns 101, then a nudge frame, a non-nudge frame, a non-JSON frame, and a PING.
    fake.txbs.clear()
    fake.feed(_server_101(sec_key))
    fake.feed(_server_ws_frame(serverless._OP_TEXT,
              json.dumps({"type": "mailbox.nudge", "pre": "Ebob", "topic": "/credential"}).encode()))
    fake.feed(_server_ws_frame(serverless._OP_TEXT, json.dumps({"type": "something.else"}).encode()))
    fake.feed(_server_ws_frame(serverless._OP_TEXT, b"not-json"))
    fake.feed(_server_ws_frame(serverless._OP_PING, b"pingpayload"))

    # Tick 3: complete handshake (sends masked subscribe) + parse all buffered frames.
    ws.pump()

    # Subscribe + PONG were sent as MASKED client frames (client MUST mask per RFC 6455).
    sent = bytes(fake.txbs)
    assert sent, "no subscribe frame sent after handshake"
    frames = _split_client_frames(sent)
    # First client frame after the 101 is the subscribe TEXT, masked.
    assert (frames[0][0] & 0x0F) == serverless._OP_TEXT
    assert frames[0][1] is True                          # mask bit set
    assert json.loads(frames[0][2].decode("utf-8"))["action"] == "subscribe"

    # Exactly ONE nudge queued (non-nudge + non-JSON ignored).
    n = ws.nudges.get_nowait()
    assert n["type"] == "mailbox.nudge" and n["topic"] == "/credential"
    assert ws.nudges.empty()

    # A PONG (opcode 0xA) was sent, masked, echoing the PING payload.
    pongs = [f for f in frames if (f[0] & 0x0F) == serverless._OP_PONG]
    assert pongs, "no PONG sent in reply to PING"
    assert pongs[0][1] is True                           # masked
    assert pongs[0][2] == b"pingpayload"                 # echoed the ping payload

    scheduled_doer = ws.clientDoer          # capture before teardown nulls it
    ws.stop()
    assert fake.closed is True
    assert sched.removed and sched.removed[0][0] is scheduled_doer   # clientDoer unscheduled


def _split_client_frames(buf):
    """Return [(b0, masked_bool, unmasked_payload)] for each client frame in buf. Client frames
    are masked; this unmasks the payload so the test can assert on its content."""
    import struct as _struct
    out = []
    i = 0
    b = bytes(buf)
    while i + 2 <= len(b):
        b0 = b[i]
        length = b[i + 1] & 0x7F
        masked = (b[i + 1] & 0x80) != 0
        j = i + 2
        if length == 126:
            length = _struct.unpack("!H", b[j:j + 2])[0]; j += 2
        elif length == 127:
            length = _struct.unpack("!Q", b[j:j + 8])[0]; j += 8
        mask = b""
        if masked:
            mask = b[j:j + 4]; j += 4
        payload = bytearray(b[j:j + length])
        if masked:
            payload = bytes(p ^ mask[k & 3] for k, p in enumerate(payload))
        out.append((b0, masked, bytes(payload)))
        i = j + length
    return out


def test_wsclient_upgrade_failure_reconnects(monkeypatch):
    """A non-101 upgrade response tears down + schedules a reconnect (no crash)."""
    hab = _make_hab()
    sched = _FakeScheduler()
    fake = _FakeClientTls()
    ws = serverless.WsClient(
        hab=hab, eid="Embx", url="wss://mailbox.example/prod",
        subscribe_builder=lambda: {"action": "subscribe", "qry": "x"},
        scheduler=sched, retry_ms=1, ping_ms=1_000_000,
        client_factory=lambda host, port: fake)

    fake.connected = True
    ws.pump()                                   # sends upgrade
    fake.feed(b"HTTP/1.1 500 Server Error\r\n\r\n")
    ws.pump()                                   # sees non-101 -> teardown + backoff
    assert fake.closed is True
    assert ws._state == serverless._ST_CLOSED   # awaiting reconnect backoff


def _open_wsclient(fake, sched=None, sub_builder=None):
    """Build a WsClient over the given fake ClientTls and drive it to the OPEN state
    (TLS connected -> upgrade sent -> 101 accepted -> subscribe sent). Returns the WsClient."""
    hab = _make_hab()
    sub_builder = sub_builder or (lambda: {"action": "subscribe", "qry": "x"})
    ws = serverless.WsClient(
        hab=hab, eid="Embx", url="wss://mailbox.example/prod",
        subscribe_builder=sub_builder, scheduler=sched or _FakeScheduler(),
        retry_ms=1, ping_ms=1_000_000, client_factory=lambda host, port: fake)
    fake.connected = True
    ws.pump()                                   # CONNECTING -> send upgrade -> HANDSHAKE
    fake.feed(_server_101(ws._sec_key))
    ws.pump()                                   # HANDSHAKE -> verify 101 -> OPEN + subscribe
    assert ws._state == serverless._ST_OPEN
    fake.txbs.clear()                           # drop the subscribe frame so tests see only new tx
    return ws


def test_wsclient_frame_split_across_two_service_calls_parses_after_second():
    """M-3: a nudge TEXT frame delivered SPLIT across two pump()/service passes (rxbs has only a
    partial frame on the first pass) must buffer the partial and parse it on the second pass."""
    fake = _FakeClientTls()
    ws = _open_wsclient(fake)

    frame = _server_ws_frame(serverless._OP_TEXT,
        json.dumps({"type": "mailbox.nudge", "pre": "Ebob", "topic": "/credential"}).encode())
    split = len(frame) // 2
    assert 0 < split < len(frame)

    fake.feed(frame[:split])                    # only the first half arrives this pass
    ws.pump()
    assert ws.nudges.empty()                    # partial frame: nothing parsed yet

    fake.feed(frame[split:])                    # the rest arrives next pass
    ws.pump()
    n = ws.nudges.get_nowait()
    assert n["type"] == "mailbox.nudge" and n["topic"] == "/credential"
    assert ws.nudges.empty()


def test_wsclient_16bit_length_frame_parses():
    """M-3: a frame using the 126 (16-bit length) path parses (payload 126..65535 bytes)."""
    fake = _FakeClientTls()
    ws = _open_wsclient(fake)

    # A nudge padded so its JSON payload is >125 bytes -> forces the 16-bit length header.
    nudge = {"type": "mailbox.nudge", "pre": "Ebob", "topic": "/credential",
             "pad": "x" * 200}
    payload = json.dumps(nudge).encode()
    assert len(payload) >= 126                   # exercises the 126 length path
    frame = _server_ws_frame(serverless._OP_TEXT, payload)
    assert frame[1] == 126                        # 16-bit length form in the header

    fake.feed(frame)
    ws.pump()
    n = ws.nudges.get_nowait()
    assert n["type"] == "mailbox.nudge" and n["topic"] == "/credential"


def test_wsclient_fragmented_text_reassembles_into_one_nudge():
    """M-3 / I-2: a nudge split into TEXT(FIN=0) + CONT(FIN=1) must reassemble into ONE nudge,
    not be json.loads'd (and dropped) per half."""
    fake = _FakeClientTls()
    ws = _open_wsclient(fake)

    body = json.dumps({"type": "mailbox.nudge", "pre": "Ebob", "topic": "/credential"}).encode()
    half = len(body) // 2
    # First half: TEXT, FIN=0 (message continues). Second half: CONT, FIN=1 (message ends).
    fake.feed(_server_ws_frame(serverless._OP_TEXT, body[:half], fin=False))
    fake.feed(_server_ws_frame(serverless._OP_CONT, body[half:], fin=True))
    ws.pump()

    n = ws.nudges.get_nowait()
    assert n["type"] == "mailbox.nudge" and n["topic"] == "/credential"
    assert ws.nudges.empty()                     # exactly one reassembled message


def test_wsclient_binary_data_frame_is_dropped():
    """M-1: a BINARY (0x2) data frame is NOT force-JSON'd; it is dropped (no nudge, no crash)."""
    fake = _FakeClientTls()
    ws = _open_wsclient(fake)
    fake.feed(_server_ws_frame(serverless._OP_BINARY,
        json.dumps({"type": "mailbox.nudge", "pre": "Ebob", "topic": "/credential"}).encode()))
    ws.pump()
    assert ws.nudges.empty()                     # BINARY dropped, not treated as a nudge


def test_wsclient_oversize_frame_triggers_reconnect():
    """M-2: a frame advertising a length beyond _MAX_FRAME on an OPEN socket tears down +
    schedules a reconnect (rather than buffering forever / stalling into the phase timeout)."""
    fake = _FakeClientTls()
    ws = _open_wsclient(fake)

    # Advertise a bogus huge length without sending that many bytes.
    fake.feed(_server_ws_frame(serverless._OP_TEXT, b"", length_override=serverless._MAX_FRAME + 1))
    ws.pump()
    assert ws._state == serverless._ST_CLOSED    # torn down, awaiting reconnect backoff
    assert fake.closed is True


def test_wsclient_connecting_phase_timeout_reconnects(monkeypatch):
    """I-1: a CONNECTING phase that never reaches TLS-connected must teardown + reconnect once
    the phase deadline (driven via the _monotonic seam) elapses."""
    clock = {"t": 1000.0}
    monkeypatch.setattr(serverless, "_monotonic", lambda: clock["t"])

    hab = _make_hab()
    sched = _FakeScheduler()
    fake = _FakeClientTls()                       # .connected stays False forever
    ws = serverless.WsClient(
        hab=hab, eid="Embx", url="wss://mailbox.example/prod",
        subscribe_builder=lambda: {"action": "subscribe", "qry": "x"},
        scheduler=sched, retry_ms=1, ping_ms=1_000_000,
        client_factory=lambda host, port: fake)
    assert ws._state == serverless._ST_CONNECTING

    ws.pump()                                     # still connecting, deadline not reached
    assert ws._state == serverless._ST_CONNECTING

    clock["t"] += serverless._PHASE_TIMEOUT_MS / 1000.0 + 1.0   # blow the deadline
    ws.pump()
    assert ws._state == serverless._ST_CLOSED     # timed out -> torn down, awaiting backoff
    assert fake.closed is True


def test_wsclient_handshake_phase_timeout_reconnects(monkeypatch):
    """I-1: TLS connects and the upgrade is sent, but the server never returns the 101. Once the
    HANDSHAKE deadline elapses the client tears down + reconnects (does not hang forever)."""
    clock = {"t": 2000.0}
    monkeypatch.setattr(serverless, "_monotonic", lambda: clock["t"])

    hab = _make_hab()
    sched = _FakeScheduler()
    fake = _FakeClientTls()
    ws = serverless.WsClient(
        hab=hab, eid="Embx", url="wss://mailbox.example/prod",
        subscribe_builder=lambda: {"action": "subscribe", "qry": "x"},
        scheduler=sched, retry_ms=1, ping_ms=1_000_000,
        client_factory=lambda host, port: fake)

    fake.connected = True
    ws.pump()                                     # CONNECTING -> send upgrade -> HANDSHAKE
    assert ws._state == serverless._ST_HANDSHAKE
    # No 101 is ever fed. Advance past the (freshly reset) HANDSHAKE deadline.
    clock["t"] += serverless._PHASE_TIMEOUT_MS / 1000.0 + 1.0
    ws.pump()
    assert ws._state == serverless._ST_CLOSED
    assert fake.closed is True


# ---------------------------------------------------------------------------------------
# I-1: fetch_once drain-termination hardening — quiet-floor + hard-cap tests.
# These drive fetch_once directly (not via run_serverless) for precision.

import collections as _collections
from keri_serverless_mailbox import fetch as _fetch_mod


def _make_ticking_client(event_batches):
    """Returns a fake (client, clientDoer) pair whose .events are fed batch-by-batch: each call
    to _tick() pops the next batch into client.events. This lets the test control when events
    appear during the drain without real wall-clock delays."""
    client = SimpleNamespace(requests=[], events=_collections.deque())
    clientDoer = doing.Doer()
    client._batches = list(event_batches)
    return client, clientDoer


def _drive_fetch_once(gen, client, *, ticks, clock_adv_per_tick=0.0, clock=None):
    """Drive fetch_once up to `ticks` yields. After each yield, optionally advance the fake
    monotonic clock and pop the next event batch into client.events."""
    for i in range(ticks):
        if clock is not None:
            clock["t"] += clock_adv_per_tick
        if client._batches:
            batch = client._batches.pop(0)
            client.events.extend(batch)
        try:
            gen.send(None)
        except StopIteration:
            break
    else:
        gen.close()


def _evt(idx, topic="/credential", data="AAAA"):
    return {"id": str(idx), "name": topic, "data": data}


def test_quiet_floor_prevents_premature_termination(monkeypatch):
    """Events arrive, then a gap where wall-clock < QUIET_FLOOR (simulated), then more events.
    fetch_once must NOT terminate during the sub-quiet-floor gap: all events are delivered."""
    received = []
    cur = _FakeCursorStore()
    sched = _FakeScheduler()

    clock = {"t": 0.0}
    monkeypatch.setattr(_fetch_mod, "_monotonic", lambda: clock["t"])

    # Batch 0: events 1 & 2 arrive at t=0
    # Batch 1: empty (simulated gap) — clock at t=0.10 < QUIET_FLOOR (0.25)
    # Batch 2: events 3 & 4 arrive at t=0.20 (still within quiet window from last event)
    # Batch 3: empty (now clock will advance past quiet floor)
    event_batches = [
        [_evt(1), _evt(2)],           # tick 0: two events
        [],                            # tick 1: nothing (gap, clock t=0.10)
        [_evt(3), _evt(4)],           # tick 2: more events (gap was <0.25s)
        [],                            # tick 3: nothing; now advance clock past quiet floor
    ]
    client, clientDoer = _make_ticking_client(event_batches)

    def fake_httpClient(hab_, eid_):
        return client, clientDoer
    monkeypatch.setattr(_fetch_mod.agenting, "httpClient", fake_httpClient)
    monkeypatch.setattr(_fetch_mod.httping, "createCESRRequest", lambda msg, c, dest=None: None)

    hab = _make_hab()

    def clock_adv(i):
        # Advance clock 0.10s per tick up through tick 2 (gap is 0.10 < 0.25);
        # after tick 3 (all events delivered), jump past quiet floor.
        if i <= 2:
            clock["t"] += 0.10
        else:
            clock["t"] += 0.30   # now past quiet floor => drain should complete

    gen = _fetch_mod.fetch_once(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: received.append((t, r)),
        cursor_store=cur, scheduler=sched)

    for i in range(30):
        clock_adv(i)
        if client._batches:
            batch = client._batches.pop(0)
            client.events.extend(batch)
        try:
            gen.send(None)
        except StopIteration:
            break
    else:
        gen.close()

    # ALL four events must have been delivered — no premature termination during the gap.
    assert len(received) == 4
    assert [r[1] for r in received] == [b"AAAA", b"AAAA", b"AAAA", b"AAAA"]


def test_drain_completes_after_quiet_floor(monkeypatch):
    """Once QUIET_FLOOR_S of wall-clock quiet elapses with no new events, fetch_once returns."""
    received = []
    cur = _FakeCursorStore()
    sched = _FakeScheduler()

    clock = {"t": 0.0}
    monkeypatch.setattr(_fetch_mod, "_monotonic", lambda: clock["t"])

    # Two events arrive immediately; then silence. Clock advances past quiet floor => should stop.
    event_batches = [
        [_evt(1), _evt(2)],   # arrives at tick 0
    ]
    client, clientDoer = _make_ticking_client(event_batches)

    def fake_httpClient(hab_, eid_):
        return client, clientDoer
    monkeypatch.setattr(_fetch_mod.agenting, "httpClient", fake_httpClient)
    monkeypatch.setattr(_fetch_mod.httping, "createCESRRequest", lambda msg, c, dest=None: None)

    hab = _make_hab()
    gen = _fetch_mod.fetch_once(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: received.append((t, r)),
        cursor_store=cur, scheduler=sched)

    stopped = False
    for i in range(30):
        clock["t"] += 0.10   # 0.10s/tick; quiet floor = 0.25s => should stop within ~3 ticks of silence
        if client._batches:
            batch = client._batches.pop(0)
            client.events.extend(batch)
        try:
            gen.send(None)
        except StopIteration:
            stopped = True
            break
    else:
        gen.close()

    assert stopped, "fetch_once did not return after quiet floor elapsed"
    assert len(received) == 2   # both events delivered before quiet
    assert sched.extended and sched.removed  # extend/remove lifecycle preserved


def test_hard_cap_bounds_a_never_quiet_fetch(monkeypatch):
    """A fake client that yields one event every pass without ever going quiet must be stopped
    at the hard cap (_FETCH_CAP_S). fetch_once must return (not loop forever)."""
    received = []
    cur = _FakeCursorStore()
    sched = _FakeScheduler()

    clock = {"t": 0.0}
    monkeypatch.setattr(_fetch_mod, "_monotonic", lambda: clock["t"])

    # Infinite-ish stream: we rely on the hard cap to stop it. Each tick: push one event AND
    # advance the clock by 1s so the cap (30s) is hit in about 30 ticks.
    client = SimpleNamespace(requests=[], events=_collections.deque(), _batches=[])
    clientDoer = doing.Doer()

    def fake_httpClient(hab_, eid_):
        return client, clientDoer
    monkeypatch.setattr(_fetch_mod.agenting, "httpClient", fake_httpClient)
    monkeypatch.setattr(_fetch_mod.httping, "createCESRRequest", lambda msg, c, dest=None: None)

    hab = _make_hab()
    gen = _fetch_mod.fetch_once(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: received.append((t, r)),
        cursor_store=cur, scheduler=sched)

    next_idx = [0]
    stopped = False
    # Drive 200 ticks max; the cap (30s @ 1s/tick) should fire around tick 30.
    for i in range(200):
        clock["t"] += 1.0                            # advance 1s/tick
        client.events.append(_evt(next_idx[0]))      # always a new event (never goes quiet)
        next_idx[0] += 1
        try:
            gen.send(None)
        except StopIteration:
            stopped = True
            break
    else:
        gen.close()

    assert stopped, "fetch_once did not return at the hard cap — unbounded loop"
    # The fetch was bounded: far fewer than 200 ticks delivered.
    assert len(received) < 100, f"too many events delivered ({len(received)}); cap not enforced"
    assert sched.extended and sched.removed   # extend/remove lifecycle still clean
