"""Tests for the Serverless (WebSocket notify-and-fetch) strategy. NO real network.

The generator (the tested unit) owns ALL scheduling/fetch logic; the WebSocket I/O is a
thin, injectable pump. Tests inject a FAKE pump factory whose nudge queue is preloaded
synchronously, so the hio generator is driven deterministically with no real socket and
no thread-timing flakiness."""
import base64
import json
import queue
import time
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


class _FakePump:
    """Injected in place of the real WS thread pump. Records subscribe envelopes and lets
    the test push nudges onto the (thread-safe) queue synchronously; no real socket/thread."""
    def __init__(self, *, hab, eid, url, subscribe_builder, retry_ms, ping_ms):
        self.hab = hab
        self.eid = eid
        self.url = url
        self.subscribe_builder = subscribe_builder
        self.retry_ms = retry_ms
        self.ping_ms = ping_ms
        self.nudges = queue.Queue()
        self.started = False
        self.stopped = False
        self.subscribe_envelopes = []      # every envelope the pump would send
        self.connects = 0                  # how many times it (re)connected+subscribed

    def start(self):
        self.started = True
        self._connect()

    def _connect(self):
        # A real connect builds + sends the subscribe envelope with current cursors.
        self.connects += 1
        self.subscribe_envelopes.append(self.subscribe_builder())

    def resubscribe(self):
        """Simulate a reconnect: the pump re-sends the subscribe envelope w/ CURRENT cursors."""
        self._connect()

    def push_nudge(self, pre, topic, cursor=None):
        self.nudges.put({"type": "mailbox.nudge", "pre": pre, "topic": topic, "cursor": cursor})

    def stop(self):
        self.stopped = True


def _make_pump_factory():
    holder = {}
    def factory(**kwa):
        pump = _FakePump(**kwa)
        holder["pump"] = pump
        return pump
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
    send (to push nudges / close sockets); stop early when `until()` is true. Bounded so a
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
    signed /mbx qry built from the current cursors (seen+1, or 0 if unseen)."""
    factory, holder = _make_pump_factory()
    # /credential seen at 4 -> query from 5; /receipt unseen -> query from 0.
    cur = _FakeCursorStore(seed={("Embx", "/credential"): 4})
    sched = _FakeScheduler()
    hab = _make_hab()

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential", "/receipt"],
        on_message=lambda t, r: None, cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)

    _drive(gen, holder, ticks=3, until=lambda: holder.get("pump") and holder["pump"].started)

    pump = holder["pump"]
    assert pump.started
    env = pump.subscribe_envelopes[0]
    assert env["action"] == "subscribe"
    decoded = base64.b64decode(env["qry"])
    assert decoded.startswith(b"SIGNED-MBX-QRY:")
    # The qry-build kwargs are embedded (our fake query echoes them): route=mbx, cursors applied.
    assert b"'route': 'mbx'" in decoded
    assert b"'/credential': 5" in decoded    # seen 4 -> +1
    assert b"'/receipt': 0" in decoded       # unseen -> 0


def test_nudge_triggers_exactly_one_fetch_and_advances_cursor(monkeypatch):
    received = []
    factory, holder = _make_pump_factory()
    cur = _FakeCursorStore()
    sched = _FakeScheduler()
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
        if i == 2 and holder.get("pump"):
            holder["pump"].push_nudge("Ebob", "/credential", cursor=7)
    # Run WELL past the single nudge (no early stop) to prove no redundant fetches on idle ticks.
    _drive(gen, holder, ticks=60, act=act)

    assert calls["n"] == 1                                # exactly ONE fetch, even after idle ticks
    assert received == [("/credential", b"AAAA-cesr")]    # raw bytes to on_message
    assert cur.saved[("Embx", "/credential")] == 7        # cursor advanced


def test_fetch_schedules_the_clientdoer_on_scheduler(monkeypatch):
    """PHASE-2 REGRESSION GUARD: a fetch that doesn't extend([clientDoer]) onto the host
    scheduler never flushes client.requests / reads client.events against a real mailbox."""
    factory, holder = _make_pump_factory()
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
        if i == 2 and holder.get("pump"):
            holder["pump"].push_nudge("Ebob", "/credential")
    _drive(gen, holder, ticks=60, act=act)

    assert sched.extended == [[the_doer]]   # the SAME clientDoer httpClient returned, scheduled ONCE
    assert sched.removed == [[the_doer]]    # and removed when the one-shot drain completes


def test_idle_with_no_nudge_does_not_fetch(monkeypatch):
    """Driving run with no nudge and no elapsed safety-net does NOT fetch (idle is cheap)."""
    factory, holder = _make_pump_factory()
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
    factory, holder = _make_pump_factory()
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
    factory, holder = _make_pump_factory()
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
    _drive(gen, holder, ticks=3, until=lambda: holder.get("pump") and holder["pump"].started)

    pump = holder["pump"]
    first = pump.subscribe_envelopes[0]
    # Cursor advances (e.g. a fetch delivered 5), then the socket drops and the pump reconnects.
    cur.set("Embx", "/credential", 5)
    pump.resubscribe()

    assert pump.connects == 2
    second = pump.subscribe_envelopes[1]
    assert base64.b64decode(first["qry"]).find(b"'/credential': 3") != -1   # seed 2 -> +1
    assert base64.b64decode(second["qry"]).find(b"'/credential': 6") != -1  # advanced 5 -> +1


def test_teardown_stops_the_pump(monkeypatch):
    """Closing the generator signals the pump to stop (never leak the WS thread)."""
    factory, holder = _make_pump_factory()
    cur = _FakeCursorStore()
    sched = _FakeScheduler()
    hab = _make_hab()

    gen = serverless.run_serverless(
        hab=hab, eid="Embx", topics=["/credential"],
        on_message=lambda t, r: None, cursor_store=cur, retry_ms=1,
        scheduler=sched, ws_factory=factory, safety_net_ms=10_000_000, ping_ms=1_000_000)
    _drive(gen, holder, ticks=3, until=lambda: holder.get("pump") and holder["pump"].started)

    assert holder["pump"].stopped is True


# --- Real WsPump I/O logic, driven with a FAKE connection (no real socket / no flakiness) ---

class _FakeConn:
    """A fake websockets sync connection: yields pre-scripted frames from recv() then stops the
    pump. Records what was sent + whether it was closed. No network, no thread timing games."""
    def __init__(self, frames, stop_event):
        self._frames = list(frames)
        self._stop = stop_event
        self.sent = []
        self.closed = False
        self.pinged = 0
    def send(self, s): self.sent.append(s)
    def recv(self, timeout=None):
        if self._frames:
            return self._frames.pop(0)
        self._stop.set()           # frames exhausted -> let the pump loop exit deterministically
        raise TimeoutError()
    def ping(self): self.pinged += 1
    def close(self): self.closed = True


def test_wspump_subscribes_and_parses_nudge_frames():
    """The real WsPump sends the subscribe envelope on connect and pushes only mailbox.nudge
    frames onto its queue (a non-nudge frame is ignored). Exercised via an injected fake conn."""
    hab = _make_hab()
    def sub_builder():
        return {"action": "subscribe", "qry": base64.b64encode(b"QRY").decode("ascii")}

    made = {}
    def connect_factory(url):
        conn = _FakeConn(
            frames=[json.dumps({"type": "mailbox.nudge", "pre": "Ebob", "topic": "/credential"}),
                    json.dumps({"type": "something.else"}),   # ignored (not a nudge)
                    "not-json"],                              # ignored (non-JSON), logged
            stop_event=made["stop"])
        made["conn"] = conn
        return conn

    pump = serverless.WsPump(hab=hab, eid="Embx", url="wss://mailbox.example/prod",
                             subscribe_builder=sub_builder, retry_ms=1, ping_ms=1_000_000,
                             connect_factory=connect_factory)
    made["stop"] = pump._stop
    pump.start()
    # Wait (bounded) for the pump to process frames + exit its loop; then stop/join.
    made_conn = None
    for _ in range(200):
        made_conn = made.get("conn")
        if made_conn is not None and made_conn.closed:
            break
        time.sleep(0.01)
    pump.stop()

    assert made_conn is not None
    # Subscribe envelope sent on connect.
    assert json.loads(made_conn.sent[0])["action"] == "subscribe"
    # Exactly ONE nudge queued (the non-nudge and the non-JSON frame were ignored).
    n = pump.nudges.get_nowait()
    assert n["type"] == "mailbox.nudge" and n["topic"] == "/credential"
    assert pump.nudges.empty()
    assert made_conn.closed is True
