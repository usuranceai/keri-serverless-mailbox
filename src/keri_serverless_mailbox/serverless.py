"""ServerlessStrategy transport: WebSocket notify-and-fetch.

Two channels, two transports:

  * NUDGE channel = WebSocket (the ``wss://`` loc for the mailbox EID, discovered KEL-native
    via ``hab.fetchUrl(eid, scheme=Schemes.wss)``). One idle socket; the server pushes tiny
    JSON nudges ``{"type":"mailbox.nudge","pre":...,"topic":...}``. The nudge carries NO CESR.
  * FETCH channel = ordinary HTTP (``agenting.httpClient(hab, eid)`` — the SAME path
    ``run_standard`` uses). On a nudge (or the infrequent safety-net timer) the client does ONE
    signed ``qry r=/mbx`` fetch and reads the drain the server produces (drain-and-close).

Testability is the Phase-2 lesson: ALL scheduling / fetch LOGIC lives in the hio generator
(``run_serverless`` — the tested unit); the WebSocket I/O is a thin, INJECTABLE pump running on
a background thread. Tests inject a fake pump whose nudge queue is fed synchronously, so the
generator is driven deterministically with no real socket and no thread-timing flakiness.

Stock keripy / hio + ``websockets`` only. Host-agnostic (no Locksmith, no Qt)."""
from __future__ import annotations

import base64
import json
import queue
import threading
import time

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


def run_serverless(*, hab, eid, topics, on_message, cursor_store, retry_ms=1000, scheduler,
                   ws_factory=None, safety_net_ms=DEFAULT_SAFETY_NET_MS, ping_ms=DEFAULT_PING_MS):
    """hio doer generator. Opens one idle WS via the pump, then on each nudge (or the
    safety-net cadence) performs ONE signed-qry HTTP fetch through the shared fetch core.

    ``scheduler`` is the owning hio DoDoer; the fetch schedules its clientDoer on it
    (extend/remove) exactly like ``run_standard`` — otherwise nothing flushes over the wire.

    ``ws_factory(**kwargs) -> pump`` is injectable: the default opens a real background-thread
    ``websockets`` pump; tests inject a fake pump whose nudge queue they feed synchronously.
    A pump exposes: ``.start()``, ``.stop()``, and ``.nudges`` (a thread-safe ``queue.Queue``)."""
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

    pump = ws_factory(hab=hab, eid=eid, url=url, subscribe_builder=_subscribe_builder,
                      retry_ms=retry_ms, ping_ms=ping_ms)
    pump.start()

    last_safety = _monotonic()
    try:
        while True:
            fetch_topics = set()

            # Drain any nudges the pump pushed onto the thread-safe queue.
            while True:
                try:
                    nudge = pump.nudges.get_nowait()
                except queue.Empty:
                    break
                topic = nudge.get("topic")
                if topic:
                    fetch_topics.add(topic)
                else:
                    # Intentional: a topic-less nudge (server doesn't know which topic changed)
                    # fans out to ALL subscribed topics so nothing is missed.
                    fetch_topics.update(topics)   # topic-less nudge => refetch all

            # Safety-net: an infrequent backstop to catch a missed nudge.
            now = _monotonic()
            if (now - last_safety) * 1000.0 >= safety_net_ms:
                last_safety = now
                fetch_topics.update(topics)

            if fetch_topics:
                # ONE fetch per wake, covering the union of nudged/safety topics.
                yield from fetch_once(hab=hab, eid=eid, topics=list(fetch_topics),
                                      on_message=on_message, cursor_store=cursor_store,
                                      scheduler=scheduler)

            yield tock
    finally:
        pump.stop()


# --------------------------------------------------------------------------------------------
# Default WS pump: a thin I/O component on a background thread. Not exercised by unit tests
# (they inject a fake); kept small and free of fetch/scheduling logic (that lives in the doer).

class WsPump:
    """Idle-WebSocket thread pump. On start it connects, sends the subscribe envelope, then
    receives frames pushing each ``mailbox.nudge`` onto ``self.nudges``. Sends WS pings more
    often than the ~10-min idle timeout; on close/drop reconnects with exponential backoff and
    RE-SUBSCRIBES with the current cursors (``subscribe_builder`` is re-evaluated each connect).

    ``connect_factory(url) -> connection`` is injectable so even this class is testable without
    a real socket; the default uses ``websockets.sync.client.connect``. A connection must expose
    blocking ``send(str)`` / ``recv(timeout)`` / ``ping()`` / ``close()`` (the ``websockets``
    sync client surface)."""

    def __init__(self, *, hab, eid, url, subscribe_builder, retry_ms=1000, ping_ms=DEFAULT_PING_MS,
                 connect_factory=None):
        self.hab = hab
        self.eid = eid
        self.url = url
        self.subscribe_builder = subscribe_builder
        self.retry_ms = retry_ms
        self.ping_ms = ping_ms
        self.nudges: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = None
        self._connect_factory = connect_factory or _default_connect

    def start(self):
        self._thread = threading.Thread(target=self._run, name="mbx-ws-pump", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=5.0)

    def _run(self):
        backoff = max(self.retry_ms, 1) / 1000.0
        while not self._stop.is_set():
            try:
                conn = self._connect_factory(self.url)
            except Exception as ex:   # noqa: BLE001 -- reconnect on any connect failure
                logger.error(f"ws connect to {self.url} failed: {ex}; backoff {backoff:.1f}s")
                if self._stop.wait(backoff):
                    return
                backoff = min(backoff * 2, _RECONNECT_CAP_MS / 1000.0)
                continue

            backoff = max(self.retry_ms, 1) / 1000.0   # reset on a good connect
            try:
                conn.send(json.dumps(self.subscribe_builder()))   # (re)subscribe w/ current cursors
                self._pump(conn)
            except Exception as ex:   # noqa: BLE001 -- any drop -> reconnect+resubscribe
                logger.info(f"ws pump loop ended for {self.url}: {ex}; reconnecting")
            finally:
                try:
                    conn.close()
                except Exception:   # noqa: BLE001
                    pass
            if self._stop.wait(backoff):
                return
            backoff = min(backoff * 2, _RECONNECT_CAP_MS / 1000.0)

    def _pump(self, conn):
        recv_timeout = min(self.ping_ms / 1000.0, 30.0)
        last_ping = _monotonic()
        while not self._stop.is_set():
            try:
                frame = conn.recv(timeout=recv_timeout)
            except TimeoutError:
                frame = None
            if frame is not None:
                self._handle_frame(frame)
            if (_monotonic() - last_ping) * 1000.0 >= self.ping_ms:
                conn.ping()                    # free keep-alive control frame
                last_ping = _monotonic()

    def _handle_frame(self, frame):
        try:
            obj = json.loads(frame)
        except (ValueError, TypeError):
            logger.error(f"non-JSON ws frame ignored: {frame!r}")
            return
        if isinstance(obj, dict) and obj.get("type") == "mailbox.nudge":
            self.nudges.put(obj)


def _default_connect(url):
    from websockets.sync.client import connect as ws_connect
    return ws_connect(url)


def _default_ws_factory(*, hab, eid, url, subscribe_builder, retry_ms, ping_ms):
    return WsPump(hab=hab, eid=eid, url=url, subscribe_builder=subscribe_builder,
                  retry_ms=retry_ms, ping_ms=ping_ms)
