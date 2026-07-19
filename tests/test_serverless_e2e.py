"""End-to-end conformance for the library's OWN hio-native ServerlessStrategy.

keripy's ``keri_cdk/probes/mailbox_conformance/probe.py`` proves the *server*
by driving it with a raw ``websockets`` client. That leaves the shipped
library's own WebSocket client (``serverless.WsClient`` over ``hio`` ``ClientTls``,
built after the package dropped the ``websockets`` dependency) exercised only by
unit tests with a fake, socket-less transport.

This test closes that gap: it drives the SHIPPED ``ServerlessStrategy`` /
``run_serverless`` against a *deployed* serverless mailbox and asserts a full
``subscribe -> nudge -> one-shot-drain`` round-trip delivers a deposited message
through ``on_message`` — proving the library, not a stand-in, drains a mailbox
over WSS. It also proves KERI-native discovery: the ``wss`` endpoint is learned
by resolving the mailbox OOBI, and ``discover_strategy`` selects Serverless from
the KEL alone.

Opt-in only. It is SKIPPED unless ``MAILBOX_URL`` names a deployed mailbox
(e.g. a dev stage). Because it DEPOSITS a message, it must never hit production
by accident; pointing ``MAILBOX_URL`` at the production host additionally
requires ``KSM_ALLOW_PROD=1``.

    MAILBOX_URL=https://dev.mailbox.keri.host \
        python -m pytest tests/test_serverless_e2e.py -v

Note: this package's ``pyproject`` sets ``pythonpath = ["src", "../keripy/src"]``
relative to the repo root. When running from a git worktree whose sibling is not
``../keripy``, add the real keripy ``src`` explicitly, e.g.
``PYTHONPATH=src:$HOME/code/keripy/src``.
"""
import json
import os
import tempfile
import time
import urllib.request

import pytest

from hio.base import doing

# HOME must exist before keri touches its default config path.
os.environ.setdefault("HOME", tempfile.mkdtemp(prefix="ksm-e2e-home-"))

from keri.app import Oobiery                       # noqa: E402
from keri.app.habbing import Habery                # noqa: E402
from keri.app.oobiing import Result                # noqa: E402
from keri.core.signing import Salter               # noqa: E402
from keri.help import helping                      # noqa: E402
from keri.kering import Schemes                    # noqa: E402
from keri.recording import OobiRecord              # noqa: E402

from keri_serverless_mailbox import (              # noqa: E402
    ServerlessStrategy,
    discover_strategy,
)

MAILBOX_URL = (os.environ.get("MAILBOX_URL") or "").rstrip("/")
SETTLE_S = float(os.environ.get("KSM_E2E_SETTLE_S", "10"))   # let subscribe register + GSI settle
TIMEOUT_S = float(os.environ.get("KSM_E2E_TIMEOUT_S", "45"))  # overall wait for nudge->drain
OOBI_LIMIT_S = float(os.environ.get("KSM_E2E_OOBI_S", "20"))  # OOBI resolution budget
TOCK = 0.03125
HTTP_TIMEOUT = 30

pytestmark = pytest.mark.skipif(
    not MAILBOX_URL,
    reason="set MAILBOX_URL to a deployed serverless mailbox (e.g. a dev stage) to run the library E2E",
)


# --------------------------------------------------------------------------------------------
# HTTP + KERI helpers (mirror keripy's mailbox_conformance probe, but drive the real library)
# --------------------------------------------------------------------------------------------

def _get_json(path):
    with urllib.request.urlopen(f"{MAILBOX_URL}{path}", timeout=HTTP_TIMEOUT) as r:
        return json.loads(r.read())


def _post_cesr(body):
    req = urllib.request.Request(
        f"{MAILBOX_URL}/", data=bytes(body),
        headers={"Content-Type": "application/cesr", "Accept": "application/cesr"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        return r.status, r.read()


def _make_fwd(sender_hab, recipient_pre, topic, embedded_msg):
    """Signed /fwd exn depositing ``embedded_msg`` for ``recipient_pre``'s ``topic``.

    Mirrors keripy's canonical forwarding construction (specialExchange + endorse)."""
    from keri.peer.exchanging import specialExchange
    fwd, atc = specialExchange(
        sender=sender_hab.pre,
        route="/fwd",
        modifiers={"pre": recipient_pre, "topic": topic},
        attributes={},
        embeds=dict(evt=bytes(embedded_msg)),
    )
    signed = sender_hab.endorse(serder=fwd, last=False, framed=True)
    signed.extend(atc)
    return bytes(signed)


def _resolve_oobi(hby, oobi_url):
    """Resolve ``oobi_url`` into ``hby`` (KEL + end roles + loc schemes) and return its roobi
    record. This is how a real client learns the mailbox's https + wss locs."""
    oobiery = Oobiery(hby=hby)
    hby.db.oobis.pin(keys=(oobi_url,), val=OobiRecord(date=helping.nowIso8601()))
    doist = doing.Doist(limit=OOBI_LIMIT_S, tock=TOCK)
    doist.do(doers=oobiery.doers)
    obr = hby.db.roobi.get(keys=(oobi_url,))
    doist.exit()
    return obr


class _MemCursorStore:
    """In-memory CursorStore (get(eid, topic) -> int|None ; set(eid, topic, idx))."""
    def __init__(self):
        self._d = {}

    def get(self, eid, topic):
        return self._d.get((eid, topic))

    def set(self, eid, topic, idx):
        self._d[(eid, topic)] = int(idx)


class _StrategyRunner(doing.DoDoer):
    """Mounts ``strat.run(...)`` on the host Doist exactly as ``MailboxClientDoer`` does
    (``scheduler=self``), so the strategy's WS + fetch clientDoers get serviced."""
    def __init__(self, *, strat, hab, eid, topics, on_message, cursor_store, **kwa):
        self._a = dict(strat=strat, hab=hab, eid=eid, topics=topics,
                       on_message=on_message, cursor_store=cursor_store)
        super().__init__(doers=[doing.doify(self._run)], **kwa)

    def _run(self, tymth=None, tock=0.0, **kwa):
        self.wind(tymth)
        self.tock = tock
        _ = (yield self.tock)
        a = self._a
        yield from a["strat"].run(hab=a["hab"], eid=a["eid"], topics=a["topics"],
                                  on_message=a["on_message"], cursor_store=a["cursor_store"],
                                  scheduler=self)


class _Delivered(Exception):
    """Raised from the director once a message reaches on_message, to end the Doist promptly."""


def _make_director(*, deposit_fn, received):
    """A doer that waits for the subscribe to settle, deposits one message, then watches for
    delivery. Raises _Delivered on success; returns (gives up) after TIMEOUT_S."""
    def directDo(tymth=None, tock=0.0, **kwa):
        _ = (yield tock)
        t0 = time.monotonic()
        deposited = False
        while True:
            now = time.monotonic()
            if not deposited and (now - t0) >= SETTLE_S:
                deposit_fn()
                deposited = True
            if received:
                raise _Delivered()
            if (now - t0) > TIMEOUT_S:
                return
            yield tock
    return doing.doify(directDo)


# --------------------------------------------------------------------------------------------
# the test
# --------------------------------------------------------------------------------------------

def test_library_serverless_strategy_drains_over_wss():
    status = _get_json("/")
    mailbox_pre = status.get("mailbox", "")
    ws_url = status.get("ws", "")
    assert mailbox_pre.startswith("B"), f"mailbox AID not non-transferable: {status!r}"
    assert ws_url.startswith(("wss://", "ws://")), f"status JSON lacks a ws field: {status!r}"
    assert status.get("mode") == "notify-and-fetch", f"mailbox not in notify-and-fetch mode: {status!r}"

    if "mailbox.keri.host" in MAILBOX_URL and os.environ.get("KSM_ALLOW_PROD") != "1":
        pytest.skip("refusing to DEPOSIT against the production mailbox; set KSM_ALLOW_PROD=1 to override")

    hby = Habery(name="ksm-e2e", temp=True, salt=Salter().qb64)
    try:
        alice = hby.makeHab(name="alice", transferable=True)
        bob = hby.makeHab(name="bob", transferable=True)

        # The mailbox verifies signatures against known KELs; publish both controllers' icp.
        assert _post_cesr(alice.msgOwnEvent(sn=0))[0] == 204, "alice icp publish"
        assert _post_cesr(bob.msgOwnEvent(sn=0))[0] == 204, "bob icp publish"

        # KERI-native discovery: resolve the mailbox OOBI so bob learns its https + wss locs.
        obr = _resolve_oobi(hby, f"{MAILBOX_URL}/oobi/{mailbox_pre}")
        assert obr is not None and obr.state == Result.resolved, f"mailbox OOBI unresolved: {obr!r}"
        assert bob.fetchUrl(mailbox_pre, scheme=Schemes.wss), \
            "wss loc not learned from the mailbox OOBI (fetchUrl returned empty)"

        # The library selects Serverless purely from the KEL — no out-of-band hint.
        strat = discover_strategy(bob, mailbox_pre)
        assert isinstance(strat, ServerlessStrategy), \
            f"discovery selected {type(strat).__name__}, expected ServerlessStrategy"

        received = []
        topics = ["/credential"]

        def deposit():
            # BARE topic modifier ("credential") — canonical keripy sender /fwd form. The
            # drained event name resolves to the slash-prefixed "/credential" queried below.
            embedded = bob.msgOwnEvent(sn=0)  # any opaque CESR payload
            fwd_ims = _make_fwd(alice, bob.pre, "credential", embedded)
            st, _ = _post_cesr(fwd_ims)
            assert st == 204, f"/fwd deposit returned {st}"

        runner = _StrategyRunner(
            strat=strat, hab=bob, eid=mailbox_pre, topics=topics,
            on_message=lambda tpc, raw: received.append((tpc, bytes(raw))),
            cursor_store=_MemCursorStore(),
        )
        director = _make_director(deposit_fn=deposit, received=received)

        doist = doing.Doist(tock=TOCK, real=True)
        try:
            doist.do(doers=[runner, director], limit=TIMEOUT_S + 2)
        except _Delivered:
            pass  # delivered — Doist already tore down its dogs on the raised exception

        assert received, (
            "the library's ServerlessStrategy did not receive the deposited message over WSS "
            f"within {TIMEOUT_S:.0f}s (subscribe -> nudge -> one-shot-drain did not complete)"
        )
        topic0, raw0 = received[0]
        assert topic0 == "/credential", f"delivered topic was {topic0!r}, expected '/credential'"
        assert raw0, "delivered payload was empty"
    finally:
        hby.close()
