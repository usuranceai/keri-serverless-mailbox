"""The mailbox query builders must serialize v1 JSON.

Regression: on the v2 base, hab.query defaults to v2 CESR-native serialization, whose
Labeler rejects the '/receipt' (slash-prefixed) topic map label -> SerializeError, which
crashed the vault. The wire protocol requires slash-prefixed query topics, so v1 JSON is
the compatible serialization.
"""
from types import SimpleNamespace

from keri.app import habbing
from keri.core import signing
from keri.kering import Vrsn_1_0

from keri_serverless_mailbox import fetch as fetch_mod
from keri_serverless_mailbox import serverless as serverless_mod


def _v1_hab():
    hby = habbing.Habery(name="q", bran="A" * 21,
                         salt=signing.Salter(raw=b"0123456789abcdef").qb64,
                         temp=True, version=Vrsn_1_0)
    hab = hby.makeHab(name="a", isith="1", icount=1, transferable=True,
                      version=Vrsn_1_0)
    return hby, hab


def test_build_and_post_serializes_v1_json(monkeypatch):
    hby, hab = _v1_hab()
    try:
        captured = {}
        monkeypatch.setattr(fetch_mod.httping, "createCESRRequest",
                            lambda msg, client, dest: captured.__setitem__("msg", bytes(msg)))
        cursor_store = SimpleNamespace(get=lambda *a, **k: None)
        # Would raise SerializeError today (v2 default) on the '/receipt' label.
        fetch_mod.build_and_post(hab, hab.pre, ["/receipt", "/replay"],
                                 cursor_store, client=object())
        assert captured["msg"].startswith(b'{"v":"KERI10JSON'), captured["msg"][:24]
    finally:
        hby.close()


def test_subscribe_builder_serializes_v1_json(monkeypatch):
    """Drive run_serverless via its injectable ws_factory seam so the REAL
    _subscribe_builder (serverless.py:113) runs; assert the envelope qry is v1 JSON."""
    import base64
    import pytest

    hby, hab = _v1_hab()
    try:
        monkeypatch.setattr(hab, "fetchUrl", lambda eid, scheme=None: "wss://x")
        captured = {}

        class _Stop(Exception):
            pass

        def fake_ws_factory(*, hab, eid, url, subscribe_builder, **kw):
            captured["env"] = subscribe_builder()   # exercises serverless.py:113
            raise _Stop()

        gen = serverless_mod.run_serverless(
            hab=hab, eid="Embx", topics=["/receipt"],
            on_message=lambda t, r: None,
            cursor_store=SimpleNamespace(get=lambda *a, **k: None),
            scheduler=SimpleNamespace(extend=lambda d: None, remove=lambda d: None),
            ws_factory=fake_ws_factory)
        next(gen)                       # advance past the initial `yield tock`
        with pytest.raises(_Stop):
            gen.send(0.0)               # reach ws_factory -> subscribe_builder() -> _Stop
        qry = base64.b64decode(captured["env"]["qry"])
        assert qry.startswith(b'{"v":"KERI10JSON'), qry[:24]
    finally:
        hby.close()
