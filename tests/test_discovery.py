from keri.app import habbing
from keri.kering import Schemes
from keri.recording import LocationRecord

from keri_serverless_mailbox import discover_strategy, StandardStrategy, ServerlessStrategy
from keri_serverless_mailbox import strategy as strategy_module


def test_discover_standard_when_no_wss_loc():
    with habbing.openHby(name="disc1", temp=True) as hby:
        hab = hby.makeHab(name="svc")
        assert isinstance(discover_strategy(hab, hab.pre), StandardStrategy)


def test_discover_serverless_when_wss_loc_present():
    with habbing.openHby(name="disc2", temp=True) as hby:
        hab = hby.makeHab(name="svc")
        hab.db.locs.pin(keys=(hab.pre, Schemes.wss),
                        val=LocationRecord(url="wss://mailbox.example/prod"))
        assert isinstance(discover_strategy(hab, hab.pre), ServerlessStrategy)


def test_discover_degrades_to_standard_on_stock_keripy(monkeypatch):
    """Stock keripy has no Schemes.wss (_WSS is None). discover_strategy must NOT raise
    AttributeError and must fall back to Standard, even with a wss loc pinned."""
    monkeypatch.setattr(strategy_module, "_WSS", None)   # simulate stock keripy
    with habbing.openHby(name="disc3", temp=True) as hby:
        hab = hby.makeHab(name="svc")
        hab.db.locs.pin(keys=(hab.pre, Schemes.wss),
                        val=LocationRecord(url="wss://mailbox.example/prod"))
        assert isinstance(discover_strategy(hab, hab.pre), StandardStrategy)
