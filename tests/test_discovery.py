from keri.app import habbing
from keri.kering import Schemes
from keri.recording import LocationRecord

from keri_serverless_mailbox import discover_strategy, StandardStrategy, ServerlessStrategy


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
