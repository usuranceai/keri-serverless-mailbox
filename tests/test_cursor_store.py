"""Unit tests for the promoted DbTopsCursorStore (durable per-(pre, eid, topic) cursor)."""
from keri.app import habbing

from keri_serverless_mailbox import DbTopsCursorStore


def test_roundtrip_per_eid_topic():
    with habbing.openHby(name="cst", temp=True) as hby:
        store = DbTopsCursorStore(hby.db, "Epre1")
        assert store.get("Eeid1", "/credential") is None      # unseen -> None
        store.set("Eeid1", "/credential", 5)
        assert store.get("Eeid1", "/credential") == 5          # round-trips


def test_eids_and_topics_are_isolated():
    with habbing.openHby(name="cst2", temp=True) as hby:
        store = DbTopsCursorStore(hby.db, "Epre1")
        store.set("Eeid1", "/credential", 5)
        store.set("Eeid1", "/receipt", 9)
        store.set("Eeid2", "/credential", 2)
        assert store.get("Eeid1", "/credential") == 5
        assert store.get("Eeid1", "/receipt") == 9             # sibling topic unaffected
        assert store.get("Eeid2", "/credential") == 2          # sibling eid unaffected
        assert store.get("Eeid1", "/reply") is None            # untouched topic -> None


def test_set_overwrites_same_key():
    with habbing.openHby(name="cst3", temp=True) as hby:
        store = DbTopsCursorStore(hby.db, "Epre1")
        store.set("Eeid1", "/credential", 5)
        store.set("Eeid1", "/credential", 12)
        assert store.get("Eeid1", "/credential") == 12
