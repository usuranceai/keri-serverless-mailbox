from types import SimpleNamespace
from keri_serverless_mailbox import MailboxClient, StandardStrategy, client as client_mod


def test_client_resolves_and_selects_strategy(monkeypatch):
    monkeypatch.setattr(client_mod.agenting, "mailbox", lambda hab, cid: "Embx")
    hab = SimpleNamespace(pre="Edoi", fetchUrl=lambda eid, scheme="": "")   # no wss -> Standard
    mc = MailboxClient(hab, topics=["/credential"], on_message=lambda t, r: None,
                       cursor_store=SimpleNamespace(get=lambda e, t: None, set=lambda e, t, i: None))
    assert mc.resolve() == "Embx"
    assert isinstance(mc.strategy_for("Embx"), StandardStrategy)


def test_client_doer_noop_when_no_mailbox(monkeypatch):
    monkeypatch.setattr(client_mod.agenting, "mailbox", lambda hab, cid: None)
    hab = SimpleNamespace(pre="Edoi", fetchUrl=lambda eid, scheme="": "")
    mc = MailboxClient(hab, topics=[], on_message=lambda t, r: None,
                       cursor_store=SimpleNamespace(get=lambda e, t: None, set=lambda e, t, i: None))
    assert mc.resolve() is None      # no crash; the doer will simply not poll
