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


def test_poller_swallows_strategy_error(monkeypatch):
    """A poll-cycle exception must not escape the doer into the host Doist.

    Regression: an unpinned v2 qry raised SerializeError inside strategy.run; it
    propagated out of the vault Doist and closed the whole vault db.
    """
    from hio.base import doing
    from keri_serverless_mailbox import MailboxClient, client as client_mod

    class BoomStrategy:
        def run(self, **kw):
            raise RuntimeError("boom")
            yield  # unreachable; makes run() a generator

    mc = MailboxClient(SimpleNamespace(pre="Edoi"), topics=["/x"],
                       on_message=lambda t, r: None,
                       cursor_store=SimpleNamespace(get=lambda *a, **k: None))
    monkeypatch.setattr(mc, "resolve", lambda: "Embx")
    monkeypatch.setattr(mc, "strategy_for", lambda eid: BoomStrategy())

    doer = client_mod.MailboxClientDoer(mc)
    doist = doing.Doist(doers=[doer], limit=1.0, tock=0.1, real=False)
    doist.do()  # must NOT raise; the doer stops itself after logging
