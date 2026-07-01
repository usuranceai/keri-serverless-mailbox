from types import SimpleNamespace
from hio.base import doing

from keri_serverless_mailbox import standard
from keri_serverless_mailbox import fetch as fetch_mod   # createCESRRequest now lives in the shared fetch core


class _FakeCursorStore:
    def __init__(self): self.saved = {}
    def get(self, eid, topic): return self.saved.get((eid, topic))
    def set(self, eid, topic, idx): self.saved[(eid, topic)] = idx


class _FakeScheduler:
    """Stands in for the owning DoDoer (MailboxClientDoer). Records extend/remove calls
    so the test can assert the transport clientDoer actually gets scheduled."""
    def __init__(self):
        self.extended = []   # list of doer-lists passed to extend()
        self.removed = []    # list of doer-lists passed to remove()
    def extend(self, doers): self.extended.append(list(doers))
    def remove(self, doers): self.removed.append(list(doers))


def test_run_standard_delivers_events_and_advances_cursor(monkeypatch):
    received = []
    cur = _FakeCursorStore()
    sched = _FakeScheduler()

    # Fake the keripy transport: httpClient returns a client whose .events yields one SSE
    # event then drains; .requests empties immediately.
    class _Client:
        def __init__(self):
            self.requests = []
            self.events = __import__("collections").deque(
                [{"id": "0", "name": "/credential", "data": "AAAA-cesr"}])
    fake_client, fake_doer = _Client(), doing.Doer()
    monkeypatch.setattr(standard.agenting, "httpClient", lambda hab, eid: (fake_client, fake_doer))
    monkeypatch.setattr(fetch_mod.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    hab = SimpleNamespace(pre="Edoi", query=lambda **kw: b"qry",
                          db=SimpleNamespace(tops=SimpleNamespace(get=lambda k: None)))
    gen = standard.run_standard(hab=hab, eid="Embx", topics=["/credential"],
                                on_message=lambda topic, raw: received.append((topic, raw)),
                                cursor_store=cur, retry_ms=1, scheduler=sched)
    # Drive the generator enough to process the queued event (bounded so the test can't hang).
    g = gen
    for _ in range(50):
        try: g.send(None) if received == [] else g.close()
        except StopIteration: break
        if received: break

    assert received and received[0][0] == "/credential"
    assert received[0][1] == b"AAAA-cesr"            # raw CESR bytes, NOT parsed
    assert cur.saved[("Embx", "/credential")] == 0   # cursor advanced to the event id

    # REGRESSION: the SSE clientDoer MUST be scheduled on the host before events are read.
    # Without scheduler.extend([clientDoer]), client.service() never runs against a real
    # mailbox -> client.requests never flushes and client.events never fills.
    assert sched.extended == [[fake_doer]]           # the SAME clientDoer httpClient returned


def test_run_standard_unschedules_clientdoer_when_window_ends(monkeypatch):
    """When the 30s poll window closes (here: forced by exhausting one bounded iteration),
    the clientDoer is removed from the scheduler before the retry yield -- mirrors the old
    Poller's self.remove([clientDoer]) at the window break."""
    cur = _FakeCursorStore()
    sched = _FakeScheduler()

    class _Client:
        def __init__(self):
            self.requests = []
            self.events = __import__("collections").deque(
                [{"id": "0", "name": "/credential", "data": "AAAA-cesr"}])
    fake_client, fake_doer = _Client(), doing.Doer()
    monkeypatch.setattr(standard.agenting, "httpClient", lambda hab, eid: (fake_client, fake_doer))
    monkeypatch.setattr(fetch_mod.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    # Force the 30s window to be "already elapsed" so the inner loop breaks on first check,
    # exercising scheduler.remove([clientDoer]) deterministically.
    import datetime as _dt
    real_now = standard.helping.nowUTC()
    times = iter([real_now, real_now + _dt.timedelta(seconds=31)])
    monkeypatch.setattr(standard.helping, "nowUTC", lambda: next(times, real_now + _dt.timedelta(seconds=31)))

    hab = SimpleNamespace(pre="Edoi", query=lambda **kw: b"qry",
                          db=SimpleNamespace(tops=SimpleNamespace(get=lambda k: None)))
    gen = standard.run_standard(hab=hab, eid="Embx", topics=["/credential"],
                                on_message=lambda topic, raw: None,
                                cursor_store=cur, retry_ms=1, scheduler=sched)
    g = gen
    for _ in range(50):
        try:
            g.send(None)
        except StopIteration:
            break
        if sched.removed:   # window has closed and clientDoer removed
            g.close()
            break

    assert sched.extended == [[fake_doer]]   # scheduled at the top of the retry iteration
    assert sched.removed == [[fake_doer]]    # and removed when the window ended
