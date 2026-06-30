from types import SimpleNamespace
from hio.base import doing

from keri_serverless_mailbox import standard


class _FakeCursorStore:
    def __init__(self): self.saved = {}
    def get(self, eid, topic): return self.saved.get((eid, topic))
    def set(self, eid, topic, idx): self.saved[(eid, topic)] = idx


def test_run_standard_delivers_events_and_advances_cursor(monkeypatch):
    received = []
    cur = _FakeCursorStore()

    # Fake the keripy transport: httpClient returns a client whose .events yields one SSE
    # event then drains; .requests empties immediately.
    class _Client:
        def __init__(self):
            self.requests = []
            self.events = __import__("collections").deque(
                [{"id": "0", "name": "/credential", "data": "AAAA-cesr"}])
    fake_client, fake_doer = _Client(), doing.Doer()
    monkeypatch.setattr(standard.agenting, "httpClient", lambda hab, eid: (fake_client, fake_doer))
    monkeypatch.setattr(standard.httping, "createCESRRequest", lambda msg, client, dest=None: None)

    hab = SimpleNamespace(pre="Edoi", query=lambda **kw: b"qry",
                          db=SimpleNamespace(tops=SimpleNamespace(get=lambda k: None)))
    gen = standard.run_standard(hab=hab, eid="Embx", topics=["/credential"],
                                on_message=lambda topic, raw: received.append((topic, raw)),
                                cursor_store=cur, retry_ms=1)
    # Drive the generator enough to process the queued event (bounded so the test can't hang).
    g = gen
    for _ in range(50):
        try: g.send(None) if received == [] else g.close()
        except StopIteration: break
        if received: break

    assert received and received[0][0] == "/credential"
    assert received[0][1] == b"AAAA-cesr"            # raw CESR bytes, NOT parsed
    assert cur.saved[("Embx", "/credential")] == 0   # cursor advanced to the event id
