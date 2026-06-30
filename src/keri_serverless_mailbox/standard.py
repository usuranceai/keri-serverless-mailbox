"""StandardStrategy transport: SSE/poll a mailbox EID for CESR, host parses.

Ported from Locksmith's Poller.eventDo (core/indirecting.py:320-406), with two changes:
(1) it does NOT parse — it calls on_message(topic, raw_bytes); (2) cursors live in the
host's CursorStore, not db.tops directly. Stock keripy primitives only."""
from __future__ import annotations
import datetime
import sys
import traceback

from keri.help import helping
from keri.app import agenting, httping
from keri import help, kering

logger = help.ogler.getLogger(__name__)


def run_standard(*, hab, eid, topics, on_message, cursor_store, retry_ms=1000, scheduler):
    """hio doer generator. SSE-polls eid's mailbox; per event calls
    on_message(topic, raw_cesr_bytes) and cursor_store.set(eid, topic, idx).

    scheduler is the owning hio DoDoer (the MailboxClientDoer): we .extend([clientDoer])
    after httpClient so the host Doist services it (clientDoer.recur() -> client.service()
    flushes client.requests over the wire and reads the SSE response into client.events),
    then .remove([clientDoer]) when the 30s poll window ends. Mirrors the old Poller."""
    tock = 0.0
    _ = (yield tock)
    retry = retry_ms

    while retry > 0:
        try:
            client, clientDoer = agenting.httpClient(hab, eid)
        except kering.MissingEntryError as e:
            traceback.print_exception(e, file=sys.stderr)
            yield tock
            continue

        scheduler.extend([clientDoer])    # host Doist now services clientDoer (flushes requests / reads SSE)

        # Build the mbx query from per-topic cursors (last-seen + 1, or 0 if unseen).
        q_topics = {}
        for topic in topics:
            seen = cursor_store.get(eid, topic)
            q_topics[topic] = (seen + 1) if seen is not None else 0
        q = dict(pre=hab.pre, topics=q_topics)

        mhab = getattr(hab, "mhab", None)         # GroupHab: query via the member hab
        querier = mhab if mhab is not None else hab
        msg = querier.query(pre=hab.pre, src=eid, route="mbx", query=q)
        httping.createCESRRequest(msg, client, dest=eid)

        while client.requests:
            yield tock

        created = helping.nowUTC()
        while True:
            if helping.nowUTC() - created > datetime.timedelta(seconds=30):
                break
            while client.events:
                evt = client.events.popleft()
                if "retry" in evt:
                    retry = evt["retry"]
                if "id" not in evt or "data" not in evt or "name" not in evt:
                    logger.error(f"bad mailbox event: {evt}")
                    continue
                idx, data, tpc = evt["id"], evt["data"], evt["name"]
                if idx == "" or not data or not tpc:
                    logger.error(f"bad mailbox event: {evt}")
                    continue
                on_message(tpc, data.encode("utf-8") if isinstance(data, str) else data)
                cursor_store.set(eid, tpc, int(idx))
                yield tock
            yield 0.25

        scheduler.remove([clientDoer])    # window over: stop servicing this clientDoer
        yield retry / 1000
