"""StandardStrategy transport: SSE/poll a mailbox EID for CESR, host parses.

Ported from Locksmith's Poller.eventDo (core/indirecting.py:320-406), with two changes:
(1) it does NOT parse — it calls on_message(topic, raw_bytes); (2) cursors live in the
host's CursorStore, not db.tops directly. Stock keripy primitives only."""
from __future__ import annotations
import datetime
import sys
import traceback

from keri.help import helping
from keri.app import agenting
from keri import help, kering

from .fetch import build_and_post, deliver_event

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

        try:
            # Build + POST the signed mbx query from per-topic cursors (shared with serverless).
            build_and_post(hab, eid, topics, cursor_store, client)

            while client.requests:
                yield tock

            created = helping.nowUTC()
            while True:
                if helping.nowUTC() - created > datetime.timedelta(seconds=30):
                    break
                while client.events:
                    evt = client.events.popleft()
                    delivered, override = deliver_event(evt, on_message=on_message,
                                                        cursor_store=cursor_store, eid=eid)
                    if override is not None:
                        retry = override
                    if not delivered:
                        continue
                    yield tock
                yield 0.25
        finally:
            scheduler.remove([clientDoer])    # window over: stop servicing this clientDoer

        yield retry / 1000
