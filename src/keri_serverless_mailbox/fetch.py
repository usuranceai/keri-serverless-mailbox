"""Shared mbx fetch core, reused by run_standard (SSE poll) and run_serverless (one-shot).

Both strategies build the SAME signed ``qry r=/mbx`` from per-topic cursors and schedule the
same keripy ``httpClient`` clientDoer on the host scheduler so the host Doist flushes the
request over the wire and reads the response into ``client.events``. They differ ONLY in the
read-loop termination:

  * ``run_standard`` holds a 30s poll window (the held-SSE mailbox re-drips events).
  * ``run_serverless`` (``fetch_once`` here) reads the drain the Task-3 server produces and
    stops when the backlog is exhausted (drain-and-close).

So we share the qry-build + per-event delivery + the extend/read/remove lifecycle, and keep the
serverless one-shot termination here; ``run_standard`` keeps its own 30s window unchanged."""
from __future__ import annotations

import sys
import traceback

from keri.app import agenting, httping
from keri import help, kering

logger = help.ogler.getLogger(__name__)


def build_qtopics(eid, topics, cursor_store):
    """Per-topic query cursors: last-seen + 1, or 0 if never seen. Identical to what the
    Poller / run_standard builds."""
    q_topics = {}
    for topic in topics:
        seen = cursor_store.get(eid, topic)
        q_topics[topic] = (seen + 1) if seen is not None else 0
    return q_topics


def deliver_event(evt, *, on_message, cursor_store, eid):
    """Validate one mailbox SSE event dict, deliver its raw CESR bytes to on_message, and
    advance the cursor. Returns ``(delivered, retry_override)``: ``delivered`` is True only when
    a well-formed event was actually delivered; ``retry_override`` is the event's ``retry`` value
    if present, else None. Shared by both strategies so delivery/validation stays identical."""
    retry = evt["retry"] if "retry" in evt else None
    if "id" not in evt or "data" not in evt or "name" not in evt:
        logger.error(f"bad mailbox event: {evt}")
        return False, retry
    idx, data, tpc = evt["id"], evt["data"], evt["name"]
    if idx == "" or not data or not tpc:
        logger.error(f"bad mailbox event: {evt}")
        return False, retry
    on_message(tpc, data.encode("utf-8") if isinstance(data, str) else data)
    cursor_store.set(eid, tpc, int(idx))
    return True, retry


def build_and_post(hab, eid, topics, cursor_store, client):
    """Build the signed mbx qry from cursors and POST it onto ``client`` as a CESR request."""
    q_topics = build_qtopics(eid, topics, cursor_store)
    q = dict(pre=hab.pre, topics=q_topics)
    mhab = getattr(hab, "mhab", None)             # GroupHab: query via the member hab
    querier = mhab if mhab is not None else hab
    msg = querier.query(pre=hab.pre, src=eid, route="mbx", query=q)
    httping.createCESRRequest(msg, client, dest=eid)


def fetch_once(*, hab, eid, topics, on_message, cursor_store, scheduler):
    """hio doer generator: perform ONE signed-qry fetch and read the drain until it completes.

    Schedules the httpClient clientDoer on ``scheduler`` (extend/remove) exactly like
    ``run_standard`` — this is what makes the fetch actually flush over the wire; the Phase-2
    regression was a fetch that never scheduled. Drains-and-stops (the Task-3 server closes the
    response once the backlog is exhausted): reads events until, after requests have flushed,
    the event queue is empty on a fresh service pass."""
    tock = 0.0
    try:
        client, clientDoer = agenting.httpClient(hab, eid)
    except kering.MissingEntryError as ex:
        traceback.print_exception(ex, file=sys.stderr)
        return

    scheduler.extend([clientDoer])    # host Doist now services clientDoer (flush request / read drain)
    try:
        build_and_post(hab, eid, topics, cursor_store, client)

        while client.requests:        # wait for the signed qry to flush over the wire
            yield tock

        # Drain: read events until the response is exhausted. The server drains-and-closes, so
        # once requests are done AND no more events arrive on a service pass, the drain is over.
        idle_passes = 0
        while True:
            drained_any = False
            while client.events:
                evt = client.events.popleft()
                deliver_event(evt, on_message=on_message, cursor_store=cursor_store, eid=eid)
                drained_any = True          # a frame arrived => the drain isn't finished
                yield tock
            if drained_any:
                idle_passes = 0
            else:
                idle_passes += 1
                # No events this pass. Give the client one more service pass to confirm the
                # drain is truly empty (the response has closed), then stop.
                if idle_passes >= 2:
                    break
            yield tock
    finally:
        scheduler.remove([clientDoer])    # one-shot done: stop servicing this clientDoer
