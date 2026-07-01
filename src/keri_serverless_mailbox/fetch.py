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
import time
import traceback

from keri.app import agenting, httping
from keri import help, kering

logger = help.ogler.getLogger(__name__)

# Indirected through the module so tests can monkeypatch a deterministic clock.
_monotonic = time.monotonic

# Quiet-floor: how long wall-clock quiet (no new events) the drain loop waits before declaring
# the backlog exhausted. Mirrors run_standard's yield 0.25 cadence.
_QUIET_FLOOR_S = 0.25

# Hard cap on the total wall-clock time a single fetch_once drain may spend. If the server
# trickles events without going quiet, we stop at the cap and let the next nudge / 5-min
# safety-net resume from the advanced cursor.
_FETCH_CAP_S = 30.0


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
    regression was a fetch that never scheduled.

    **Drain termination — §5.4 drain-and-close contract assumed:**
    The Task-3 server closes the SSE response once the backlog is exhausted. hio's
    ``client.responses`` does not expose a clean end-of-stream signal, so termination is driven
    by a QUIET-FLOOR heuristic: the drain is considered complete once ``_QUIET_FLOOR_S`` seconds
    of wall-clock time have elapsed with no new events in ``client.events``. A ``_FETCH_CAP_S``
    hard cap bounds the total fetch wall-clock in case a server trickles events indefinitely
    without going quiet (on cap, the fetch returns and the next nudge / 5-min safety-net re-drains
    from the advanced cursor). Both thresholds are confirmed correct via the live e2e."""
    tock = _QUIET_FLOOR_S
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

        # Drain: read events until quiet-floor or the hard cap.
        fetch_start = _monotonic()
        last_event_t = _monotonic()   # quiet measured from drain entry when no events yet
        while True:
            now = _monotonic()

            # Hard cap: a trickling-forever server must not spin indefinitely.
            if now - fetch_start >= _FETCH_CAP_S:
                logger.info(
                    f"fetch_once hard cap ({_FETCH_CAP_S}s) reached for {eid}; "
                    f"stopping drain — next nudge/safety-net will resume from cursor"
                )
                return   # finally block still runs (scheduler.remove)

            # Deliver all events queued so far in this service pass.
            # Also check the hard cap inside this loop: if the server trickles one event per
            # pass indefinitely the outer cap check is never reached from inside here.
            delivered_any = False
            while client.events:
                if _monotonic() - fetch_start >= _FETCH_CAP_S:
                    logger.info(
                        f"fetch_once hard cap ({_FETCH_CAP_S}s) reached for {eid}; "
                        f"stopping drain — next nudge/safety-net will resume from cursor"
                    )
                    return
                evt = client.events.popleft()
                deliver_event(evt, on_message=on_message, cursor_store=cursor_store, eid=eid)
                last_event_t = _monotonic()   # reset quiet clock on every delivered event
                delivered_any = True
                yield tock

            # Quiet-floor check: if no event arrived for _QUIET_FLOOR_S, the drain is done.
            if not delivered_any and (_monotonic() - last_event_t) >= _QUIET_FLOOR_S:
                break

            yield tock
    finally:
        scheduler.remove([clientDoer])    # one-shot done: stop servicing this clientDoer
