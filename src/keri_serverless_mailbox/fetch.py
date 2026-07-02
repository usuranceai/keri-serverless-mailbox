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

# After the connection is up, how long to wait for the FIRST event before treating the mailbox
# as empty. Covers the TLS connect + server first-response latency that a 0.25s quiet floor
# measured from drain-entry would cut short (the request leaves client.requests before connect).
_FIRST_EVENT_TIMEOUT_S = 8.0


def build_qtopics(eid, topics, cursor_store):
    """Per-topic query cursors = the LAST-SEEN ordinal (or -1 if never seen).

    The mailbox drain iterates from cursor+1 — keripy's MailboxIterable AND our
    _format_sse_events both do ``cloneTopicIter(fn=idx+1)`` — so the qry value must be the
    last-seen ordinal and the SERVER does the single increment. Sending ``seen+1`` (or ``0``
    when unseen) double-increments and skips the next (or, unseen, the very FIRST) message.
    Confirmed live against mailbox.keri.host: a qry cursor of -1 drains ordinal 0; a cursor
    of 0 drains nothing."""
    q_topics = {}
    for topic in topics:
        seen = cursor_store.get(eid, topic)
        q_topics[topic] = seen if seen is not None else -1
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

        # Drain: read events until the backlog goes quiet, or the hard cap.
        #
        # The signed qry leaves client.requests before the TLS connection completes, so a quiet
        # floor measured from drain-entry would give up mid-handshake — before the server's
        # response (connect + first event can take a few seconds). Gate the give-up logic on the
        # connection being established:
        #   * while connecting: keep waiting (bounded only by the hard cap);
        #   * connected, no event yet: wait _FIRST_EVENT_TIMEOUT_S for the first event (empty
        #     mailbox => return);
        #   * after events start: _QUIET_FLOOR_S of silence marks end-of-backlog.
        # A client with no .connector (unit fakes) is treated as already connected, preserving
        # the original immediate quiet-floor behavior.
        fetch_start = _monotonic()
        last_event_t = _monotonic()
        connected_at = None
        delivered_ever = False
        has_connector = getattr(client, "connector", None) is not None
        while True:
            now = _monotonic()
            if now - fetch_start >= _FETCH_CAP_S:
                logger.info(
                    f"fetch_once hard cap ({_FETCH_CAP_S}s) reached for {eid}; "
                    f"stopping drain — next nudge/safety-net will resume from cursor"
                )
                return   # finally block still runs (scheduler.remove)

            connected = getattr(getattr(client, "connector", None), "connected", True)
            if connected and connected_at is None:
                connected_at = now
                last_event_t = now      # start the quiet window at connect, not drain-entry

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
                last_event_t = _monotonic()
                delivered_ever = True
                delivered_any = True
                yield tock

            if not delivered_any and connected_at is not None:
                if delivered_ever:
                    # end-of-backlog: quiet floor after the last delivered event
                    if (_monotonic() - last_event_t) >= _QUIET_FLOOR_S:
                        break
                elif not has_connector:
                    # unit fake (no connector): preserve the original immediate quiet floor
                    if (_monotonic() - last_event_t) >= _QUIET_FLOOR_S:
                        break
                elif (_monotonic() - connected_at) >= _FIRST_EVENT_TIMEOUT_S:
                    # real client, connected, no event within the first-response window: empty
                    break

            yield tock
    finally:
        scheduler.remove([clientDoer])    # one-shot done: stop servicing this clientDoer
