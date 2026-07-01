"""Retrieval strategy interface + the Phase-3 Serverless stub.

A Strategy is transport + cursor only: it fetches CESR for the resolved mailbox EID and
calls on_message(topic, raw_cesr_bytes) per message, advancing the CursorStore. It does
NOT parse — the host parses (its own Parser, or psr.parse)."""
from __future__ import annotations
from abc import ABC, abstractmethod

from keri.kering import Schemes

# NOTE: wss scheme requires keripy with the wss loc scheme; absent on stock keripy -> Standard
_WSS = getattr(Schemes, "wss", None)   # module-level; absent on stock keripy


class Strategy(ABC):
    @abstractmethod
    def run(self, *, hab, eid, topics, on_message, cursor_store, retry_ms=1000, scheduler):
        """An hio doer generator: yields control, fetches, calls on_message + cursor_store.

        scheduler is a hio DoDoer (exposing .extend([doer]) / .remove([doer])) owned by the
        host; the strategy schedules its transport doer(s) on it so they get serviced."""
        raise NotImplementedError


class ServerlessStrategy(Strategy):
    """Phase 3: WebSocket notify-and-fetch. Selected when the mailbox advertises a wss loc.
    run() delegates to serverless.run_serverless (mirrors how StandardStrategy delegates)."""
    def run(self, *, hab, eid, topics, on_message, cursor_store, retry_ms=1000, scheduler):
        from .serverless import run_serverless
        yield from run_serverless(hab=hab, eid=eid, topics=topics,
                                  on_message=on_message, cursor_store=cursor_store,
                                  retry_ms=retry_ms, scheduler=scheduler)


class StandardStrategy(Strategy):
    """SSE/poll retrieval for non-serverless mailboxes. run() implemented in standard.py
    (Task 3) via _run_standard; this shell exists so discovery can select it now."""
    def run(self, *, hab, eid, topics, on_message, cursor_store, retry_ms=1000, scheduler):
        from .standard import run_standard
        yield from run_standard(hab=hab, eid=eid, topics=topics,
                                on_message=on_message, cursor_store=cursor_store,
                                retry_ms=retry_ms, scheduler=scheduler)


def discover_strategy(hab, eid) -> Strategy:
    """KERI-native capability discovery: a wss loc scheme on the mailbox EID => Serverless.

    Degrades gracefully on stock keripy, which has no wss scheme (_WSS is None)."""
    if _WSS is not None and hab.fetchUrl(eid, scheme=_WSS):
        return ServerlessStrategy()
    return StandardStrategy()
