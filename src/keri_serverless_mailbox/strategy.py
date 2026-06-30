"""Retrieval strategy interface + the Phase-3 Serverless stub.

A Strategy is transport + cursor only: it fetches CESR for the resolved mailbox EID and
calls on_message(topic, raw_cesr_bytes) per message, advancing the CursorStore. It does
NOT parse — the host parses (its own Parser, or psr.parse)."""
from __future__ import annotations
from abc import ABC, abstractmethod

from keri.kering import Schemes


class Strategy(ABC):
    @abstractmethod
    def run(self, *, hab, eid, topics, on_message, cursor_store, retry_ms=1000):
        """An hio doer generator: yields control, fetches, calls on_message + cursor_store."""
        raise NotImplementedError


class ServerlessStrategy(Strategy):
    """Phase 3: WebSocket notify-and-fetch. Selected when the mailbox advertises a wss loc."""
    def run(self, *, hab, eid, topics, on_message, cursor_store, retry_ms=1000):
        raise NotImplementedError("Phase 3: WS notify-and-fetch")
        yield  # pragma: no cover  (keeps this a generator function)


class StandardStrategy(Strategy):
    """SSE/poll retrieval for non-serverless mailboxes. run() implemented in standard.py
    (Task 3) via _run_standard; this shell exists so discovery can select it now."""
    def run(self, *, hab, eid, topics, on_message, cursor_store, retry_ms=1000):
        from .standard import run_standard
        yield from run_standard(hab=hab, eid=eid, topics=topics,
                                on_message=on_message, cursor_store=cursor_store,
                                retry_ms=retry_ms)


def discover_strategy(hab, eid) -> Strategy:
    """KERI-native capability discovery: a wss loc scheme on the mailbox EID => Serverless."""
    return ServerlessStrategy() if hab.fetchUrl(eid, scheme=Schemes.wss) else StandardStrategy()
