"""CursorStore port: the host supplies per-(eid, topic) cursor persistence."""
from __future__ import annotations
from typing import Protocol


class CursorStore(Protocol):
    def get(self, eid: str, topic: str) -> int | None:
        """Last-seen index for (eid, topic), or None if never seen."""
        ...

    def set(self, eid: str, topic: str, idx: int) -> None:
        """Persist the last-seen index for (eid, topic)."""
        ...
