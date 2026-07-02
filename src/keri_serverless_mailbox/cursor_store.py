"""Durable CursorStore over a keripy Baser's `tops` subdb (TopicsRecord keyed by (pre, eid)).

Persists the last-seen mailbox index per (pre, eid, topic). Depends only on stock
keripy (keri.db.basing) so the concierge-api CLI host and the Locksmith wallet share
one implementation. Implements the CursorStore protocol: get/set by (eid, topic).
"""
from __future__ import annotations

from keri.db import basing


class DbTopsCursorStore:
    def __init__(self, db, pre):
        self.db = db
        self.pre = pre

    def get(self, eid, topic):
        rec = self.db.tops.get((self.pre, eid))
        if rec is None or topic not in rec.topics:
            return None
        return rec.topics[topic]

    def set(self, eid, topic, idx):
        rec = self.db.tops.get((self.pre, eid)) or basing.TopicsRecord(topics=dict())
        rec.topics[topic] = int(idx)
        self.db.tops.pin((self.pre, eid), rec)
