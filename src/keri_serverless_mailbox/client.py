"""MailboxClient: resolve the mailbox (agenting.mailbox), discover its capability, run the
selected strategy. MailboxClientDoer mounts it under a host hio Doist."""
from __future__ import annotations

from hio.base import doing
from keri.app import agenting
from keri import help

from .strategy import discover_strategy

logger = help.ogler.getLogger(__name__)


class MailboxClient:
    def __init__(self, hab, *, topics, on_message, cursor_store, retry_ms=1000):
        self.hab = hab
        self.topics = list(topics)
        self.on_message = on_message
        self.cursor_store = cursor_store
        self.retry_ms = retry_ms

    def resolve(self):
        """The mailbox EID for this AID: its mailbox end-role, else a witness, else None."""
        return agenting.mailbox(self.hab, self.hab.pre)

    def strategy_for(self, eid):
        return discover_strategy(self.hab, eid)


class MailboxClientDoer(doing.DoDoer):
    """Mounts a MailboxClient: resolves the EID, selects the strategy, runs it. If no mailbox
    resolves, it logs and idles (does not crash)."""
    def __init__(self, client: MailboxClient, **kwa):
        self.client = client
        super().__init__(doers=[doing.doify(self.runDo)], **kwa)

    def runDo(self, tymth=None, tock=0.0, **kwa):
        self.wind(tymth)
        self.tock = tock
        _ = (yield self.tock)
        c = self.client
        eid = c.resolve()
        if eid is None:
            logger.info(f"no mailbox resolved for {c.hab.pre}; not polling")
            return
        strategy = c.strategy_for(eid)
        # scheduler=self: MailboxClientDoer IS a DoDoer (has .extend/.remove) and is already
        # entered/running on the host Doist when runDo executes, so extending it schedules the
        # strategy's transport doer(s) on that same Doist -- exactly how the old Poller worked.
        yield from strategy.run(hab=c.hab, eid=eid, topics=c.topics,
                                on_message=c.on_message, cursor_store=c.cursor_store,
                                retry_ms=c.retry_ms, scheduler=self)
