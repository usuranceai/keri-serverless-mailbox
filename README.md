# keri-serverless-mailbox

A small, reusable, KERI-native mailbox client for any KERI Python application. It resolves an AID's mailbox endpoint via stock keripy `agenting.mailbox` (mailbox end-role first, witness fallback), discovers the mailbox's delivery capability from the KEL (a `wss` location scheme signals WebSocket notify-and-fetch), and runs a swappable retrieval strategy against that endpoint.

## Status

**Standard strategy** (held-SSE poll, compatible with stock keripy mailboxes) — **implemented and tested**.

**Serverless strategy** (WebSocket notify-and-fetch, for serverless mailboxes that push a ready-signal then serve a bounded fetch) — **implemented** (hio-native WebSocket client; no `websockets` dependency). End-to-end conformance against a deployed serverless mailbox stack is pending a fresh dev-stage deploy (see `tests/test_serverless_e2e.py`, skipped unless `MAILBOX_URL` is set).

Capability discovery (`discover_strategy`) requires keripy carrying the `wss` location scheme. On stock keripy, discovery finds no `wss` endpoint and degrades gracefully to the Standard strategy.

## Design

Transport + cursor only. `MailboxClient` delivers raw CESR bytes to an `on_message(topic, raw_bytes)` callback and advances a host-supplied `CursorStore`. The host is responsible for parsing — the client is host-agnostic.

## Usage

```python
from keri_serverless_mailbox import (
    MailboxClient,
    MailboxClientDoer,
    CursorStore,
)

# Implement CursorStore to persist per-topic cursors across restarts.
class MyCursorStore(CursorStore):
    def get(self, topic: str) -> int: ...
    def set(self, topic: str, cursor: int) -> None: ...

def on_message(topic: str, raw: bytes) -> None:
    # Parse and dispatch the raw CESR message.
    ...

client = MailboxClient(
    hab=hab,                      # keripy Hab
    topics=["/receipt", "/replay"],
    on_message=on_message,
    cursor_store=MyCursorStore(),
)

# Mount under an hio Doist alongside your other doers.
doer = MailboxClientDoer(client)
doist.do(doers=[doer, ...])
```

## Public API

| Name | Kind | Notes |
|---|---|---|
| `MailboxClient` | class | Main client; holds hab, topics, strategy |
| `MailboxClientDoer` | class | hio `Doer` wrapper; drives the client generator |
| `Strategy` | enum | `STANDARD`, `SERVERLESS` |
| `StandardStrategy` | class | SSE-poll strategy (implemented) |
| `ServerlessStrategy` | class | WS notify-and-fetch strategy (implemented; hio-native) |
| `CursorStore` | ABC | Implement to persist per-topic cursors |
| `discover_strategy` | function | Inspects KEL for `wss` loc; returns `Strategy` |

## Requirements

- Python >= 3.12
- [`keri`](https://github.com/WebOfTrust/keripy) — unpinned; the package degrades gracefully on stock keripy (serverless discovery requires keripy carrying the `wss` location scheme)
- [`hio`](https://github.com/ioflo/hio)

## License

Apache-2.0. Copyright 2026 Usurance.
