"""Constants shared by the proxy (proxy.py) and its workers (server.py).

Kept in a module of their own so the proxy can use them without importing the
server, which loads FastMCP and registers every tool.
"""

from __future__ import annotations

# Version of the handoff protocol on the private control pipes. A worker that
# answers with a different version is not swapped in: the proxy keeps the old
# worker and tells the session to restart (the one case that still needs one).
CONTROL_PROTOCOL = 1

# Env var carrying the worker's control-pipe fd numbers, "<read>,<write>". It
# holds fd numbers only, never a secret.
CONTROL_FDS_ENV = "DLB_CONTROL_FDS"

# Prefix of the JSON-RPC ids the proxy uses for its own requests to a worker
# (handshake replay, probes). A client request using it is refused, so a
# client can never receive, or swallow, a proxy-internal response.
PRIVATE_ID_PREFIX = "dlb-proxy:"
