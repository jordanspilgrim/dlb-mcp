"""Token identity — random, rotating, hashed at rest (replaces the old
deterministic-token scheme). Recovery no longer relies on re-derivation; it
rests on per-process ownership / session-id / the reclaim credential.
"""

from __future__ import annotations

import sqlite3

import pytest

from dlb_mcp import store
from dlb_mcp.store import AuthError


def test_db_stores_hash_not_plaintext_token() -> None:
    # Core of the at-rest hardening: the token itself is never written to the DB,
    # only sha256(token). A read of the SQLite file yields no usable token.
    tok = store.register("alpha")["session_token"]
    with sqlite3.connect(str(store.store_path())) as c:
        stored_hash = c.execute("SELECT token_hash FROM agents WHERE name='alpha'").fetchone()[0]
        dump = " ".join(str(v) for row in c.execute("SELECT * FROM agents").fetchall() for v in row)
    assert stored_hash == store.token_hash(tok)
    assert tok not in dump, "plaintext token must not appear anywhere in the agents table"


def test_distinct_names_get_distinct_tokens() -> None:
    a = store.register("alpha")["session_token"]
    b = store.register("bravo")["session_token"]
    assert a != b


def test_token_is_high_entropy_hex() -> None:
    tok = store.register("alpha")["session_token"]
    assert len(tok) == 64 and all(c in "0123456789abcdef" for c in tok)


def test_reregister_is_random_not_derived_from_name() -> None:
    # The core change from deterministic ids: release + re-register the SAME name
    # yields a DIFFERENT random token (nothing is derived from the name).
    t1 = store.register("alpha")["session_token"]
    store.unregister("alpha", t1)
    t2 = store.register("alpha")["session_token"]
    assert t1 != t2


def test_force_reclaim_rotates_and_invalidates_old_token() -> None:
    t1 = store.register("alpha")["session_token"]
    # A legitimate reclaim/handoff via prior_token rotates to a fresh token...
    t2 = store.register("alpha", force=True, prior_token=t1)["session_token"]
    assert t2 != t1
    # ...and the old token no longer authenticates (real eviction).
    with pytest.raises(AuthError):
        store.read("alpha", session_token=t1)
    assert store.read("alpha", session_token=t2) == []


def test_register_writes_token_sidecar() -> None:
    reg = store.register("alpha", working_on="x")
    sidecar = store.sidecar_path("alpha")
    assert sidecar.exists()
    contents = sidecar.read_text()
    assert "alpha" in contents
    assert reg["session_token"] in contents
    mode = sidecar.stat().st_mode & 0o077
    assert mode == 0
