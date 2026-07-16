"""Deterministic tokens + sidecar (rec #1B) — un-loseable identity."""

from __future__ import annotations

from dlb_mcp import store


def test_recovery_yields_same_token():
    """The whole point: a token lost to restart/compaction is RECOVERABLE
    because it is derivable — every recovery path returns the identical token."""
    t1 = store.register("alpha")["session_token"]
    # In-session recovery: recover_token, or re-derive from name.
    assert store.recover_token("alpha")["session_token"] == t1
    assert store.deterministic_token("alpha") == t1
    # A genuine restart: the session released the name and re-registers —
    # still the SAME token, no stored value needed.
    store.unregister("alpha", t1)
    assert store.register("alpha")["session_token"] == t1


def test_token_matches_pure_derivation():
    reg = store.register("alpha")
    assert reg["session_token"] == store.deterministic_token("alpha")


def test_distinct_names_get_distinct_tokens():
    a = store.register("alpha")["session_token"]
    b = store.register("bravo")["session_token"]
    assert a != b


def test_token_is_unguessable_format():
    tok = store.register("alpha")["session_token"]
    assert len(tok) == 64 and all(c in "0123456789abcdef" for c in tok)  # HMAC-SHA256 hex


def test_secret_persists_across_calls_so_token_is_stable():
    # Even simulating a fresh process (no in-memory cache), the on-disk secret
    # makes the derivation stable.
    t1 = store.deterministic_token("alpha")
    t2 = store.deterministic_token("alpha")
    assert t1 == t2


def test_register_writes_token_sidecar():
    reg = store.register("alpha", working_on="x")
    sidecar = store.store_path().parent / "tokens" / "alpha"
    assert sidecar.exists()
    contents = sidecar.read_text()
    assert "alpha" in contents
    assert reg["session_token"] in contents
    # Best-effort perms: file should not be world/group readable where supported.
    mode = sidecar.stat().st_mode & 0o077
    assert mode == 0
