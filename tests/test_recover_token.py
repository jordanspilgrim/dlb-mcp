"""recover_token — compaction recovery for a lost session_token (rec #1)."""

from __future__ import annotations

from dlb_mcp import store


def test_recover_returns_live_token_and_enables_read():
    reg = store.register("alpha", working_on="doing things")
    token = reg["session_token"]

    rec = store.recover_token("alpha")
    assert rec is not None
    assert rec["session_token"] == token  # same live token, not a new one
    assert rec["name"] == "alpha"
    assert rec["working_on"] == "doing things"

    # The recovered token actually works for a privileged op.
    store.send(to="alpha", body="hi", from_="bravo")
    msgs = store.read(name="alpha", session_token=rec["session_token"])
    assert len(msgs) == 1


def test_recover_unknown_name_returns_none():
    assert store.recover_token("nobody") is None


def test_recover_does_not_mint_new_token_unlike_reregister():
    t1 = store.register("gamma")["session_token"]
    # recover twice → stable token
    assert store.recover_token("gamma")["session_token"] == t1
    assert store.recover_token("gamma")["session_token"] == t1
