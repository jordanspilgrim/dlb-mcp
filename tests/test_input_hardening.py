"""Stage 3 red-team regressions — sidecar collision, int overflow, unicode."""

from __future__ import annotations

import pytest

from dlb_mcp import server, store
from dlb_mcp.store import DLBError

# ── #5 sidecar filename collision ────────────────────────────────────────────


def test_distinct_names_get_distinct_sidecars() -> None:
    # 'a/b', 'a_b', 'a b' collided onto one file under the old sanitizer.
    for a, b in [("a/b", "a_b"), ("a_b", "a b"), ("x", "x ")]:
        assert store.sidecar_path(a) != store.sidecar_path(b), (a, b)


def test_registering_colliding_name_does_not_clobber_reclaim_secret() -> None:
    ra = store.register("a_b")
    sec_ab = store.sidecar_path("a_b").read_text().splitlines()[1]
    store.register("a/b")  # would overwrite a_b's sidecar under the old scheme
    # a_b's sidecar (and thus its reclaim secret) is untouched.
    assert store.sidecar_path("a_b").read_text().splitlines()[1] == sec_ab
    # And a_b can still reclaim with its own secret.
    reg = store.register("a_b", force=True, prior_token=sec_ab)
    assert reg["session_token"] != ra["session_token"]


def test_recover_hook_lists_human_names_from_sidecar_contents() -> None:
    from dlb_mcp import recover_hook

    store.register("wörker/1")
    store.register("boss")
    assert set(recover_hook.registered_names()) == {"wörker/1", "boss"}


# ── #6 integer overflow ──────────────────────────────────────────────────────


def test_oversized_message_id_raises_dlberror_not_overflow() -> None:
    big = 2**63
    tok = store.register("a")["session_token"]
    with pytest.raises(DLBError):
        store.get_task_status(big)
    with pytest.raises(DLBError):
        store.ack(big, tok)
    with pytest.raises(DLBError):
        store.update_status(big, "done", tok)
    with pytest.raises(DLBError):
        store.send(to="a", body="hi", in_reply_to=big)


def test_ttl_days_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLB_MESSAGE_TTL_DAYS", str(10**18))
    assert store.ttl_days() == store._MAX_TTL_DAYS
    # A send with the clamped TTL must not overflow.
    store.send(to="a", body="hi")


def test_list_threads_huge_active_within_is_clamped() -> None:
    server.register("a")
    # Must not raise OverflowError from timedelta(hours=10**18).
    server.list_threads(active_within_hours=10**18)


# ── Codex review follow-ups (0.6.2) ──────────────────────────────────────────


def test_recover_hook_prints_resolved_hashed_sidecar_path(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Codex #1: with sha256 sidecar filenames, the hook must print each name's
    # RESOLVED path (tokens/<hash>), not the broken generic tokens/<name>.
    from dlb_mcp import recover_hook

    reg = store.register("wörker/1")
    recover_hook.main()
    out = capsys.readouterr().out
    path = store.sidecar_path("wörker/1")
    assert str(path) in out, "hook must show the resolved sidecar path"
    assert f"{store.tokens_dir()}/wörker/1" not in out  # not the broken generic path
    # The printed path actually locates the reclaim secret, and it reclaims.
    secret = path.read_text().splitlines()[1]
    reclaimed = store.register("wörker/1", force=True, prior_token=secret)
    assert reclaimed["session_token"] != reg["session_token"]
