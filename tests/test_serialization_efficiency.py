"""Token-efficiency serialization contracts (v0.3.1+).

Locks the two payload-shrinking behaviors added for large-fleet token cost:
  1. `_message_dict` omits optional lifecycle fields when null.
  2. `_agent_summary_dict` truncates `working_on`.
Both are pure functions over store dataclasses, so we test them directly
rather than over the JSON-RPC transport.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dlb_mcp import server, store

_NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)


def _msg(**overrides):
    base = dict(
        id=1,
        recipient_name="alpha",
        sender_name="bravo",
        subject="hi",
        body="body",
        sent_at=_NOW,
        read_at=None,
        expires_at=_NOW,
        msg_type=None,
        in_reply_to=None,
        status=None,
        status_note=None,
        status_updated_at=None,
    )
    base.update(overrides)
    return store.Message(**base)


def test_message_dict_omits_null_lifecycle_fields():
    d = server._message_dict(_msg())
    # Core fields always present.
    for k in ("id", "recipient_name", "sender_name", "subject", "body", "sent_at", "expires_at"):
        assert k in d
    # Null optionals omitted entirely (not present as explicit null).
    for k in ("read_at", "msg_type", "in_reply_to", "status", "status_note", "status_updated_at"):
        assert k not in d


def test_message_dict_includes_set_lifecycle_fields():
    d = server._message_dict(
        _msg(msg_type="task", in_reply_to=7, status="running", status_note="eta 5m", status_updated_at=_NOW, read_at=_NOW)
    )
    assert d["msg_type"] == "task"
    assert d["in_reply_to"] == 7
    assert d["status"] == "running"
    assert d["status_note"] == "eta 5m"
    assert "status_updated_at" in d
    assert "read_at" in d


def test_agent_summary_truncates_working_on():
    long_text = "x" * 500
    s = store.AgentSummary(name="a", working_on=long_text, last_seen=_NOW, unread_count=0, stale=False)
    d = server._agent_summary_dict(s, working_on_chars=140)
    assert len(d["working_on"]) <= 140
    assert d["working_on"].endswith("…")


def test_agent_summary_no_truncation_when_short_or_disabled():
    s = store.AgentSummary(name="a", working_on="short", last_seen=_NOW, unread_count=0, stale=False)
    assert server._agent_summary_dict(s, working_on_chars=140)["working_on"] == "short"
    # None disables truncation.
    long_text = "y" * 300
    s2 = store.AgentSummary(name="a", working_on=long_text, last_seen=_NOW, unread_count=0, stale=False)
    assert server._agent_summary_dict(s2, working_on_chars=None)["working_on"] == long_text
