"""`content` as an alias for `body` on the send tool (v0.5.1).

LLM callers frequently pass the message text under `content`, which used to
fail with a confusing "body Field required" validation error. The send tool
now coalesces the two.
"""

from __future__ import annotations

import pytest

from dlb_mcp import server, store


def test_content_alias_populates_body() -> None:
    msg = server.send(to="alpha", content="hello via content", from_="bob")
    assert msg["body"] == "hello via content"


def test_body_takes_precedence_over_content() -> None:
    msg = server.send(to="alpha", body="B wins", content="C loses", from_="bob")
    assert msg["body"] == "B wins"


def test_body_still_works_alone() -> None:
    msg = server.send(to="alpha", body="plain body", from_="bob")
    assert msg["body"] == "plain body"


def test_missing_both_raises() -> None:
    with pytest.raises(store.DLBError):
        server.send(to="alpha", from_="bob")
