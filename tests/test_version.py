"""Tests for stale-version detection (monitor exit 75, server stale_server)."""

from __future__ import annotations

from pathlib import Path

import pytest

from dlb_mcp import _version, monitor, store


def test_installed_version_matches_loaded_in_dev_env() -> None:
    # The test env installs this checkout, so disk and memory agree.
    assert _version.installed_version() == _version.LOADED_VERSION
    assert _version.stale_version() is None
    assert _version.stale_server_notice() is None


def test_stale_version_reports_loaded_and_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_version, "installed_version", lambda: "9.9.9")
    assert _version.stale_version() == (_version.LOADED_VERSION, "9.9.9")
    notice = _version.stale_server_notice()
    assert notice is not None
    assert notice.startswith(f"{_version.LOADED_VERSION} -> 9.9.9:")
    assert "Restart the session" in notice


def test_unknown_installed_version_is_not_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    # A missing or mid-replacement dist-info must not trigger a restart.
    monkeypatch.setattr(_version, "installed_version", lambda: None)
    assert _version.stale_version() is None
    assert _version.stale_server_notice() is None


def test_installed_version_returns_none_on_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(_name: str) -> None:
        raise _version.metadata.PackageNotFoundError(_name)

    monkeypatch.setattr(_version.metadata, "distribution", boom)
    assert _version.installed_version() is None


def test_monitor_exits_75_on_upgrade_and_clears_heartbeat(
    isolated_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store.register("alpha")
    monkeypatch.setattr(_version, "installed_version", lambda: "9.9.9")
    monkeypatch.setattr(monitor.time, "sleep", lambda *_: None)

    exit_code = monitor.run(
        name="alpha",
        interval_seconds=0.01,
        include_senders=None,
        exclude_senders=None,
    )

    assert exit_code == _version.EXIT_VERSION_CHANGED == 75
    assert "upgraded" in capsys.readouterr().err
    # The finally block clears the heartbeat, so alpha is no longer wakeable.
    assert store.dispatch_assessment("alpha")["wakeable"] is False


def test_monitor_keeps_running_when_version_unchanged(
    isolated_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store.register("alpha")
    calls: list[int] = []

    def iteration(*_args, **_kwargs) -> int:
        calls.append(1)
        if len(calls) >= 3:
            raise KeyboardInterrupt
        return 0

    monkeypatch.setattr(monitor, "_poll_iteration", iteration)
    monkeypatch.setattr(monitor.time, "sleep", lambda *_: None)

    exit_code = monitor.run(
        name="alpha",
        interval_seconds=0.01,
        include_senders=None,
        exclude_senders=None,
    )
    assert exit_code == 0
    assert len(calls) == 3


# --- list_threads stale-server sentinel ------------------------------------


def test_list_threads_appends_sentinel_when_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    from dlb_mcp import server

    server.register("a")
    monkeypatch.setattr(_version, "installed_version", lambda: "9.9.9")
    out = server.list_threads()
    assert out[-1]["name"] == server.STALE_SERVER_SENTINEL
    assert "Restart the session" in out[-1]["stale_server"]
    assert any(e.get("name") == "a" for e in out[:-1])


def test_list_threads_no_sentinel_when_current() -> None:
    from dlb_mcp import server

    server.register("a")
    out = server.list_threads()
    assert all("stale_server" not in e for e in out)


def test_list_threads_sentinel_survives_only_unread(monkeypatch: pytest.MonkeyPatch) -> None:
    from dlb_mcp import server

    server.register("a")
    monkeypatch.setattr(_version, "installed_version", lambda: "9.9.9")
    out = server.list_threads(only_unread=True)
    assert [e["name"] for e in out] == [server.STALE_SERVER_SENTINEL]
