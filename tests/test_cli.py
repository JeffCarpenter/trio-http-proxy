"""The nano CLI only accepts host and port; environment settings are ignored."""

from functools import partial

import pytest

from trio_http_proxy import proxy


@pytest.mark.parametrize(
    ("args", "host", "port"),
    [([], "127.0.0.1", 8080), (["--host", "::1", "--port", "9090"], "::1", 9090)],
)
def test_main_passes_listen_options(
    args: list[str], host: str, port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []
    monkeypatch.setattr("sys.argv", ["trio_http_proxy", *args])
    monkeypatch.setenv("TRIO_HTTP_PROXY_PORT", "not-an-integer")
    monkeypatch.setenv("PORT", "not-an-integer")
    monkeypatch.setattr(proxy.trio, "run", calls.append)

    proxy.main()

    assert len(calls) == 1
    target = calls[0]
    assert isinstance(target, partial)
    assert target.func is proxy.start_server
    assert target.args == (port,)
    assert target.keywords == {"host": host}


def test_main_handles_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupt(target):
        raise KeyboardInterrupt

    monkeypatch.setattr("sys.argv", ["trio_http_proxy"])
    monkeypatch.setattr(proxy.trio, "run", interrupt)
    proxy.main()


@pytest.mark.parametrize("bad_port", ["-1", "65536", "70000"])
def test_main_rejects_out_of_range_port(
    bad_port: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("sys.argv", ["trio_http_proxy", "--port", bad_port])
    with pytest.raises(SystemExit) as exc_info:
        proxy.main()
    assert exc_info.value.code == 2
