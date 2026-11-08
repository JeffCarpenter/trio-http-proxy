"""Snippet tests for the AST policy; prohibited code is never executed."""

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.check_no_monkeypatch import check_source


@pytest.mark.parametrize(
    "source",
    [
        "from unittest.mock import patch as p\nwith p('trio.open_tcp_stream'): pass",
        "import unittest.mock as m\nm.patch.object(stream, 'send_all')",
        "from unittest import mock as m\nf = m.patch\nf('trio.sleep')",
        "def test_bad(monkeypatch):\n    monkeypatch.setattr(stream, 'send_all', fake)",
        "def test_bad(mocker):\n    mocker.patch.object(stream, 'send_all')",
        "from pytest import MonkeyPatch as MP\nMP().setattr(stream, 'send_all', fake)",
        "import trio as t\nt.open_tcp_stream = fake",
        "from trio import SocketStream as S\nS.send_all = fake",
        "import trio\nstream = trio.SocketStream(sock)\nstream.closed = True",
        "import trio\nalias = trio\nsetattr(alias, 'open_tcp_stream', fake)",
        "import trio\nalias: object = trio\nalias.sleep = fake",
        "def replacement(): pass\nsetattr(fake, 'work', replacement)",
        "def replacement(): pass\nalias = replacement\nfake.work = alias",
        "def replacement(): pass\nalias = replacement\nsetattr(fake, 'work', alias)",
        "alias = lambda: None\nfake.work = alias",
        "def replacement(): pass\na = replacement\nb = a\nsetattr(fake, 'work', b)",
        "import trio\ndef test_bad(stream: trio.SocketStream):\n    stream.closed = True",
        "from typing import Any\ndef test_bad(stream: Any):\n    stream.send_all = callback",
        "import typing\ntyping.Any.closed = True",
        "import trio\nfrom typing import Optional\ndef test_bad(stream: Optional[trio.SocketStream]):\n    stream.closed = True",
        "import trio\nfrom typing import Annotated\ndef test_bad(stream: Annotated[trio.SocketStream, 'meta']):\n    stream.closed = True",
        "import trio\nfrom typing import Union\ndef test_bad(stream: Union[None, trio.SocketStream]):\n    stream.closed = True",
        "async def replacement(): pass\nalias = replacement\nfake.work = alias",
        "def test_bad():\n    fake.work = replacement\ndef replacement(): pass",
        "setattr(fake, 'work', lambda: None)",
        "def test_bad(mocker):\n    mocker.spy(stream, 'send_all')",
        "def fake(): pass\nstream.send_all = fake",
        "stream.send_all = lambda data: None",
        "setattr(stream, 'send_all', fake)",
        "del stream.send_all",
        "stream.send_all: object = fake",
        "import trio\ntrio.__dict__['open_tcp_stream'] = fake",
    ],
)
def test_rejects_monkeypatches(source: str) -> None:
    assert check_source(source), source


@pytest.mark.parametrize(
    "source",
    [
        "# monkeypatch.setattr(stream, 'send_all', fake)\ntext = 'mock.patch(...)'",
        "class Fake:\n    async def send_all(self, data):\n        self.sent = True",
        "fake = Fake()\nfake.closed = True\nfake.events.append('closed')",
        "setattr(fake, 'closed', True)",
        "from functools import partial\nrunner = partial(run, port=0)",
        "async def connect(host, port):\n    return fake\nhttp_proxy(stream, _connect=connect)",
        "from unittest.mock import Mock\nvalue = Mock()",  # fake creation alone is not patching
        "self.before_send = before_send",  # constructor-injected fake behavior
        "def before_send(): pass\nclass Fake:\n    def __init__(self, before_send):\n        self.before_send = before_send",
        "def callback(): pass\ndef test_fake(callback):\n    setattr(fake, 'callback', callback)",
        "def callback(): pass\ndef test_fake(*callback):\n    fake.events = callback",
        "def callback(): pass\ndef test_fake(**callback):\n    fake.events = callback",
        "from typing import Any\ndef test_fake(fake: Any):\n    fake.closed = True",
        "import typing as t\ndef test_fake(fake: t.Any):\n    setattr(fake, 'closed', True)",
        "from typing import Optional\ndef test_fake(fake: Optional[Fake]):\n    fake.closed = True",
        "from typing import Callable\ndef test_fake(callback: Callable[[], None]):\n    callback.called = True",
        "import trio\nfrom typing import Annotated\ndef test_fake(fake: Annotated[Fake, trio.SocketStream]):\n    fake.closed = True",
        "def helper():\n    def state(): pass\ndef test_fake(state):\n    fake.state = state",
        "def callback(): pass\ncallback = 1\nfake.state = callback",
        "from typing_extensions import Any\ndef test_fake(fake: Any):\n    fake.closed = True",
        "def helper():\n    def state(): pass\nfake.state = state",
        "class Fake:\n    def state(self): pass\nfake.state = state",
        "def callback(): pass\nalias = callback\nalias = None\nfake.state = alias",
        "def test_one():\n    alias = lambda: None\nfake.state = alias",
    ],
)
def test_allows_fake_state_and_explicit_injection(source: str) -> None:
    assert check_source(source) == ()


def test_reports_line_and_reason() -> None:
    violations = check_source("import trio\ntrio.open_tcp_stream = fake\n")
    assert len(violations) == 1
    assert violations[0].line == 2
    assert "replacement" in violations[0].reason


def test_cli_reports_path_and_fails_closed(tmp_path: Path) -> None:
    target = tmp_path / "test_bad.py"
    target.write_text("import trio\ntrio.sleep = fake\n")
    command = [sys.executable, "scripts/check_no_monkeypatch.py", str(tmp_path)]
    bad = subprocess.run(
        command, capture_output=True, text=True, timeout=5, check=False
    )
    assert bad.returncode == 1
    assert f"{target}:2:" in bad.stdout
    target.write_text("def broken(")
    invalid = subprocess.run(
        command, capture_output=True, text=True, timeout=5, check=False
    )
    assert invalid.returncode == 1
    assert "syntax" in invalid.stdout.lower()
    target.write_text("fake.closed = True\n")
    clean = subprocess.run(
        command, capture_output=True, text=True, timeout=5, check=False
    )
    assert clean.returncode == 0
