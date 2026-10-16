import socket
import subprocess

import pytest
import requests


def test_remote_host_is_refused() -> None:
    """The unit-test network guard fails a live request with a clear message."""
    with pytest.raises(RuntimeError, match="must not use the network"):
        requests.get("https://raw.githubusercontent.com/", timeout=1)


def test_loopback_is_allowed() -> None:
    """Loopback stays usable, e.g. for tests that run a local server."""
    assert socket.getaddrinfo("localhost", 80)


def test_remote_git_is_refused() -> None:
    """Git subprocesses may only use local repositories."""
    result = subprocess.run(
        ["git", "ls-remote", "https://github.com/CWorthy-ocean/C-Star.git"],
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
