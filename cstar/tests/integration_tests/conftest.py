import logging
from collections.abc import Callable, Generator
from contextlib import _GeneratorContextManager, contextmanager
from unittest import mock

import pytest

from cstar.base.log import get_logger
from cstar.tests.integration_tests.blueprints.fixtures import (
    modify_template_blueprint,  # noqa : F401
)
from cstar.tests.integration_tests.fixtures import (
    fetch_remote_test_case_data,  # noqa: F401
    fetch_roms_tools_source_data,  # noqa: F401
)


@pytest.fixture
def mock_user_input() -> Callable[[str], _GeneratorContextManager[Callable[..., str]]]:
    """Monkeypatch which will automatically respond to any call for input.

    Use it like this:

        ```
        def some_test(mock_user_input):
            with mock_user_input("yes"):
                assert input("Enter your choice: ") == "yes"
        ```
    """

    @contextmanager
    def _mock_input(
        input_string: str,
    ) -> Generator[_GeneratorContextManager[Callable[..., str]], None, None]:
        with mock.patch("builtins.input", return_value=input_string) as _:
            yield _

    return _mock_input


@pytest.fixture
def log() -> logging.Logger:
    """Retrieve a logger instance for logging during tests."""
    return get_logger("cstar.tests.integration_tests")
