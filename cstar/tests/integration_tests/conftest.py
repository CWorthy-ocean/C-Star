import builtins
import logging
from contextlib import contextmanager

import pytest

from cstar.base.log import get_logger
from cstar.tests.integration_tests.blueprints.fixtures import (
    modify_template_blueprint,  # noqa : F401  # noqa : F401
)
from cstar.tests.integration_tests.fixtures import (
    fetch_remote_test_case_data,  # noqa: F401  # noqa: F401
    fetch_roms_tools_source_data,  # noqa: F401  # noqa: F401
)


@pytest.fixture
def mock_user_input():
    """Monkeypatch which will automatically respond to any call for input.

    Use it like this:

        ```
        def some_test(mock_user_input):
            with mock_user_input("yes"):
                assert input("Enter your choice: ") == "yes"
        ```
    """

    @contextmanager
    def _mock_input(input_string):
        original_input = builtins.input

        def mock_input_function(_):
            return input_string

        builtins.input = mock_input_function
        try:
            yield
        finally:
            builtins.input = original_input

    return _mock_input


@pytest.fixture
def log() -> logging.Logger:
    return get_logger("cstar.tests.integration_tests")
