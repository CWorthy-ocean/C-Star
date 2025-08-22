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
def log() -> logging.Logger:
    return get_logger("cstar.tests.integration_tests")
