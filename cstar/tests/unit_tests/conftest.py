import logging

import pytest

from cstar.base.log import get_logger


@pytest.fixture
def log() -> logging.Logger:
    return get_logger("cstar.tests.unit_tests")
