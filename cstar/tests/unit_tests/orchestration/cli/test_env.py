import os
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from cstar.cli.common import set_env
from cstar.cli.environment.show import app


@pytest.mark.usefixtures("mock_env")
@pytest.mark.asyncio
async def test_cli_env_show() -> None:
    """Verify that CLI env show command produces output."""
    runner = CliRunner()

    result = runner.invoke(
        app,
        [],
        color=False,
    )
    assert "CSTAR_RUNID:" in result.stdout
    assert "CSTAR_:" not in result.stdout
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
@pytest.mark.asyncio
async def test_cli_common_set_env() -> None:
    """Verify that the expected environment key is set after the operation completes."""
    key = "unit-test-key"
    value = " unit-test-value \t \n"
    exp_value = "unit-test-value"

    with mock.patch.dict(os.environ, {key: "old-value"}):
        cb = set_env(key)

        mock_context = mock.MagicMock(spec=typer.Context)
        actual = cb(mock_context, value)

        assert actual == exp_value
        assert os.getenv(key) == exp_value


@pytest.mark.usefixtures("mock_env")
@pytest.mark.asyncio
async def test_cli_common_set_env_extra() -> None:
    """Verify that the expected environment key is set after the operation completes."""
    key = "unit-test-key"
    value = " unit-test-value \t \n"
    exp_value = "unit-test-valueunit-test-value"

    def doubler(ctx: typer.Context, value: str) -> str:
        return value * 2

    with mock.patch.dict(os.environ, {key: "old-value"}):
        cb = set_env(key, doubler)

        mock_context = mock.MagicMock(spec=typer.Context)
        actual = cb(mock_context, value)

        assert actual == exp_value
        assert os.getenv(key) == exp_value
