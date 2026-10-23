from collections.abc import Callable

import pytest
from typer.testing import CliRunner

from cstar.cli.environment.show import app


@pytest.mark.usefixtures("mock_env")
@pytest.mark.asyncio
async def test_cli_env_show() -> None:
    """Verify that CLI env show command produces output and omits the internal group."""
    runner = CliRunner()

    result = runner.invoke(
        app,
        [],
        color=False,
    )
    assert "CSTAR_LOG_LEVEL:" in result.stdout
    assert "CSTAR_RUNID:" not in result.stdout
    assert "CSTAR_:" not in result.stdout
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_internal_group() -> None:
    """Verify that requesting the internal group by name displays its variables."""
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--group", "internal"],
        color=False,
    )

    assert "CSTAR_RUNID:" in result.stdout
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_export_omits_internal(
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify that export statements leave out the internal group but still
    include a setting that is not persistable.

    Parameters
    ----------
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--display", "export"],
        color=False,
    )

    assert "CSTAR_RUNID" not in result.stdout
    assert "export CSTAR_CONFIG_HOME=" in flatten_cli_output(result.stdout)
    assert not result.stderr


@pytest.mark.usefixtures("mock_env")
def test_cli_env_show_internal_export_is_empty(
    flatten_cli_output: Callable[[str], str],
) -> None:
    """Verify that exporting the internal group says it is never exported.

    Parameters
    ----------
    flatten_cli_output : Callable[[str], str]
        Fixture that joins the output lines that rich wrapped.
    """
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["--group", "internal", "--display", "export"],
        color=False,
    )

    assert "CSTAR_RUNID" not in result.stdout
    assert "The internal group is never exported" in flatten_cli_output(result.stdout)
    assert not result.stderr
