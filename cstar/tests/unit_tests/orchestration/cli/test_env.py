import pytest
from typer.testing import CliRunner

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
