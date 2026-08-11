"""C-Star Forge subcommands for the ``cstar`` CLI.

Registered under C-Star's ``cstar.cli`` entry-point group (see
``[project.entry-points."cstar.cli"]`` in pyproject.toml), so when both
packages are installed the commands appear as::

    cstar forge run <forge_blueprint.yaml> [executor options...]
    cstar forge wizard [--port 8866] [voila options...]

``forge run`` is a deliberate passthrough to the executor's own argparse CLI
(`cstar_forge.run.main`) — the full option set (stage selection, dask tuning,
diagnostics) lives there and is per-invocation by design, not blueprint
content. The no-frills alternative, ``cstar blueprint run``, executes a forge
blueprint through the C-Star application framework with defaults.
"""

import os
import shutil
from importlib.resources import files

import typer

app = typer.Typer(
    help="C-Star Forge: generate domains and launch the blueprint wizard."
)

# Passthrough commands: typer must not parse or intercept anything (including
# --help, which argparse/voila should answer with the real option list).
_PASSTHROUGH = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}


@app.command(context_settings=_PASSTHROUGH)
def run(ctx: typer.Context) -> None:
    """Process a forge blueprint with the full executor option set."""
    from cstar_forge.run import main

    raise typer.Exit(main(list(ctx.args)))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def wizard(
    ctx: typer.Context,
    port: int = typer.Option(8866, help="port for the voila web app"),
) -> None:
    """Launch the forge blueprint wizard (voila web app).

    Extra arguments are passed through to voila.
    """
    notebook = files("cstar_forge") / "forge-blueprint-wizard-app.ipynb"
    argv = [
        "voila",
        str(notebook),
        f"--port={port}",
        '--Voila.tornado_settings={"allow_origin": "*"}',
        *ctx.args,
    ]
    _exec_voila(argv)


def _exec_voila(argv: list[str]) -> None:
    """Replace this process with voila (signals/Ctrl-C flow to the server)."""
    if shutil.which("voila") is None:
        typer.echo(
            "voila is not installed in this environment. It ships with the "
            "conda-forge `cstar-forge` package and with `pip install "
            "'cstar-forge[app]'`.",
            err=True,
        )
        raise typer.Exit(1)
    os.execvp(argv[0], argv)


def main() -> None:  # pragma: no cover - thin standalone hook, exercised manually
    """Allow ``python -m cstar_forge.cli`` as a cstar-independent fallback."""
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
