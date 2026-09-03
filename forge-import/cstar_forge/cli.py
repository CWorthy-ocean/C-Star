"""C-Star Forge subcommands for the ``cstar`` CLI.

Registered under C-Star's ``cstar.cli`` entry-point group (see
``[project.entry-points."cstar.cli"]`` in pyproject.toml), so when both
packages are installed the commands appear as::

    cstar forge run <forge_blueprint.yaml> [executor options...]
    cstar forge wizard [--port 8866] [voila options...]
    cstar forge copy-notebook [--dest ...] [--force]
    cstar forge register-kernel [--clean] [--name ...]
    cstar forge show-paths [--json]

``forge run`` is a deliberate passthrough to the executor's own argparse CLI
(`cstar_forge.run.main`) — the full option set (stage selection, dask tuning,
diagnostics) lives there and is per-invocation by design, not blueprint
content. The no-frills alternative, ``cstar blueprint run``, executes a forge
blueprint through the C-Star application framework with defaults.
"""

import os
import shutil
from importlib.resources import as_file, files
from pathlib import Path

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

    raise typer.Exit(main(list(ctx.args), prog="cstar forge run"))


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
    notebook = files("cstar_forge.ui") / "_voila_app.ipynb"
    # Steer MPI's libfabric away from the default "sockets" provider before
    # exec'ing voila (the kernel inherits our environment): the first xESMF
    # regrid in a wizard kernel initializes ESMF/MPI, and the sockets
    # provider's progress threads busy-poll at ~100% CPU each (macOS) for the
    # life of the kernel; the tcp provider services the same single-process
    # MPI without spinning. setdefault so an explicit user choice wins; batch
    # ROMS runs launched outside this command keep their own default.
    os.environ.setdefault("FI_PROVIDER", "tcp")
    argv = [
        "voila",
        str(notebook),
        f"--port={port}",
        '--Voila.tornado_settings={"allow_origin": "*"}',
        *ctx.args,
    ]
    _exec_voila(argv)


@app.command()
def copy_notebook(
    dest: Path = typer.Option(
        Path("~/cstar/forge-blueprint-wizard.ipynb"),
        help="where to place the copy (~ is expanded)",
    ),
    force: bool = typer.Option(
        False, "--force", help="overwrite an existing file at --dest"
    ),
) -> None:
    """Copy the bundled wizard notebook (Jupyter alternative to the web app).

    For installs without a source checkout (e.g. conda/pip): places a runnable
    copy of ``forge-blueprint-wizard.ipynb`` outside the installed package so
    it can be opened in Jupyter. A copy rather than a symlink on purpose --
    Jupyter autosaves executed output back into the file, which must never
    land in site-packages. Re-run with --force after upgrading cstar-forge to
    refresh the copy.
    """
    with as_file(files("cstar_forge") / "forge-blueprint-wizard.ipynb") as src:
        payload = src.read_bytes()
    target = dest.expanduser()
    if target.is_dir():
        typer.echo(f"Error: {target} is a directory.", err=True)
        raise typer.Exit(1)
    if (target.is_symlink() or target.exists()) and not force:
        if not target.is_symlink() and target.read_bytes() == payload:
            typer.echo(f"Already up to date: {target}")
            return
        kind = "is a symlink" if target.is_symlink() else "already exists"
        typer.echo(
            f"Error: {target} {kind}; re-run with --force to replace it "
            "with a fresh copy of the packaged notebook.",
            err=True,
        )
        raise typer.Exit(1)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_symlink():
        target.unlink()  # write_bytes would otherwise write through the link
    target.write_bytes(payload)
    typer.echo(f"Wizard notebook copied to: {target}")
    typer.echo(f"Open it in Jupyter, e.g.: jupyter lab {target}")


@app.command()
def register_kernel(
    name: str | None = typer.Option(
        None, help="kernel name (default: the active env's name)"
    ),
    display_name: str | None = typer.Option(
        None, help="display name shown in Jupyter (default: the kernel name)"
    ),
    clean: bool = typer.Option(
        False, "--clean", help="remove an existing kernelspec of this name first"
    ),
    package_manager: str = typer.Option(
        "auto", help="tool the wrapper activates with: micromamba, conda, or auto"
    ),
    micromamba_bin: str = typer.Option(
        "micromamba", help="micromamba binary the wrapper should invoke"
    ),
) -> None:
    """Register this env's Jupyter kernel, launched via an activation wrapper.

    Makes the env usable from a Jupyter server hosted outside it (e.g. an HPC
    OnDemand portal): the kernelspec launches through a wrapper that activates
    the env first, so shell magics and activate.d-dependent packages work
    inside notebooks.
    """
    from cstar_forge.register_kernel import RegisterKernelError
    from cstar_forge.register_kernel import register_kernel as _register_kernel

    try:
        _register_kernel(
            name=name,
            display_name=display_name,
            clean=clean,
            package_manager=package_manager,
            micromamba_bin=micromamba_bin,
            log=typer.echo,
        )
    except RegisterKernelError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc


@app.command()
def show_paths(
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output paths as JSON instead of human-readable text.",
    ),
) -> None:
    """Show the detected compute system and configured data paths.

    A warning-free replacement for ``python -m cstar_forge.config show-paths``.
    """
    from cstar_forge.config import format_paths

    typer.echo(format_paths(as_json=json_output))


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
