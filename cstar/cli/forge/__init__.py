"""C-Star Forge subcommands for the ``cstar`` CLI.

Attached as a core subcommand of the root ``cstar`` app (see
``attach_subcommands`` in ``cstar/cli/cli.py``), so the commands are always
available::

    cstar forge run <forge_blueprint.yaml> [executor options...]
    cstar forge wizard [--port 8866] [voila options...]
    cstar forge copy-notebook [--dest ...] [--force]
    cstar forge show-paths [--json]

(``cstar forge register-kernel`` moved to ``cstar env register-kernel``; see
``cstar/cli/environment/register_kernel.py``.)

``forge run`` exposes the full executor option set (stage selection, dask
tuning, diagnostics) — per-invocation by design, not blueprint content — and
calls ``cstar.applications.forge.runtime.run_blueprint``. The no-frills alternative, ``cstar
blueprint run``, executes a forge blueprint through the C-Star application
framework with defaults.
"""

import os
import shutil
from importlib.resources import as_file, files
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(
    help="C-Star Forge: generate domains and launch the blueprint wizard."
)


def _split_only_inputs(values: list[str]) -> list[str]:
    """Flatten ``--only-inputs`` occurrences, splitting each on commas.

    Lets ``--only-inputs grid,tidal`` and ``--only-inputs grid --only-inputs
    tidal`` express the same selection; whitespace around each name is
    stripped and empty entries are dropped.
    """
    return [
        name for value in values for raw in value.split(",") if (name := raw.strip())
    ]


@app.command()
def run(
    forge_blueprint: Annotated[
        str, typer.Argument(help="path to a forge_blueprint.yaml")
    ],
    no_data: Annotated[
        bool, typer.Option("--no-data", help="skip ensure_source_data")
    ] = False,
    no_generate: Annotated[
        bool, typer.Option("--no-generate", help="skip generate_inputs")
    ] = False,
    no_configure: Annotated[
        bool, typer.Option("--no-configure", help="skip configure_build")
    ] = False,
    clobber: Annotated[
        bool, typer.Option("--clobber", help="overwrite existing input files")
    ] = False,
    no_dask: Annotated[
        bool,
        typer.Option("--no-dask", help="disable dask in input generation"),
    ] = False,
    dask_num_workers: Annotated[
        int,
        typer.Option(
            "--dask-num-workers",
            help="cap on dask's default local threaded-scheduler worker count "
            "during input generation (each worker's own BLAS/numba call is, "
            "in turn, capped to its own share of the remaining cores), to "
            "avoid thread oversubscription hangs on high-core HPC nodes. "
            "Ignored with --no-dask. Distinct from --dask-workers, which "
            "sizes the opt-in --dask distributed Client.",
        ),
    ] = 8,
    serialize_dask_write: Annotated[
        bool | None,
        typer.Option(
            "--serialize-dask-write/--no-serialize-dask-write",
            help="force every IC/boundary NetCDF write onto dask's "
            "synchronous scheduler, one task at a time with BLAS/numba "
            "boosted to every core, with --serialize-dask-write: a manual "
            "low-memory/troubleshooting tool that bounds peak memory to one "
            "task's footprint at a wall-time cost (default and "
            "--no-serialize-dask-write are the ordinary concurrent write; "
            "PyESPER protects its own chunks). This is only the FALLBACK: a "
            "bgc source that sets its own 'serialize_dask' in the blueprint "
            "(BgcSourceItem) always wins for that source, whatever this flag "
            "is set to -- this flag only decides the write behavior for "
            "sources that leave it unset (None). Ignored with --no-dask.",
        ),
    ] = None,
    subchunk: Annotated[
        bool,
        typer.Option(
            "--subchunk/--no-subchunk",
            help="just-in-time build a kerchunk-subchunked reference for "
            "multi-file GLORYS sources and read from it instead of the raw "
            "per-day files (see cstar/applications/forge/glorys_subchunk.py). "
            "On by default; disable with --no-subchunk",
        ),
    ] = True,
    only_inputs: Annotated[
        list[str],
        typer.Option(
            "--only-inputs",
            metavar="INPUT",
            callback=_split_only_inputs,
            help="generate only these input categories (grid, "
            "initial_conditions, surface, boundary, tidal, river, cdr) and "
            "skip configure_build/blueprint emission -- a one-off run for "
            "slow or human-checked inputs. Existing files are still reused "
            "per the normal skip-existing logic. Re-run without this flag "
            "later to generate the rest and emit the blueprint. Repeat the "
            "flag or pass a comma-separated list (--only-inputs grid,tidal).",
        ),
    ] = [],
    host_only: Annotated[
        bool,
        typer.Option("--host-only", help="just print the resolved host and exit"),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            help="enable verbose diagnostics: timestamped logging throughout "
            "the executor, roms-tools verbose=True on the calls that support "
            "it, and timing/memory instrumentation around roms-tools "
            "constructors and saves",
        ),
    ] = False,
    working_dir: Annotated[
        str | None,
        typer.Option(
            "--working-dir",
            help="override the spec's working_dir (per-run artifact root) "
            "for this host",
        ),
    ] = None,
    dask: Annotated[
        bool,
        typer.Option(
            "--dask",
            help="start a dask.distributed Client for this run, so input "
            "generation uses it instead of dask's default local threaded "
            "scheduler. Omitting this flag leaves current behavior "
            "unchanged. Combine with the other --dask-* flags to sweep "
            "cluster configs.",
        ),
    ] = False,
    dask_workers: Annotated[
        int | None,
        typer.Option("--dask-workers", help="n_workers (requires --dask)"),
    ] = None,
    dask_threads_per_worker: Annotated[
        int | None,
        typer.Option(
            "--dask-threads-per-worker",
            help="threads_per_worker (requires --dask)",
        ),
    ] = None,
    dask_memory_limit: Annotated[
        str | None,
        typer.Option(
            "--dask-memory-limit",
            help="per-worker memory_limit, e.g. '4GB' (requires --dask)",
        ),
    ] = None,
    dask_processes: Annotated[
        bool | None,
        typer.Option(
            "--dask-processes/--no-dask-processes",
            help="process-based workers (--dask-processes) vs thread-based "
            "(--no-dask-processes); omit to use dask's own default "
            "(requires --dask)",
        ),
    ] = None,
    dask_dashboard_address: Annotated[
        str | None,
        typer.Option(
            "--dask-dashboard-address",
            help="dashboard address, e.g. ':8787' (requires --dask)",
        ),
    ] = None,
) -> None:
    """Process a forge blueprint with the full executor option set."""
    from cstar.applications.forge.runtime import run_blueprint

    code = run_blueprint(
        forge_blueprint=forge_blueprint,
        no_data=no_data,
        no_generate=no_generate,
        no_configure=no_configure,
        clobber=clobber,
        no_dask=no_dask,
        dask_num_workers=dask_num_workers,
        serialize_dask_write=serialize_dask_write,
        subchunk=subchunk,
        only_inputs=only_inputs or None,
        host_only=host_only,
        verbose=verbose,
        working_dir=working_dir,
        dask=dask,
        dask_workers=dask_workers,
        dask_threads_per_worker=dask_threads_per_worker,
        dask_memory_limit=dask_memory_limit,
        dask_processes=dask_processes,
        dask_dashboard_address=dask_dashboard_address,
    )
    raise typer.Exit(code)


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
    notebook = files("cstar.wizard") / "_voila_app.ipynb"
    # Steer MPI's libfabric away from the default "sockets" provider before
    # exec'ing voila (the kernel inherits our environment): the first xESMF
    # regrid in a wizard kernel initializes ESMF/MPI, and the sockets
    # provider's progress threads busy-poll at ~100% CPU each (macOS) for the
    # life of the kernel; the tcp provider services the same single-process
    # MPI without spinning. setdefault so an explicit user choice wins; batch
    # ROMS runs launched outside this command keep their own default.
    os.environ.setdefault("FI_PROVIDER", "tcp")
    # Hide notebook 7.x's JupyterLab extension from voila's frontend. Voila
    # 0.5.12 bundles JupyterLab core 4.2.5, but @jupyter-notebook/lab-extension
    # as shipped by notebook >=7.3 is built against JupyterLab 4.4+/4.6 APIs.
    # Module federation then fails to construct it ("The getter for the shared
    # module is not a function"), killing the whole frontend bundle before it
    # attaches the kernel websocket -- the symptom is a blank page plus a
    # stream of "Kernel does not exist" 404s in the server log. The extension
    # is useless under voila anyway. Denylist goes before ctx.args so a caller
    # can still pass their own. Revisit once voila ships a JupyterLab 4.4+
    # frontend.
    argv = [
        "voila",
        str(notebook),
        f"--port={port}",
        '--Voila.tornado_settings={"allow_origin": "*"}',
        '--VoilaConfiguration.extension_denylist=["@jupyter-notebook/lab-extension"]',
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
    land in site-packages. Re-run with --force after upgrading cstar-ocean to
    refresh the copy.
    """
    with as_file(files("cstar.wizard") / "forge-blueprint-wizard.ipynb") as src:
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
def show_paths(
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output paths as JSON instead of human-readable text.",
    ),
) -> None:
    """Show the detected compute system and configured data paths.

    A warning-free replacement for ``python -m cstar.applications.forge.config show-paths``.
    """
    from cstar.applications.forge.config import format_paths

    typer.echo(format_paths(as_json=json_output))


def _exec_voila(argv: list[str]) -> None:
    """Replace this process with voila (signals/Ctrl-C flow to the server)."""
    if shutil.which("voila") is None:
        typer.echo(
            "voila is not installed in this environment. It is a core "
            "dependency of cstar-ocean; reinstall or upgrade cstar-ocean to "
            "get it.",
            err=True,
        )
        raise typer.Exit(1)
    os.execvp(argv[0], argv)


def main() -> None:  # pragma: no cover - thin standalone hook, exercised manually
    """Allow ``python -m cstar.cli.forge`` as a cstar-independent fallback."""
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
