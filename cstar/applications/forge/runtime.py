"""Forge-side entry point for running the forge application on this machine.

This is the **disposable host-resolution glue**: it auto-detects the host via
``cstar.applications.forge.config`` and injects a ``HostPaths`` into the host-independent
``process_forge_blueprint``. Users run this (or its CLI); paths are auto-detected, never
typed.

CLI:  ``cstar forge run <forge_blueprint.yaml> [options]`` -- a native ``cstar forge
run`` typer command parses the options and calls ``run_blueprint`` below. That command
is not wired up yet as of this commit; it arrives in a later commit.
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import cstar
from cstar.applications.forge import config
from cstar.applications.forge.blueprint import (
    ForgeBlueprint,
    _installed_version,
)
from cstar.applications.forge.engine import process_forge_blueprint

# Loggers whose level gets lowered while capturing, so the file actually receives
# useful content on the C-Star app path (which never calls logging.basicConfig).
# Forge's own loggers now live under "cstar.applications.forge.*", covered by "cstar".
_CAPTURED_LOGGER_NAMES = ("cstar", "roms_tools")


class _Tee:
    """Write to the original stream AND a file, forwarding attribute access (isatty,
    fileno, encoding, ...) to the original stream so tty-probing libs keep working.

    Lone carriage-return redraws (tqdm/dask progress bars) are written to the screen
    as usual but skipped in the file, so a progress bar doesn't turn into megabytes
    of redraw lines in the log.
    """

    def __init__(self, stream, fh):
        self._stream = stream
        self._fh = fh

    def detach(self):
        """Stop writing to the file.

        Something that grabbed this stream by reference during the run (a lazily
        created logging handler, a tqdm/dask progress bar, a background thread) can
        keep writing through it after ``_capture_output`` exits and the log file is
        closed. Call this before closing the file so those late writes fall through
        to the screen only, instead of raising on the closed file.
        """
        self._fh = None

    def write(self, data):
        self._stream.write(data)
        fh = self._fh
        if fh is not None and data and not (data.startswith("\r") and "\n" not in data):
            try:
                fh.write(data)
            except (ValueError, OSError):
                self._fh = None
        return len(data)

    def flush(self):
        self._stream.flush()
        fh = self._fh
        if fh is not None:
            try:
                fh.flush()
            except (ValueError, OSError):
                self._fh = None

    def __getattr__(self, name):
        return getattr(self._stream, name)


def _version_banner_lines(cfg=None) -> list[str]:
    """Best-effort CWorthy library/version lines for the run-log startup banner.

    Forge is ``cstar.applications.forge`` now -- it has no separate version from
    ``cstar-ocean`` -- so the banner's own library line is ``cstar.__version__``
    directly (a dev/editable install's ``setuptools_scm`` version already embeds
    commit info, e.g. ``0.13.7.dev2+gcb931baef``); ``roms-tools`` comes from
    installed-package metadata (see ``_installed_version``). ``ucla-roms`` and
    MARBL have no pip package, so their pinned git ref is read off ``cfg.code``
    instead, if a blueprint was supplied. Entries with no available value are
    skipped. Never raises -- this is logging, not a dependency check.
    """
    # TODO(punchlist follow-up): warn when installed/pinned versions are known
    # incompatible with each other -- deferred, this only records what's present.
    lines = [f"  cstar-ocean=={cstar.__version__}"]
    roms_tools_version = _installed_version("roms-tools")
    if roms_tools_version:
        lines.append(f"  {roms_tools_version}")

    code = getattr(cfg, "code", None)
    for label, repo in (
        ("ucla-roms", getattr(code, "roms", None)),
        ("marbl", getattr(code, "marbl", None)),
    ):
        commit = getattr(repo, "commit", None)
        branch = getattr(repo, "branch", None)
        if commit:
            lines.append(f"  {label}@commit:{commit}")
        elif branch:
            lines.append(f"  {label}@branch:{branch}")

    return lines


@contextlib.contextmanager
def _capture_output(working_dir, *, verbose=False, cfg=None):
    """Tee screen output (print + logging) into
    ``<working_dir>/logs/forge_<timestamp>.log`` for the duration of the block, in
    addition to the existing screen output.

    Each run gets its own timestamped file (``working_dir`` is reused across re-runs).
    Logging is routed to the file via a dedicated handler on the root logger rather
    than through the stdout/stderr tee, so log lines aren't double-written when a
    pre-existing ``basicConfig`` handler (the CLI's ``--verbose`` setup) also writes
    to the original stderr. ``cfg`` (the resolved ``ForgeBlueprint``, if available) is
    used only to add pinned ucla-roms/marbl git refs to the version banner printed
    at the start of the block (see ``_version_banner_lines``).
    """
    log_dir = Path(working_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"forge_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

    with log_path.open("a", buffering=1) as fh:
        fh.write(f"=== forge run started {datetime.now().isoformat()} ===\n")

        old_out, old_err = sys.stdout, sys.stderr
        out_tee, err_tee = _Tee(old_out, fh), _Tee(old_err, fh)
        sys.stdout, sys.stderr = out_tee, err_tee

        level = logging.DEBUG if verbose else logging.INFO
        handler = logging.StreamHandler(fh)
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        root = logging.getLogger()
        root.addHandler(handler)
        prev_levels = {
            name: logging.getLogger(name).level for name in _CAPTURED_LOGGER_NAMES
        }
        for name in _CAPTURED_LOGGER_NAMES:
            logging.getLogger(name).setLevel(level)

        try:
            # print (not fh.write): sys.stdout is now the tee, so these lines
            # reach the screen, this log file, AND any subprocess-stdout capture
            # a caller (e.g. a C-Star step) sets up around this process.
            for line in _version_banner_lines(cfg):
                print(line)
            print(f"Forge log: {log_path}")
            yield log_path
        except BaseException:
            # The pretty traceback (typer/rich) is rendered by our caller only after
            # this context has restored stderr and closed the file, so record the
            # failure here or the log ends silently mid-run.
            fh.write(traceback.format_exc())
            raise
        finally:
            root.removeHandler(handler)
            for name, prev_level in prev_levels.items():
                logging.getLogger(name).setLevel(prev_level)
            sys.stdout, sys.stderr = old_out, old_err
            # Detach the tees we created (by reference, not by re-reading sys.stdout
            # /sys.stderr here) before the `with` above closes fh, so anything that
            # captured out_tee/err_tee mid-run keeps writing safely afterwards.
            out_tee.detach()
            err_tee.detach()


def process(spec, *, working_dir=None, **kwargs):
    """Resolve this machine's host and run ``process_forge_blueprint`` on it.

    Thin Forge convenience: deduces a ``HostPaths`` via ``config.resolve_host()`` and
    injects it, so callers never supply paths by hand. ``working_dir`` defaults to the
    spec's stored ``working_dir`` (a per-host override may be passed here). Screen
    output (print + logging) is teed into ``<host.working_dir>/logs/`` for the
    duration of the run -- see ``_capture_output``.
    """
    cfg = spec if isinstance(spec, ForgeBlueprint) else ForgeBlueprint.from_yaml(spec)
    wd = working_dir if working_dir is not None else cfg.working_dir
    config.ensure_data_dirs()
    host = config.resolve_host(wd)
    with _capture_output(
        host.working_dir, verbose=kwargs.get("verbose", False), cfg=cfg
    ):
        return process_forge_blueprint(cfg, host=host, **kwargs)


def _dask_client_kwargs(
    *,
    dask_workers: int | None,
    dask_threads_per_worker: int | None,
    dask_memory_limit: str | None,
    dask_processes: bool | None,
    dask_dashboard_address: str | None,
) -> dict:
    """Assemble the ``dask.distributed.Client`` kwargs for ``--dask``.

    Node-local disk beats network scratch for dask spill; respects ``TMPDIR``
    when set, but defaults to ``/tmp`` rather than network-mounted scratch.
    Each ``--dask-*`` flag is omitted from the kwargs (letting dask apply its
    own default) unless explicitly given.
    """
    client_kwargs: dict = {"local_directory": os.environ.get("TMPDIR", "/tmp")}
    if dask_workers is not None:
        client_kwargs["n_workers"] = dask_workers
    if dask_threads_per_worker is not None:
        client_kwargs["threads_per_worker"] = dask_threads_per_worker
    if dask_memory_limit is not None:
        client_kwargs["memory_limit"] = dask_memory_limit
    if dask_processes is not None:
        client_kwargs["processes"] = dask_processes
    if dask_dashboard_address is not None:
        client_kwargs["dashboard_address"] = dask_dashboard_address
    return client_kwargs


def run_blueprint(
    *,
    forge_blueprint: str,
    no_data: bool = False,
    no_generate: bool = False,
    no_configure: bool = False,
    clobber: bool = False,
    no_dask: bool = False,
    dask_num_workers: int = 8,
    serialize_dask_write: bool | None = None,
    subchunk: bool = True,
    only_inputs: list[str] | None = None,
    host_only: bool = False,
    verbose: bool = False,
    working_dir: str | None = None,
    dask: bool = False,
    dask_workers: int | None = None,
    dask_threads_per_worker: int | None = None,
    dask_memory_limit: str | None = None,
    dask_processes: bool | None = None,
    dask_dashboard_address: str | None = None,
) -> int:
    """Process a forge blueprint given already-parsed option values.

    Body of the former ``main()`` after argument parsing -- the ``cstar forge run``
    typer command (not yet wired up as of this commit; see the module docstring)
    parses ``sys.argv`` and calls this with one keyword argument per option, so the
    argument surface (names, types, defaults) lives once, in the typer command, and
    this function only executes it.
    """
    if verbose:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            force=True,
        )
        for name in _CAPTURED_LOGGER_NAMES:
            logging.getLogger(name).setLevel(logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)
    cfg = ForgeBlueprint.from_yaml(forge_blueprint)
    wd = working_dir if working_dir is not None else cfg.working_dir
    config.ensure_data_dirs()
    host = config.resolve_host(wd)

    with _capture_output(host.working_dir, verbose=verbose, cfg=cfg):
        print(host.summary(casename=cfg.casename))
        if host_only:
            return 0

        dask_client = None
        if dask:
            from dask.distributed import Client

            client_kwargs = _dask_client_kwargs(
                dask_workers=dask_workers,
                dask_threads_per_worker=dask_threads_per_worker,
                dask_memory_limit=dask_memory_limit,
                dask_processes=dask_processes,
                dask_dashboard_address=dask_dashboard_address,
            )
            dask_client = Client(**client_kwargs)
            print(f"\n{dask_client}")
            print(f"Dask dashboard: {dask_client.dashboard_link}")

        try:
            executor = process_forge_blueprint(
                cfg,
                host=host,
                ensure_data=not no_data,
                generate=not no_generate,
                configure=not no_configure,
                clobber=clobber,
                use_dask=not no_dask,
                dask_num_workers=dask_num_workers,
                serialize_dask_write=serialize_dask_write,
                subchunk=subchunk,
                only_inputs=only_inputs,
                verbose=verbose,
            )
        finally:
            if dask_client is not None:
                dask_client.close()

        if not no_configure and not only_inputs:
            blueprint_path = executor.path_roms_marbl_blueprint()
            print(f"\nBlueprint: {blueprint_path}")
            print(f"Run it with:  cstar blueprint run {blueprint_path}")
        return 0


if __name__ == "__main__":  # pragma: no cover - retired entry point
    raise SystemExit(
        "cstar.applications.forge.runtime is not a command; use `cstar forge run "
        "<blueprint>` (a native typer command arriving in a later commit)."
    )
