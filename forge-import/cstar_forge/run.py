"""Forge-side entry point for running the forge application on this machine.

This is the **disposable host-resolution glue**: it auto-detects the host via
``cstar_forge.config`` and injects a ``HostPaths`` into the host-independent
``process_forge_blueprint``. Users run this (or its CLI); paths are auto-detected, never
typed. When the forge application relocates into C-Star, C-Star provides its own entry
point + host resolution, and this module is not carried over.

CLI:  ``python -m cstar_forge.run <forge_blueprint.yaml> [options]``
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import sys
from datetime import datetime
from pathlib import Path

from cstar_forge import config
from cstar_forge.forge.forge_blueprint import ForgeBlueprint
from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint

# Loggers whose level gets lowered while capturing, so the file actually receives
# useful content on the C-Star app path (which never calls logging.basicConfig).
_CAPTURED_LOGGER_NAMES = ("cstar_forge", "roms_tools", "cstar")


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


@contextlib.contextmanager
def _capture_output(working_dir, *, verbose=False):
    """Tee screen output (print + logging) into
    ``<working_dir>/logs/forge_<timestamp>.log`` for the duration of the block, in
    addition to the existing screen output.

    Each run gets its own timestamped file (``working_dir`` is reused across re-runs).
    Logging is routed to the file via a dedicated handler on the root logger rather
    than through the stdout/stderr tee, so log lines aren't double-written when a
    pre-existing ``basicConfig`` handler (the CLI's ``--verbose`` setup) also writes
    to the original stderr.
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
            print(f"Forge log: {log_path}")
            yield log_path
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
    host = config.resolve_host(wd)
    with _capture_output(host.working_dir, verbose=kwargs.get("verbose", False)):
        return process_forge_blueprint(cfg, host=host, **kwargs)


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cstar_forge.run",
        description="Process a forge_blueprint.yaml on this machine "
        "(generate inputs + configure build).",
    )
    parser.add_argument("forge_blueprint", help="path to a forge_blueprint.yaml")
    parser.add_argument(
        "--no-data", action="store_true", help="skip ensure_source_data"
    )
    parser.add_argument(
        "--no-generate", action="store_true", help="skip generate_inputs"
    )
    parser.add_argument(
        "--no-configure", action="store_true", help="skip configure_build"
    )
    parser.add_argument(
        "--clobber", action="store_true", help="overwrite existing input files"
    )
    parser.add_argument(
        "--no-dask", action="store_true", help="disable dask in input generation"
    )
    parser.add_argument(
        "--dask-num-workers",
        type=int,
        default=8,
        help="cap on dask's default local threaded-scheduler worker count during "
        "input generation, paired with pinning BLAS/OpenMP to 1 thread, to avoid "
        "thread oversubscription hangs on high-core HPC nodes. Ignored with "
        "--no-dask. Distinct from --dask-workers, which sizes the opt-in "
        "--dask distributed Client.",
    )
    parser.add_argument(
        "--subchunk",
        action="store_true",
        help="interim hack: just-in-time build a kerchunk-subchunked reference for "
        "multi-file GLORYS sources and read from it instead of the raw per-day "
        "files (see cstar_forge/forge/glorys_subchunk.py)",
    )
    parser.add_argument(
        "--only-inputs",
        nargs="+",
        default=None,
        metavar="INPUT",
        help="generate only these input categories (grid, initial_conditions, "
        "surface, boundary, tidal, river, cdr) and skip configure_build/blueprint "
        "emission -- a one-off run for slow or human-checked inputs. Existing "
        "files are still reused per the normal skip-existing logic. Re-run "
        "without this flag later to generate the rest and emit the blueprint.",
    )
    parser.add_argument(
        "--stage-ic-sources",
        action="store_true",
        help="I/O performance experiment: copy the initial-conditions source "
        "netCDF files (physics + bgc) into the working directory (scratch) "
        "before constructing InitialConditions, and read from those copies "
        "instead of the originals on project space",
    )
    parser.add_argument(
        "--host-only", action="store_true", help="just print the resolved host and exit"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable verbose diagnostics: timestamped logging throughout the "
        "executor, roms-tools verbose=True on the calls that support it, and "
        "timing/memory instrumentation around roms-tools constructors and saves",
    )
    parser.add_argument(
        "--working-dir",
        default=None,
        help="override the spec's working_dir (per-run artifact root) for this host",
    )
    parser.add_argument(
        "--dask",
        action="store_true",
        help="start a dask.distributed Client for this run, so input generation "
        "uses it instead of dask's default local threaded scheduler. Omitting "
        "this flag leaves current behavior unchanged. Combine with the other "
        "--dask-* flags to sweep cluster configs.",
    )
    parser.add_argument(
        "--dask-workers", type=int, default=None, help="n_workers (requires --dask)"
    )
    parser.add_argument(
        "--dask-threads-per-worker",
        type=int,
        default=None,
        help="threads_per_worker (requires --dask)",
    )
    parser.add_argument(
        "--dask-memory-limit",
        default=None,
        help="per-worker memory_limit, e.g. '4GB' (requires --dask)",
    )
    parser.add_argument(
        "--dask-processes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="process-based workers (--dask-processes) vs thread-based "
        "(--no-dask-processes); omit to use dask's own default (requires --dask)",
    )
    parser.add_argument(
        "--dask-dashboard-address",
        default=None,
        help="dashboard address, e.g. ':8787' (requires --dask)",
    )
    args = parser.parse_args(argv)

    if args.verbose:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            force=True,
        )
        for name in ("cstar_forge", "roms_tools", "cstar"):
            logging.getLogger(name).setLevel(logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)
    cfg = ForgeBlueprint.from_yaml(args.forge_blueprint)
    wd = args.working_dir if args.working_dir is not None else cfg.working_dir
    host = config.resolve_host(wd)

    with _capture_output(host.working_dir, verbose=args.verbose):
        print(host.summary(casename=cfg.casename))
        if args.host_only:
            return 0

        dask_client = None
        if args.dask:
            from dask.distributed import Client

            client_kwargs = {"local_directory": "/tmp"}
            if args.dask_workers is not None:
                client_kwargs["n_workers"] = args.dask_workers
            if args.dask_threads_per_worker is not None:
                client_kwargs["threads_per_worker"] = args.dask_threads_per_worker
            if args.dask_memory_limit is not None:
                client_kwargs["memory_limit"] = args.dask_memory_limit
            if args.dask_processes is not None:
                client_kwargs["processes"] = args.dask_processes
            if args.dask_dashboard_address is not None:
                client_kwargs["dashboard_address"] = args.dask_dashboard_address
            dask_client = Client(**client_kwargs)
            print(f"\n{dask_client}")
            print(f"Dask dashboard: {dask_client.dashboard_link}")

        try:
            executor = process_forge_blueprint(
                cfg,
                host=host,
                ensure_data=not args.no_data,
                generate=not args.no_generate,
                configure=not args.no_configure,
                clobber=args.clobber,
                use_dask=not args.no_dask,
                dask_num_workers=args.dask_num_workers,
                subchunk=args.subchunk,
                stage_ic_sources=args.stage_ic_sources,
                only_inputs=args.only_inputs,
                verbose=args.verbose,
            )
        finally:
            if dask_client is not None:
                dask_client.close()

        if not args.no_configure and not args.only_inputs:
            blueprint_path = executor.path_roms_marbl_blueprint()
            print(f"\nBlueprint: {blueprint_path}")
            print(f"Run it with:  cstar blueprint run {blueprint_path}")
        return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
