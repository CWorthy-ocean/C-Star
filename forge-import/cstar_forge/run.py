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
import logging

from cstar_forge import config
from cstar_forge.forge.forge_blueprint import ForgeBlueprint
from cstar_forge.forge.forge_blueprint_engine import process_forge_blueprint


def process(spec, *, working_dir=None, **kwargs):
    """Resolve this machine's host and run ``process_forge_blueprint`` on it.

    Thin Forge convenience: deduces a ``HostPaths`` via ``config.resolve_host()`` and
    injects it, so callers never supply paths by hand. ``working_dir`` defaults to the
    spec's stored ``working_dir`` (a per-host override may be passed here).
    """
    cfg = spec if isinstance(spec, ForgeBlueprint) else ForgeBlueprint.from_yaml(spec)
    wd = working_dir if working_dir is not None else cfg.working_dir
    return process_forge_blueprint(cfg, host=config.resolve_host(wd), **kwargs)


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
    print(host.summary(casename=cfg.casename))
    if args.host_only:
        return 0

    dask_client = None
    if args.dask:
        from dask.distributed import Client

        client_kwargs = {}
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
            subchunk=args.subchunk,
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
