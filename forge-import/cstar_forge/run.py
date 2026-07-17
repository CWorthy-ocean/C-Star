"""Forge-side entry point for running the forge application on this machine.

This is the **disposable host-resolution glue**: it auto-detects the host via
``cstar_forge.config`` and injects a ``HostPaths`` into the host-independent
``process_forge_blueprint``. Users run this (or its CLI); paths are auto-detected, never
typed. When the forge application relocates into C-Star, C-Star provides its own entry
point + host resolution, and this module is not carried over.

CLI:  ``python -m cstar_forge.run <forge_blueprint.yml> [options]``
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
        description="Process a forge_blueprint.yml on this machine "
        "(generate inputs + configure build).",
    )
    parser.add_argument("forge_blueprint", help="path to a forge_blueprint.yml")
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
        "--host-only", action="store_true", help="just print the resolved host and exit"
    )
    parser.add_argument(
        "--working-dir",
        default=None,
        help="override the spec's working_dir (per-run artifact root) for this host",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)
    cfg = ForgeBlueprint.from_yaml(args.forge_blueprint)
    wd = args.working_dir if args.working_dir is not None else cfg.working_dir
    host = config.resolve_host(wd)
    print(host.summary(casename=cfg.casename))
    if args.host_only:
        return 0

    executor = process_forge_blueprint(
        cfg,
        host=host,
        ensure_data=not args.no_data,
        generate=not args.no_generate,
        configure=not args.no_configure,
        clobber=args.clobber,
        use_dask=not args.no_dask,
    )
    if not args.no_configure:
        print(f"\nBlueprint: {executor.path_roms_marbl_blueprint()}")
        print("Run it with:  cstar blueprint run <path>")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
