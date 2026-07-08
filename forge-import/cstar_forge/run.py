"""Forge-side entry point for running the forge application on this machine.

This is the **disposable host-resolution glue**: it auto-detects the host via
``cstar_forge.config`` and injects a ``HostPaths`` into the host-independent
``process_spec_config``. Users run this (or its CLI); paths are auto-detected, never
typed. When the forge application relocates into C-Star, C-Star provides its own entry
point + host resolution, and this module is not carried over.

CLI:  ``python -m cstar_forge.run <spec_config.yml> [options]``
"""

from __future__ import annotations

import argparse
import logging

from cstar_forge import config
from cstar_forge.forge.spec_config import SpecConfig
from cstar_forge.forge.spec_config_engine import process_spec_config


def process(spec, **kwargs):
    """Resolve this machine's host and run ``process_spec_config`` on it.

    Thin Forge convenience: deduces a ``HostPaths`` via ``config.resolve_host()`` and
    injects it, so callers never supply paths by hand.
    """
    return process_spec_config(spec, host=config.resolve_host(), **kwargs)


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cstar_forge.run",
        description="Process a spec_config.yml on this machine "
        "(generate inputs + configure build).",
    )
    parser.add_argument("spec_config", help="path to a spec_config.yml")
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
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = SpecConfig.from_yaml(args.spec_config)
    host = config.resolve_host()
    print(
        host.summary(
            casename=cfg.casename,
            run_output_dir=str(cfg.run_output_dir(host.scratch)),
        )
    )
    if args.host_only:
        return 0

    executor = process_spec_config(
        cfg,
        host=host,
        ensure_data=not args.no_data,
        generate=not args.no_generate,
        configure=not args.no_configure,
        clobber=args.clobber,
        use_dask=not args.no_dask,
    )
    if not args.no_configure:
        print(f"\nBuild blueprint: {executor.path_blueprint(stage='build')}")
        print("Run it with:  cstar blueprint run <path>")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
