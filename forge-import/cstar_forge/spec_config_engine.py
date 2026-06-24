"""
Phase 2 processing engine: ingest a :class:`SpecConfig` and run the heavy work
(``generate_inputs`` + ``configure_build``) on *this* machine.

This is the counterpart to the Phase-1 resolver (``spec_config_resolve``). It runs
on the user's machine of choice, where the **host** (machine config + data paths) is
resolved from :mod:`cstar_forge.config` — nothing host-specific is read from the
config file. The reviewed, host-independent ``SpecConfig`` provides everything else.

Strategy (see ``docs/spec-config-inventory.md`` §3): the existing
``CstarSpecBuilder`` already performs host resolution, grid building, input
generation, and namelist/cppdefs writing. So Phase 2:

1. reconstructs a ``CstarSpecBuilder`` from the config's atomic inputs (identity,
   run window, grid kwargs, boundaries, partitioning, CDR) — the builder resolves
   the host and the artifact-derived values (``s_coord``, file paths, ``run_output_dir``,
   ``output_root_name``) itself;
2. runs ``ensure_source_data`` → ``generate_inputs``;
3. **overlays the reviewed ``model_settings``** from the config onto the builder via
   ``configure_build(compile_time_settings=…, run_time_settings=…)`` — so any edits a
   user made to the config (physics/output/cppdefs/timestep) win over the builder's
   re-derived defaults, while the processing-filled sections (grid/initial/forcing/
   s_coord/title/output_root_name, which the config deliberately omits) come from the
   builder.

The result is the usual blueprint + NetCDF inputs + ``namelist.nml`` + ``cppdefs.opt``,
ready for ``cstar blueprint run``.
"""

from __future__ import annotations

import argparse
import copy
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

from .spec_config import SpecConfig

logger = logging.getLogger(__name__)

# Sections the config omits because they are filled at processing time; listed here
# only for documentation/clarity (the overlay never touches them).
PROCESSING_FILLED_SECTIONS = ("grid", "initial", "forcing", "s_coord",
                              "title", "output_root_name")


def spec_config_to_builder_kwargs(cfg: SpecConfig) -> Dict[str, Any]:
    """Map a ``SpecConfig``'s atomic inputs to ``CstarSpecBuilder`` constructor kwargs.

    Host/machine/path values are intentionally NOT passed — the builder resolves
    those from :mod:`cstar_forge.config` on the run host.
    """
    return dict(
        description=cfg.identity.description,
        model_name=cfg.identity.model_name,
        grid_name=cfg.identity.grid_name,
        grid_kwargs=dict(cfg.domain.grid_kwargs),
        open_boundaries=cfg.domain.open_boundaries.model_dump(),
        partitioning=cfg.domain.partitioning.model_dump(),
        start_time=cfg.run.start_date,
        end_time=cfg.run.end_date,
        ensemble_id=cfg.identity.ensemble_id,
        cdr_forcing=cfg.sources.cdr_forcing,
    )


def split_model_settings(cfg: SpecConfig) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Split the flat ``model_settings`` into (run_time_overrides, compile_time_overrides).

    ``cppdefs`` is the only compile-time section; everything else is a namelist
    (run-time) section. These are passed to ``configure_build`` as overrides so the
    reviewed config wins over the builder's re-derived defaults.
    """
    run_overrides = {k: copy.deepcopy(v) for k, v in cfg.model_settings.items()}
    cppdefs = run_overrides.pop("cppdefs", None)
    compile_overrides = {"cppdefs": copy.deepcopy(cppdefs)} if cppdefs is not None else {}
    return run_overrides, compile_overrides


def resolve_host(cfg: Optional[SpecConfig] = None) -> Dict[str, Any]:
    """Resolve the host (machine tag + data paths) from :mod:`cstar_forge.config`.

    If ``cfg`` is given, also include the host-derived run paths
    (``run_output_dir`` / namelist ``output_root_name``) computed from the scratch path.
    """
    from . import config

    p = config.paths
    info: Dict[str, Any] = {
        "system": config.system,
        "paths": {k: str(getattr(p, k)) for k in ("source_data", "input_data", "scratch", "catalog")},
    }
    mc = getattr(config, "machine_config", None)
    if mc is not None:
        info["machine"] = {
            "account": getattr(mc, "account", None),
            "pes_per_node": getattr(mc, "pes_per_node", None),
            "queues": getattr(mc, "queues", None),
        }
    if cfg is not None:
        info["run_output_dir"] = str(cfg.run_output_dir(p.scratch))
        info["output_root_name"] = cfg.output_root_name(p.scratch)
        info["casename"] = cfg.casename
    return info


def host_summary(cfg: Optional[SpecConfig] = None) -> str:
    """A human-readable one-block summary of the resolved host."""
    h = resolve_host(cfg)
    lines = [f"Host: {h['system']}"]
    if "machine" in h and h["machine"].get("account"):
        lines.append(f"  account: {h['machine']['account']}  pes/node: {h['machine'].get('pes_per_node')}")
    for k, v in h["paths"].items():
        lines.append(f"  {k:11s} -> {v}")
    if cfg is not None:
        lines.append(f"  casename     -> {h['casename']}")
        lines.append(f"  run_output   -> {h['run_output_dir']}")
    return "\n".join(lines)


def _default_builder_factory(**kwargs):
    # Imported lazily so this module (and the dependency-light bits above) can be
    # imported without the full forge stack.
    from ._core import CstarSpecBuilder

    return CstarSpecBuilder(**kwargs)


def process_spec_config(
    spec: Union[SpecConfig, str, Path],
    *,
    ensure_data: bool = True,
    generate: bool = True,
    configure: bool = True,
    clobber: bool = False,
    use_dask: bool = True,
    partition_files: bool = False,
    builder_factory: Optional[Callable[..., Any]] = None,
) -> Any:
    """Run Phase-2 processing for a ``SpecConfig`` (object or path to a YAML file).

    Returns the ``CstarSpecBuilder`` (so callers can reach ``.path_blueprint('build')``,
    ``.prep_cstar_environment(...)``, ``.run()``, etc.).

    ``builder_factory`` is injectable for testing; it defaults to ``CstarSpecBuilder``.
    """
    cfg = spec if isinstance(spec, SpecConfig) else SpecConfig.from_yaml(spec)
    factory = builder_factory or _default_builder_factory

    logger.info("Resolved host:\n%s", host_summary(cfg))

    builder = factory(**spec_config_to_builder_kwargs(cfg))

    if ensure_data:
        builder.ensure_source_data()
    if generate:
        builder.generate_inputs(clobber=clobber, use_dask=use_dask,
                                partition_files=partition_files)
    if configure:
        run_overrides, compile_overrides = split_model_settings(cfg)
        builder.configure_build(compile_time_settings=compile_overrides,
                                run_time_settings=run_overrides)
    return builder


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cstar_forge.spec_config_engine",
        description="Phase 2: process a spec_config.yml on this machine "
                    "(generate inputs + configure build).")
    parser.add_argument("spec_config", help="path to a spec_config.yml")
    parser.add_argument("--no-data", action="store_true", help="skip ensure_source_data")
    parser.add_argument("--no-generate", action="store_true", help="skip generate_inputs")
    parser.add_argument("--no-configure", action="store_true", help="skip configure_build")
    parser.add_argument("--clobber", action="store_true", help="overwrite existing input files")
    parser.add_argument("--no-dask", action="store_true", help="disable dask in input generation")
    parser.add_argument("--host-only", action="store_true",
                        help="just print the resolved host and exit")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = SpecConfig.from_yaml(args.spec_config)
    print(host_summary(cfg))
    if args.host_only:
        return 0

    builder = process_spec_config(
        cfg,
        ensure_data=not args.no_data,
        generate=not args.no_generate,
        configure=not args.no_configure,
        clobber=args.clobber,
        use_dask=not args.no_dask,
    )
    if not args.no_configure:
        print(f"\nBuild blueprint: {builder.path_blueprint(stage='build')}")
        print("Run it with:  cstar blueprint run <path>")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
