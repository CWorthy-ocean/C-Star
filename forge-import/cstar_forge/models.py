"""
Pydantic models for the ModelSpec catalog format.

A ModelSpec is a single YAML (``model.yml``) with two top-level sections:
``code`` (roms/marbl/pio source pins + template refs) and ``model_settings``
(flat, mirrors ``ForgeBlueprint.model_settings`` 1:1). It intentionally holds
nothing a Domain/Forcing/Output spec already provides -- no grid/initial-
conditions/forcing source selection (a ForcingSpec's job) and no output-control
sections (an OutputSpec's job). Both must always be explicitly selected; there
is no more "model default" fallback embedded here.

The forcing/IC item models (``SurfaceForcingItem``, ``BoundaryForcingItem``, etc.)
are defined once in ``cstar_forge.forge.forge_blueprint`` and re-exported here
(single source of truth -- see ``docs/roms-tools-contributor-guide.md`` and
``test_roms_tools_coverage.py::test_forge_item_models_are_single_sourced``) for
callers that import them from ``cstar_forge.models``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from cstar_forge.forge.forge_blueprint import (
    BoundaryForcingItem,
    CodeRepo,
    RiverForcingItem,
    SourceSpec,
    SurfaceForcingItem,
    TidalForcingItem,
    TopographySource,
)
from cstar_forge.forge.forge_blueprint import (
    InitialConditions as InitialConditionsInput,
)
from cstar_forge.forge.forge_blueprint import (
    OpenBoundaries as OpenBoundaries,
)

__all__ = [
    "BoundaryForcingItem",
    "InitialConditionsInput",
    "ModelCode",
    "ModelSpec",
    "ModelTemplates",
    "OpenBoundaries",
    "RiverForcingItem",
    "SourceSpec",
    "SurfaceForcingItem",
    "TidalForcingItem",
    "TopographySource",
    "load_models_yaml",
]


class ModelTemplates(BaseModel):
    """A ModelSpec's compile/run-time template file references.

    Deliberately has no ``location``: that's filled in later from the resolver's
    ``templates_repo`` default (or a CLI override), since a ModelSpec doesn't pin
    which fork/mirror of the forge repo serves its templates.
    """

    model_config = ConfigDict(extra="forbid")

    directory: str
    files: list[str] = Field(default_factory=list)


class ModelCode(BaseModel):
    """Code repositories for a ModelSpec: source pins + template refs.

    Mirrors ``forge.forge_blueprint.Code`` (roms/marbl/pio/templates_compile_time/
    templates_run_time), but with the lighter ``ModelTemplates`` shape for the
    template refs, since a ModelSpec only ever authors directory+files, never a
    resolved ``location`` (that comes from the resolver's ``templates_repo``).
    """

    model_config = ConfigDict(extra="forbid")

    roms: CodeRepo
    marbl: CodeRepo | None = None
    pio: CodeRepo | None = None
    templates_commit: str | None = None
    templates_compile_time: ModelTemplates
    templates_run_time: ModelTemplates


class ModelSpec(BaseModel):
    """Description of an ocean model configuration (e.g., ROMS/MARBL).

    Parameters
    ----------
    name : str
        Logical name of the model (e.g., "cson_roms-marbl_v0.1").
    code : ModelCode
        Code repository refs (roms/marbl/pio) + template refs.
    model_settings : dict[str, Any]
        Flat model-specific physics/numerics defaults, mirroring
        ``ForgeBlueprint.model_settings`` 1:1. Contains nothing a Domain/Forcing/
        Output spec already provides.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    code: ModelCode
    model_settings: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_settings_templates_cross_ref(self) -> ModelSpec:
        """Cross-validate that compile-time template files have corresponding
        model_settings keys.

        Only files ending with ``.j2`` in ``code.templates_compile_time.files``
        are validated. Each ``.j2`` template file should have a corresponding
        top-level key in ``model_settings`` (e.g. ``cppdefs.opt.j2`` -> ``"cppdefs"``
        -- this also guarantees ``cppdefs`` itself is present, since it's always
        one of the compile-time template files).
        """
        template_files = self.code.templates_compile_time.files or []
        template_base_names: set[str] = set()
        for f in template_files:
            if not f.endswith(".j2"):
                continue
            base_name = f[: -len(".j2")]
            section_name = base_name.split(".")[0] if "." in base_name else base_name
            template_base_names.add(section_name)

        missing_keys = template_base_names - set(self.model_settings.keys())
        if missing_keys:
            raise ValueError(
                f"Template files with sections {sorted(missing_keys)} do not have "
                f"corresponding keys in model_settings. Available keys: "
                f"{sorted(self.model_settings.keys())}"
            )
        return self

    @model_validator(mode="after")
    def _validate_template_files_exist(self) -> ModelSpec:
        """Best-effort: check that template files exist at the forge repo root
        (each stage's ``directory`` is repo-root-relative). Silently skipped if
        the repo root can't be found (e.g. an installed-package-only environment,
        where the checked-out forge repo's ``templates/`` tree isn't necessarily
        present) -- this is a development-time nicety, not a runtime requirement.
        """
        repo_root = Path(__file__).resolve().parents[1]
        for stage_name, stage in (
            ("compile_time", self.code.templates_compile_time),
            ("run_time", self.code.templates_run_time),
        ):
            template_dir = repo_root / stage.directory
            if not template_dir.exists():
                continue
            missing = [f for f in stage.files if not (template_dir / f).exists()]
            if missing:
                raise FileNotFoundError(
                    f"{stage_name} template files listed in model spec do not "
                    f"exist in {template_dir}:\n"
                    f"  Missing files: {sorted(missing)}\n"
                    f"  Available files: "
                    f"{sorted(p.name for p in template_dir.iterdir() if p.is_file())}"
                )
        return self


def load_models_yaml(path: Path, model_name: str) -> ModelSpec:
    """Load a ModelSpec from a ``model.yml`` file.

    Parameters
    ----------
    path : Path
        Path to the model.yml file.
    model_name : str
        Name of the model (used as ``ModelSpec.name``; either the multi-model
        block key, or -- for a single-model file -- the directory/file's logical
        name).

    Returns
    -------
    ModelSpec

    Raises
    ------
    KeyError
        If the requested model is not present in a multi-model YAML file.
    ValueError
        If required fields are missing.
    """
    with path.open() as f:
        data = yaml.safe_load(f) or {}

    _SINGLE_MODEL_KEYS = frozenset({"code", "model_settings"})
    if model_name in data:
        block = data[model_name]  # multi-model file
    elif _SINGLE_MODEL_KEYS.intersection(data.keys()):
        block = data  # single-model file: content at top level, filename is model name
    else:
        raise KeyError(f"Model '{model_name}' not found in models YAML file: {path}")

    if "code" not in block:
        raise ValueError(f"Model '{model_name}' must specify 'code' in model.yml")

    code_block = block["code"]

    def _repo(val: dict[str, Any] | None) -> CodeRepo | None:
        if not val:
            return None
        commit = val.get("commit")
        return CodeRepo(
            location=val.get("location"),
            commit=str(commit) if commit is not None else None,
            branch=val.get("branch"),
        )

    roms = _repo(code_block.get("roms"))
    if roms is None:
        raise ValueError(f"Model '{model_name}' must include 'roms' in code")

    def _templates(stage: str) -> ModelTemplates:
        t = code_block.get(f"templates_{stage}", {}) or {}
        return ModelTemplates(directory=t.get("directory"), files=t.get("files", []))

    model_code = ModelCode(
        roms=roms,
        marbl=_repo(code_block.get("marbl")),
        pio=_repo(code_block.get("pio")),
        templates_commit=code_block.get("templates_commit"),
        templates_compile_time=_templates("compile_time"),
        templates_run_time=_templates("run_time"),
    )

    return ModelSpec(
        name=model_name,
        code=model_code,
        model_settings=block.get("model_settings", {}) or {},
    )
