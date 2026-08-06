"""DomainCatalog: manages the catalog directory structure for C-Star forge."""

from __future__ import annotations

import logging
import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import fsspec
import yaml

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_CATALOG_ROOT = Path(__file__).parent / "catalog"


def _is_github_catalog_url(catalog_root: str) -> bool:
    """Return True if *catalog_root* looks like a GitHub repository URL."""
    s = catalog_root.strip()
    return s.startswith(("https://github.com/", "http://github.com/")) or s.startswith(
        "git@github.com:"
    )


def _parse_github_catalog_url(url: str) -> tuple[str, str, str, Path]:
    """Parse a GitHub URL into ``(org, repo, branch, path_within_repo)``."""
    raw = url.strip().rstrip("/")
    if raw.startswith("git@github.com:"):
        path = raw[len("git@github.com:") :]
    elif "github.com/" in raw:
        path = raw.split("github.com/", 1)[1]
    else:
        raise ValueError(f"Not a GitHub catalog URL: {url!r}")

    path = path.removesuffix(".git")

    parts = [p for p in path.split("/") if p]
    if len(parts) < 2:
        raise ValueError(
            f"Could not parse GitHub org/repo from catalog_root {url!r}. "
            "Expected https://github.com/<org>/<repo>[/path/...]."
        )

    org_name, repo_name = parts[0], parts[1]
    rest = parts[2:]
    branch = "main"
    if rest and rest[0] in ("tree", "blob"):
        if len(rest) >= 2:
            branch = rest[1]
        rest = rest[2:]  # drop tree|blob and branch name

    repo_path = Path(*rest) if rest else Path(".")
    return org_name, repo_name, branch, repo_path


class DomainCatalog:
    """C-Star DomainCatalog manages the hierarchical system of validated/registered "domains."

    The DomainCatalog holds, inside a folder called 'catalog', models and model domains
    that together describe "validated" C-Star model solutions.

    The base of a given catalog (self.catalog_root) is the *inner* catalog directory that
    directly contains the subdirectories below. It can be a local path or a remote URL;
    file access is mediated by fsspec for portability.

    Catalog structure::

        catalog/
        ├── Machines/
        │   ├── MacOS.yaml
        │   ├── NERSC_perlmutter.yaml
        │   └── RCAC_anvil.yaml
        ├── ModelSpec/
        │   └── cson_roms-marbl_v0.1/
        │       └── model.yaml   (single consolidated file: code + model_settings)
        ├── DomainSpec/
        │   ├── ccs-12km/
        │   │   ├── Domain.yaml
        │   │   └── Assets/
        │   └── PAC_2fth_deg/
        │       ├── Domain.yaml
        │       └── Assets/
        ├── Blueprints/  (alias: blueprints/)
        │   └── <machine>/<blueprint-name>/
        │       ├── B_*.yaml
        │       └── Build/
        └── Observations/

    Parameters
    ----------
    catalog_root : str or Path or None
        Root of the catalog (inner directory containing Machines/, ModelSpec/, etc.).
        Defaults to the package-bundled catalog at ``<cstar_forge>/catalog``.
        Pass a github URL string for remote catalogs.
    """

    def __init__(
        self,
        catalog_root: str | Path | None = None,
        initialize_catalog_from: str | Path | None = None,
        initialize_catalog_clobber: bool = False,
        suppress_validation: bool = False,
        github_token: str | None = None,
    ) -> None:
        _using_default = catalog_root is None

        if catalog_root is None:
            self.catalog_root: Path = _DEFAULT_CATALOG_ROOT
            self._fs = fsspec.filesystem("file")
        elif isinstance(catalog_root, Path):
            expanded = catalog_root.expanduser()
            self.catalog_root = (
                expanded.resolve() if not expanded.is_absolute() else expanded
            )
            self._fs = fsspec.filesystem("file")
        elif isinstance(catalog_root, str):
            if catalog_root.strip().lower() == "local":
                self.catalog_root = _DEFAULT_CATALOG_ROOT
                self._fs = fsspec.filesystem("file")
            elif _is_github_catalog_url(catalog_root):
                import os

                org_name, repo_name, branch, repo_path = _parse_github_catalog_url(
                    catalog_root
                )
                token = github_token or os.environ.get("GITHUB_TOKEN")
                gh_kwargs: dict[str, Any] = dict(
                    org=org_name, repo=repo_name, sha=branch
                )
                if token:
                    gh_kwargs["username"] = "x-access-token"
                    gh_kwargs["token"] = token
                self._fs = fsspec.filesystem("github", **gh_kwargs)
                self._github_ref = branch  # fsspec GithubFileSystem stores no ref attr
                self.catalog_root = repo_path
            elif catalog_root.startswith("http"):
                self._fs = fsspec.filesystem("http")
                self.catalog_root = Path(catalog_root)
            else:
                self.catalog_root = Path(catalog_root).expanduser().resolve()
                self._fs = fsspec.filesystem("file")
        else:
            raise ValueError(
                f"catalog_root must be a Path, str, or None; got {type(catalog_root)}"
            )

        # Merge catalog skeleton from a source catalog before scanning.
        if initialize_catalog_from is not None:
            self._initialize_from(
                initialize_catalog_from, clobber=initialize_catalog_clobber
            )

        # Internal registries
        self._models: dict[str, Path] = {}
        self._machines: dict[str, Path] = {}
        self._domains: dict[str, Path] = {}  # domain_name -> DomainSpec/<name>/ dir
        self._forcing: dict[str, Path] = {}  # forcing_name -> ForcingSpec/<name>/ dir
        self._output: dict[str, Path] = {}  # output_name -> OutputSpec/<name>/ dir
        self._roms_marbl_blueprints: dict[
            str, Path
        ] = {}  # roms_marbl_blueprint_name -> blueprints/<machine>/<name>/ dir

        self._scan_machines()
        self._scan_models()
        self._scan_roms_marbl_blueprints()
        self._scan_domains()
        self._scan_forcing()
        self._scan_output()

        # Validate non-default catalogs that weren't just initialized.
        if (
            not _using_default
            and initialize_catalog_from is None
            and not suppress_validation
        ):
            self._validate_catalog()

    # ------------------------------------------------------------------
    # Filesystem helpers (local vs. remote)
    # ------------------------------------------------------------------

    @property
    def _is_local(self) -> bool:
        return getattr(self._fs, "protocol", "file") in (
            "file",
            "local",
            ("file", "local"),
        )

    def _fs_exists(self, path: Path) -> bool:
        return path.exists() if self._is_local else self._fs.exists(str(path))

    def _fs_isdir(self, path: Path) -> bool:
        return path.is_dir() if self._is_local else self._fs.isdir(str(path))

    def _fs_glob(self, directory: Path, pattern: str) -> list[Path]:
        if self._is_local:
            return list(directory.glob(pattern))
        return [Path(f) for f in self._fs.glob(str(directory / pattern))]

    def _fs_glob_dual(self, directory: Path, stem_pattern: str) -> list[Path]:
        """Glob *stem_pattern* (no extension, e.g. ``"*"`` or ``"*/model"``) matching
        both ``.yaml`` and ``.yml`` files. New catalog entries are always written as
        ``.yaml``; this keeps existing on-disk ``.yml`` catalogs/blueprints (outside
        the repo, e.g. a user's scratch catalog) discoverable without regeneration.
        ``.yaml`` wins if both exist for the same stem.
        """
        yml_matches = {
            p.parent / p.stem: p
            for p in self._fs_glob(directory, f"{stem_pattern}.yml")
        }
        yaml_matches = {
            p.parent / p.stem: p
            for p in self._fs_glob(directory, f"{stem_pattern}.yaml")
        }
        return sorted({**yml_matches, **yaml_matches}.values())

    def _fs_iterdir(self, path: Path) -> list[Path]:
        if self._is_local:
            return list(path.iterdir())
        return [Path(f) for f in self._fs.ls(str(path), detail=False)]

    def _fs_iterdir_dirs(self, path: Path) -> list[Path]:
        """Return only subdirectories. For remote fs, uses a single detail=True ls call."""
        if self._is_local:
            return [p for p in path.iterdir() if p.is_dir()]
        entries = self._fs.ls(str(path), detail=True)
        return [Path(e["name"]) for e in entries if e.get("type") == "directory"]

    def _fs_open(self, path: Path):
        if self._is_local:
            return path.open("r")
        return self._fs.open(str(path), "r")

    def _resolve_stem_file(self, directory: Path, stem: str) -> Path:
        """Resolve ``directory/{stem}.yaml`` or ``directory/{stem}.yml``, preferring
        ``.yaml``. Falls back to the ``.yaml`` path (even if absent) so callers get a
        sensible path to report in a "not found" error or to write a fresh file to.
        """
        yaml_path = directory / f"{stem}.yaml"
        if self._fs_exists(yaml_path):
            return yaml_path
        yml_path = directory / f"{stem}.yml"
        if self._fs_exists(yml_path):
            return yml_path
        return yaml_path

    def _to_raw_github_url(self, path: Path) -> str:
        """Return the raw.githubusercontent.com URL for a path in the GitHub repo."""
        org = self._fs.org
        repo = self._fs.repo
        ref = getattr(self, "_github_ref", None) or "HEAD"
        return f"https://raw.githubusercontent.com/{org}/{repo}/{ref}/{path}"

    # ------------------------------------------------------------------
    # Scanning
    # ------------------------------------------------------------------

    def _scan_machines(self) -> None:
        """Scan Machines/ for per-machine YAML files."""
        self._machines = {}
        machine_dir = self.catalog_root / "Machines"
        try:
            for f in sorted(self._fs_glob_dual(machine_dir, "*")):
                self._machines[f.stem] = f
        except Exception as exc:
            logger.warning("Failed to scan machines: %s", exc)

    def _scan_models(self) -> None:
        """Scan ModelSpec/ for per-model directories containing model.yaml (or
        legacy model.yml).
        """
        self._models = {}
        model_dir_root = self.catalog_root / "ModelSpec"
        try:
            for f in sorted(self._fs_glob_dual(model_dir_root, "*/model")):
                model_dir = f.parent
                self._models[model_dir.name] = model_dir  # store dir, not file
        except Exception as exc:
            logger.warning("Failed to scan models: %s", exc)

    def _scan_roms_marbl_blueprints(self) -> None:
        """Scan blueprints/ (and Blueprints/) for blueprint directories.

        Expected layout: blueprints/<machine>/<name>/B_*.yaml
        Uses _fs_iterdir_dirs to retrieve directory type from a single ls call,
        avoiding a separate isdir API call per entry.
        """
        self._roms_marbl_blueprints = {}
        for subdir_name in ("blueprints", "Blueprints"):
            bp_root = self.catalog_root / subdir_name
            if not self._fs_exists(bp_root):
                continue
            try:
                for machine_dir in sorted(self._fs_iterdir_dirs(bp_root)):
                    for bp_dir in sorted(self._fs_iterdir_dirs(machine_dir)):
                        self._roms_marbl_blueprints[bp_dir.name] = bp_dir
            except Exception as exc:
                logger.warning(
                    "Failed to scan roms_marbl_blueprints under %s: %s",
                    subdir_name,
                    exc,
                )

    def _scan_domains(self) -> None:
        """Scan DomainSpec/ for domain directories containing Domain.yaml (or
        legacy Domain.yml).

        Uses a single dual-extension glob for */Domain.{yaml,yml} to find all
        domains in one API call (per extension).
        """
        self._domains = {}
        domain_spec_dir = self.catalog_root / "DomainSpec"
        try:
            for domain_yaml in sorted(self._fs_glob_dual(domain_spec_dir, "*/Domain")):
                domain_dir = domain_yaml.parent
                self._domains[domain_dir.name] = domain_dir
        except Exception as exc:
            logger.warning("Failed to scan domains: %s", exc)

    def _scan_forcing(self) -> None:
        """Scan ForcingSpec/ for directories containing Forcing.yaml (or legacy
        Forcing.yml).
        """
        self._forcing = {}
        forcing_spec_dir = self.catalog_root / "ForcingSpec"
        try:
            for forcing_yaml in sorted(
                self._fs_glob_dual(forcing_spec_dir, "*/Forcing")
            ):
                forcing_dir = forcing_yaml.parent
                self._forcing[forcing_dir.name] = forcing_dir
        except Exception as exc:
            logger.warning("Failed to scan forcing: %s", exc)

    def _scan_output(self) -> None:
        """Scan OutputSpec/ for directories containing Output.yaml (or legacy
        Output.yml).
        """
        self._output = {}
        output_spec_dir = self.catalog_root / "OutputSpec"
        try:
            for output_yaml in sorted(self._fs_glob_dual(output_spec_dir, "*/Output")):
                output_dir = output_yaml.parent
                self._output[output_dir.name] = output_dir
        except Exception as exc:
            logger.warning("Failed to scan output: %s", exc)

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def _initialize_from(self, source: str | Path, clobber: bool = False) -> None:
        """Merge Machines/, ModelSpec/, and DomainSpec/ from a source catalog into self.catalog_root.

        Files that do not already exist at the destination are always copied.
        Files that exist at both source and destination are "conflicts":
        - ``clobber=True``: overwrite conflicts silently.
        - ``clobber=False``: raise ``ValueError`` listing all conflicts and
          suggest re-running with ``initialize_catalog_clobber=True``.

        Parameters
        ----------
        source : str or Path
            Inner catalog directory to merge from, or ``'local'`` to use the
            package-bundled catalog (``cstar_forge/catalog``).
        clobber : bool
            Whether to overwrite conflicting destination files.
        """
        if isinstance(source, str) and source.strip().lower() == "local":
            src_root = _DEFAULT_CATALOG_ROOT
        else:
            src_root = Path(source).expanduser().resolve()

        self.catalog_root.mkdir(parents=True, exist_ok=True)

        # Collect all source files and map each to its destination path.
        pairs: list[tuple[Path, Path]] = []
        for subdir in (
            "Machines",
            "ModelSpec",
            "DomainSpec",
            "ForcingSpec",
            "OutputSpec",
        ):
            src_sub = src_root / subdir
            if not src_sub.exists():
                continue
            for src_file in sorted(src_sub.rglob("*")):
                if src_file.is_file():
                    rel = src_file.relative_to(src_root)
                    pairs.append((src_file, self.catalog_root / rel))

        # Detect conflicts before writing anything.
        if not clobber:
            conflicts = [dst for _, dst in pairs if dst.exists()]
            if conflicts:
                conflict_list = "\n".join(f"  {c}" for c in conflicts)
                raise ValueError(
                    f"Catalog merge conflict: the following files already exist "
                    f"in '{self.catalog_root}' and would be overwritten:\n"
                    f"{conflict_list}\n\n"
                    f"To overwrite conflicting files, use "
                    f"initialize_catalog_clobber=True."
                )

        # Perform the merge.
        for src_file, dst_file in pairs:
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dst_file)

    def _validate_catalog(self) -> None:
        """Raise ValueError if Machines/ or ModelSpec/ are missing or empty.

        Only called for non-default catalog roots that were not just initialized.
        Uses the already-populated _machines/_models dicts so remote filesystems work.
        """
        if not self._machines or not self._models:
            missing = []
            if not self._machines:
                missing.append("Machines/ (with at least one .yaml)")
            if not self._models:
                missing.append("ModelSpec/ (with at least one <name>/model.yaml)")
            raise ValueError(
                f"No valid catalog found at '{self.catalog_root}'. "
                f"Missing: {', '.join(missing)}.\n"
                f"To initialize from the built-in package catalog run:\n"
                f"    DomainCatalog(catalog_root=..., initialize_catalog_from='local')\n"
                f"Or pass initialize_catalog_from=<inner-catalog-path> to copy from "
                f"another existing catalog."
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def model_names(self) -> list[str]:
        """Return a sorted list of available model names."""
        return sorted(self._models.keys())

    @property
    def machine_names(self) -> list[str]:
        """Return a sorted list of available machine names."""
        return sorted(self._machines.keys())

    @property
    def domain_names(self) -> list[str]:
        """Return a sorted list of available domain names."""
        return sorted(self._domains.keys())

    @property
    def forcing_names(self) -> list[str]:
        """Return a sorted list of available forcing-spec names."""
        return sorted(self._forcing.keys())

    @property
    def output_names(self) -> list[str]:
        """Return a sorted list of available output-spec names."""
        return sorted(self._output.keys())

    @property
    def roms_marbl_blueprint_names(self) -> list[str]:
        """Return a sorted list of available blueprint names."""
        return sorted(self._roms_marbl_blueprints.keys())

    @property
    def roms_marbl_blueprints_dir(self) -> Path:
        """Path to the blueprints directory (catalog_root/blueprints)."""
        return self.catalog_root / "blueprints"

    @property
    def workplans_dir(self) -> Path:
        """Path to the workplans directory (catalog_root/workplans)."""
        return self.catalog_root / "workplans"

    # ------------------------------------------------------------------
    # Path helpers (used by ForgeExecutor)
    # ------------------------------------------------------------------
    def tree(self) -> None:
        """Print the tree of the catalog."""
        try:
            print(self._fs.tree(str(self.catalog_root)))
        except (AttributeError, NotImplementedError):
            # fsspec GitHub FS has no tree(); fall back to find()
            entries = self._fs.find(str(self.catalog_root))
            print("\n".join(entries))

    def roms_marbl_blueprint_dir_for(
        self, machine_id: str, roms_marbl_blueprint_name: str
    ) -> Path:
        """Return the blueprint directory for a given machine and blueprint name."""
        return self.roms_marbl_blueprints_dir / machine_id / roms_marbl_blueprint_name

    def build_dir_for(self, machine_id: str, roms_marbl_blueprint_name: str) -> Path:
        """Return the Build/ directory inside the blueprint folder.

        Build artifacts live at ``blueprints/<machine_id>/<roms_marbl_blueprint_name>/Build/``,
        co-located with the blueprint YAML files.
        """
        return (
            self.roms_marbl_blueprints_dir
            / machine_id
            / roms_marbl_blueprint_name
            / "Build"
        )

    # ------------------------------------------------------------------
    # Path accessors (raise KeyError if not found)
    # ------------------------------------------------------------------

    def model_path(self, model_name: str) -> Path:
        """Return the path to the model.yaml (or legacy model.yml) file for a
        named model.
        """
        if model_name not in self._models:
            raise KeyError(
                f"Model '{model_name}' not found in catalog at {self.catalog_root}. "
                f"Available models: {self.model_names}"
            )
        return self._resolve_stem_file(self._models[model_name], "model")

    def model_dir(self, model_name: str) -> Path:
        """Return the directory containing model.yaml for a named model."""
        if model_name not in self._models:
            raise KeyError(
                f"Model '{model_name}' not found in catalog at {self.catalog_root}. "
                f"Available models: {self.model_names}"
            )
        return self._models[model_name]

    def machine_path(self, machine_name: str) -> Path:
        """Return the path to the YAML file for a named machine."""
        if machine_name not in self._machines:
            raise KeyError(
                f"Machine '{machine_name}' not found in catalog at {self.catalog_root}. "
                f"Available machines: {self.machine_names}"
            )
        return self._machines[machine_name]

    def domain_path(self, domain_name: str) -> Path:
        """Return the directory path for a named domain (contains Domain.yaml and Assets/)."""
        if domain_name not in self._domains:
            raise KeyError(
                f"Domain '{domain_name}' not found in catalog at {self.catalog_root}. "
                f"Available domains: {self.domain_names}"
            )
        return self._domains[domain_name]

    def roms_marbl_blueprint_path(self, roms_marbl_blueprint_name: str) -> Path:
        """Return the directory path for a named blueprint."""
        if roms_marbl_blueprint_name not in self._roms_marbl_blueprints:
            raise KeyError(
                f"Blueprint '{roms_marbl_blueprint_name}' not found in catalog at {self.catalog_root}. "
                f"Available blueprints: {self.roms_marbl_blueprint_names}"
            )
        return self._roms_marbl_blueprints[roms_marbl_blueprint_name]

    # ------------------------------------------------------------------
    # Data accessors (return raw dicts)
    # ------------------------------------------------------------------

    def machine_data(self, machine_name: str) -> dict:
        """Return the raw YAML data dict for a named machine."""
        path = self.machine_path(machine_name)
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    def model_data(self, model_name: str) -> dict:
        """Return the raw YAML data dict for a named model."""
        path = self.model_path(model_name)
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    def domain_data(self, domain_name: str) -> dict:
        """Return the raw YAML data dict for a named domain (reads Domain.yaml)."""
        path = self._resolve_stem_file(self.domain_path(domain_name), "Domain")
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    def forcing_data(self, forcing_name: str) -> dict:
        """Return the raw YAML data dict for a named forcing spec (reads Forcing.yaml)."""
        if forcing_name not in self._forcing:
            raise KeyError(
                f"ForcingSpec '{forcing_name}' not found in catalog at {self.catalog_root}. "
                f"Available: {self.forcing_names}"
            )
        path = self._resolve_stem_file(self._forcing[forcing_name], "Forcing")
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    def output_data(self, output_name: str) -> dict:
        """Return the raw YAML data dict for a named output spec (reads Output.yaml)."""
        if output_name not in self._output:
            raise KeyError(
                f"OutputSpec '{output_name}' not found in catalog at {self.catalog_root}. "
                f"Available: {self.output_names}"
            )
        path = self._resolve_stem_file(self._output[output_name], "Output")
        with self._fs_open(path) as f:
            data = yaml.safe_load(f) or {}
        data.pop("description", None)
        return data

    # ------------------------------------------------------------------
    # Sketch-compatible accessor methods (name or index)
    # ------------------------------------------------------------------

    def domain(self, domain_id: str | int) -> dict:
        """Return a domain spec dict by name (str) or index (int).

        Parameters
        ----------
        domain_id : str or int
            Domain name or zero-based index into domain_names.

        Returns
        -------
        dict
            Parsed Domain.yaml content.
        """
        if isinstance(domain_id, str):
            return self.domain_data(domain_id)
        elif isinstance(domain_id, int):
            return self.domain_data(self.domain_names[domain_id])
        else:
            raise ValueError(f"domain_id must be str or int, got {type(domain_id)}")

    def model(self, model_id: str | int) -> dict:
        """Return a model spec dict by name (str) or index (int).

        Parameters
        ----------
        model_id : str or int
            Model name or zero-based index into model_names.

        Returns
        -------
        dict
            Parsed model YAML content.
        """
        if isinstance(model_id, str):
            return self.model_data(model_id)
        elif isinstance(model_id, int):
            return self.model_data(self.model_names[model_id])
        else:
            raise ValueError(f"model_id must be str or int, got {type(model_id)}")

    def roms_marbl_blueprint(self, roms_marbl_blueprint_id: str | int) -> Path:
        """Return a blueprint directory Path by name (str) or index (int).

        Parameters
        ----------
        roms_marbl_blueprint_id : str or int
            Blueprint name or zero-based index into roms_marbl_blueprint_names.

        Returns
        -------
        Path
            Path to the blueprint's directory (contains B_*.yaml).
        """
        if isinstance(roms_marbl_blueprint_id, str):
            return self.roms_marbl_blueprint_path(roms_marbl_blueprint_id)
        elif isinstance(roms_marbl_blueprint_id, int):
            return self._roms_marbl_blueprints[
                self.roms_marbl_blueprint_names[roms_marbl_blueprint_id]
            ]
        else:
            raise ValueError(
                f"roms_marbl_blueprint_id must be str or int, got {type(roms_marbl_blueprint_id)}"
            )

    # ------------------------------------------------------------------
    # Model/spec loading
    # ------------------------------------------------------------------

    def load_model_spec(self, model_name: str) -> Any:
        """Load and return a parsed ModelSpec for the named model.

        Parameters
        ----------
        model_name : str
            Name of the model (must exist in ModelSpec/).

        Returns
        -------
        ModelSpec
            Parsed Pydantic ModelSpec instance.
        """
        from cstar_forge.models import load_models_yaml

        path = self.model_path(model_name)
        return load_models_yaml(path, model_name)

    # ------------------------------------------------------------------
    # Registration / mutation methods
    # ------------------------------------------------------------------

    def register_model(self, model_dir: Path | str) -> None:
        """Register a new model by copying its directory (containing model.yaml) into ModelSpec/ and rescanning.

        Parameters
        ----------
        model_dir : str or Path
            Path to the model directory (which must contain model.yaml, or
            legacy model.yml). The directory name is used as the model name.
        """
        src = Path(model_dir).expanduser().resolve()
        if not (src / "model.yaml").exists() and not (src / "model.yml").exists():
            raise ValueError(f"model_dir must contain a model.yaml file: {src}")
        dest_dir = self.catalog_root / "ModelSpec" / src.name
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        shutil.copytree(src, dest_dir)
        self._scan_models()

    def register_domain(self, builder: Any) -> None:
        """Create a new DomainSpec entry from a ForgeExecutor instance.

        Writes a Domain.yaml file and creates an empty Assets/ directory under
        DomainSpec/<grid_name>/. The domain name is taken from builder.grid_name.

        Parameters
        ----------
        builder : ForgeExecutor
            A configured builder whose grid_name, grid_kwargs, open_boundaries,
            and partitioning will be recorded. ``ForgeExecutor`` no longer tracks
            a bare ``model_name`` (post naming-refactor it only carries the
            combined blueprint ``name``), so the written ``Domain.yaml`` omits it;
            ``register_domain_from_dict`` accepts an explicit ``model_name`` when
            the caller has one (e.g. the wizard's composition).
        """
        domain_name = builder.grid_name
        domain_dir = self.catalog_root / "DomainSpec" / domain_name
        domain_dir.mkdir(parents=True, exist_ok=True)
        (domain_dir / "Assets").mkdir(exist_ok=True)

        domain_data: dict[str, Any] = {
            "description": builder.description,
            "grid_name": builder.grid_name,
            "start_time": builder.start_date.isoformat(),
            "end_time": builder.end_date.isoformat(),
            "grid_kwargs": builder.grid_kwargs,
            "open_boundaries": builder.open_boundaries.model_dump(),
            "partitioning": {
                "n_procs_x": builder.partitioning.n_procs_x,
                "n_procs_y": builder.partitioning.n_procs_y,
            },
        }
        if builder.grid_kwargs_parent:
            domain_data["grid_kwargs_parent"] = builder.grid_kwargs_parent
        if builder.grid_kwargs_child:
            domain_data["grid_kwargs_child"] = builder.grid_kwargs_child
        # v_sponge: best-effort -- this executor-driven path predates the
        # wizard's touched/derived distinction (see forge_blueprint_wizard's
        # _domain_piece_data, the authoritative save path), so it always
        # records whatever the resolver computed rather than tracking whether
        # it was a user override.
        _v_sponge = (
            (builder.resolved_settings or {}).get("v_sponge", {}).get("v_sponge")
        )
        if _v_sponge is not None:
            domain_data["v_sponge"] = _v_sponge
        # dt: same best-effort reasoning as v_sponge above.
        _dt = (builder.resolved_settings or {}).get("time_stepping", {}).get("dt")
        if _dt is not None:
            domain_data["dt"] = _dt

        with (domain_dir / "Domain.yaml").open("w") as f:
            yaml.safe_dump(
                domain_data,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )

        self._scan_domains()

    def register_domain_from_dict(self, name: str, domain_data: dict[str, Any]) -> None:
        """Create a new DomainSpec entry from a plain dict (the wizard's save path).

        Writes ``domain_data`` verbatim as ``Domain.yaml`` under
        ``DomainSpec/<name>/`` (+ an empty ``Assets/`` dir), then rescans.
        Refuses to overwrite an existing entry -- catalog entries are named,
        shared resources; pick a different name or delete the old one first.
        """
        domain_dir = self.catalog_root / "DomainSpec" / name
        if domain_dir.exists():
            raise FileExistsError(f"DomainSpec '{name}' already exists at {domain_dir}")
        domain_dir.mkdir(parents=True)
        (domain_dir / "Assets").mkdir(exist_ok=True)
        with (domain_dir / "Domain.yaml").open("w") as f:
            yaml.safe_dump(
                domain_data,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        self._scan_domains()

    def register_output(
        self, name: str, output_settings: dict[str, Any], description: str = ""
    ) -> None:
        """Create a new OutputSpec entry (``OutputSpec/<name>/Output.yaml``).

        ``output_settings`` is the output-owned subset of ``model_settings`` (see
        ``forge_blueprint_resolve.extract_output_settings``). Refuses to overwrite
        an existing entry.
        """
        output_dir = self.catalog_root / "OutputSpec" / name
        if output_dir.exists():
            raise FileExistsError(f"OutputSpec '{name}' already exists at {output_dir}")
        output_dir.mkdir(parents=True)
        data = {"description": description, **output_settings}
        with (output_dir / "Output.yaml").open("w") as f:
            yaml.safe_dump(
                data, f, default_flow_style=False, sort_keys=False, allow_unicode=True
            )
        self._scan_output()

    def register_forcing(
        self,
        name: str,
        forcing_inputs: dict[str, Any],
        cdr_forcing: dict[str, Any] | None = None,
        description: str = "",
    ) -> None:
        """Create a new ForcingSpec entry (``ForcingSpec/<name>/Forcing.yaml``).

        ``forcing_inputs`` is the ``{initial_conditions, forcing}`` shape returned
        by the wizard's forcing editor ``gather()``. ``cdr_forcing``, if given, is
        embedded under an optional ``cdr_forcing:`` block (not part of the
        original ForcingSpec schema, added so a domain using CDR forcing can still
        save/reload its ForcingSpec). Refuses to overwrite an existing entry.
        """
        forcing_dir = self.catalog_root / "ForcingSpec" / name
        if forcing_dir.exists():
            raise FileExistsError(
                f"ForcingSpec '{name}' already exists at {forcing_dir}"
            )
        forcing_dir.mkdir(parents=True)
        data = {"description": description, **forcing_inputs}
        if cdr_forcing:
            data["cdr_forcing"] = cdr_forcing
        with (forcing_dir / "Forcing.yaml").open("w") as f:
            yaml.safe_dump(
                data, f, default_flow_style=False, sort_keys=False, allow_unicode=True
            )
        self._scan_forcing()

    def register_model_from_settings(
        self,
        name: str,
        model_settings: dict[str, Any],
        base_model_dir: Path | str,
        description: str = "",
        *,
        bgc_mode: str | None = None,
        use_pio: bool | None = None,
        roms_ref: str | None = None,
    ) -> None:
        """Create a new ModelSpec entry (``ModelSpec/<name>/model.yaml``).

        Clones ``code``/``templates_commit`` verbatim from ``base_model_dir``'s
        ``model.yaml`` and swaps in ``model_settings`` (the model-owned subset --
        see the wizard's ``_model_owned_settings``). ``bgc_mode``/``use_pio``/
        ``roms_ref`` clone the base value when left ``None``, or take the given
        override when not -- these are the live per-run toggles the wizard's
        widgets carry (``use_pio`` and ``code.marbl``/``bgc_mode`` don't survive
        in ``model_settings`` since they're resolver-derived leaves stripped by
        ``_model_owned_settings``, and ``code.roms.commit`` isn't part of
        ``model_settings`` at all). Refuses to overwrite an existing entry.
        """
        model_dir = self.catalog_root / "ModelSpec" / name
        if model_dir.exists():
            raise FileExistsError(f"ModelSpec '{name}' already exists at {model_dir}")
        base_model_path = self._resolve_stem_file(Path(base_model_dir), "model")
        base = yaml.safe_load(base_model_path.read_text()) or {}
        code = dict(base.get("code") or {})
        if roms_ref and code.get("roms"):
            # Mirror forge_blueprint_resolve.py's roms_ref handling: commit/branch/
            # tag are all valid checkout targets, so store the override in `commit`
            # and drop `branch` (C-Star requires exactly one of the two). Keep every
            # other repo (pio/marbl) verbatim -- even if currently unused -- so the
            # saved spec stays toggleable for use_pio/bgc_mode later.
            roms = dict(code["roms"])
            roms["commit"] = roms_ref
            roms.pop("branch", None)
            code["roms"] = roms
        data = {
            "description": description or base.get("description", ""),
            "bgc_mode": (
                bgc_mode if bgc_mode is not None else base.get("bgc_mode", "marbl")
            ),
            "use_pio": (
                bool(use_pio) if use_pio is not None else base.get("use_pio", False)
            ),
            "code": code,
            "model_settings": model_settings,
        }
        model_dir.mkdir(parents=True)
        with (model_dir / "model.yaml").open("w") as f:
            yaml.safe_dump(
                data, f, default_flow_style=False, sort_keys=False, allow_unicode=True
            )
        self._scan_models()

    def add_asset_to_domain(
        self,
        domain_name: str,
        asset_name: str,
        asset_file: Any,
        asset_metadata: dict,
    ) -> None:
        """Add an asset file to a domain's Assets/ folder and record it in Domain.yaml.

        Parameters
        ----------
        domain_name : str
            Name of the existing domain.
        asset_name : str
            Filename to store the asset under in Assets/.
        asset_file : file-like or path-like
            Source of the asset: a file-like object (must have .read()) or a path.
        asset_metadata : dict
            Arbitrary key/value metadata recorded alongside the asset in Domain.yaml.
        """
        domain_dir = self.domain_path(domain_name)
        assets_dir = domain_dir / "Assets"
        assets_dir.mkdir(exist_ok=True)

        dest = assets_dir / asset_name
        if hasattr(asset_file, "read"):
            dest.write_bytes(asset_file.read())
        else:
            shutil.copy2(Path(asset_file), dest)

        domain_yml = self._resolve_stem_file(domain_dir, "Domain")
        with domain_yml.open() as f:
            data = yaml.safe_load(f) or {}
        data.setdefault("assets", {})[asset_name] = {
            "path": f"Assets/{asset_name}",
            **asset_metadata,
        }
        with domain_yml.open("w") as f:
            yaml.safe_dump(
                data,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )

    def copy_domain(self, domain_name: str, catalog: DomainCatalog) -> None:
        """Copy a domain spec directory (Domain.yaml + Assets/) to another DomainCatalog.

        Parameters
        ----------
        domain_name : str
            Name of the domain to copy from this catalog.
        catalog : DomainCatalog
            Target catalog to copy the domain into.
        """
        src = self.domain_path(domain_name)
        dest = catalog.catalog_root / "DomainSpec" / domain_name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest)
        catalog._scan_domains()

    def copy_model(self, model_name: str, catalog: DomainCatalog) -> None:
        """Copy a model directory (model.yaml) to another DomainCatalog.

        Parameters
        ----------
        model_name : str
            Name of the model to copy from this catalog.
        catalog : DomainCatalog
            Target catalog to copy the model into.
        """
        src = self.model_dir(model_name)
        dest = catalog.catalog_root / "ModelSpec" / model_name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest)
        catalog._scan_models()

    # ------------------------------------------------------------------
    # Blueprint DataFrame methods (merged from BlueprintCatalog)
    # ------------------------------------------------------------------

    def _find_roms_marbl_blueprint_files(self) -> list[Path]:
        """Find B_*.yaml (or legacy B_*.yml) files across all known blueprint
        directories.
        """
        files: list[Path] = []
        for bp_dir in self._roms_marbl_blueprints.values():
            files.extend(
                f
                for f in self._fs_glob_dual(bp_dir, "B_*")
                if ".ipynb_checkpoints" not in str(f)
            )
        return sorted(set(files))

    def _load_roms_marbl_blueprint_yaml(
        self, roms_marbl_blueprint_path: Path
    ) -> dict[str, Any]:
        """Load a single B_*.yaml (or legacy B_*.yml) file."""
        if not self._fs_exists(roms_marbl_blueprint_path):
            raise FileNotFoundError(
                f"Blueprint file not found: {roms_marbl_blueprint_path}"
            )
        with self._fs_open(roms_marbl_blueprint_path) as f:
            return yaml.safe_load(f) or {}

    def _load_grid_kwargs(self, grid_yaml_path: Path) -> dict[str, Any]:
        """Load Grid kwargs from a two-document _grid.yaml (or legacy _grid.yml) file."""
        if not self._fs_exists(grid_yaml_path):
            raise FileNotFoundError(f"Grid YAML file not found: {grid_yaml_path}")
        with self._fs_open(grid_yaml_path) as f:
            docs = list(yaml.safe_load_all(f))
        if len(docs) != 2:
            raise ValueError(
                f"Expected 2 documents in {grid_yaml_path}, found {len(docs)}"
            )
        grid_data = docs[1]
        if "Grid" not in grid_data:
            raise KeyError(f"Grid section not found in {grid_yaml_path}")
        return grid_data["Grid"]

    def _extract_model_and_grid_name(
        self, roms_marbl_blueprint_name: str
    ) -> tuple[str | None, str | None]:
        """Extract (model_name, grid_name) from a blueprint name.

        Strips a trailing _NNprocs suffix, then tries to match against known
        model names (longest first). Falls back to splitting on the last underscore.
        """
        if not roms_marbl_blueprint_name:
            return None, None
        name = re.sub(r"_\d+procs$", "", roms_marbl_blueprint_name)
        for model_name in sorted(self.model_names, key=len, reverse=True):
            if name.startswith(model_name + "_"):
                return model_name, name[len(model_name) + 1 :]
        parts = name.rsplit("_", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None

    def roms_marbl_blueprint_df(self) -> pd.DataFrame:
        """Load all blueprints and return a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: model_name, grid_name, blueprint_name,
            description, start_time, end_time, blueprint_path, grid_yaml_path.
        """
        import pandas as pd

        records = []
        for bp_file in self._find_roms_marbl_blueprint_files():
            try:
                bp = self._load_roms_marbl_blueprint_yaml(bp_file)
                roms_marbl_blueprint_name = bp.get("name")
                if not roms_marbl_blueprint_name:
                    logger.warning("Skipping %s: missing 'name' field", bp_file)
                    continue
                model_name, grid_name = self._extract_model_and_grid_name(
                    roms_marbl_blueprint_name
                )
                if not model_name or not grid_name:
                    logger.warning(
                        "Skipping %s: could not parse model/grid from '%s'",
                        bp_file,
                        roms_marbl_blueprint_name,
                    )
                    continue
                is_github = hasattr(self._fs, "org")
                grid_yaml = self._resolve_stem_file(bp_file.parent, "_grid")
                grid_yaml_exists = self._fs_exists(grid_yaml)
                if grid_yaml_exists and is_github:
                    grid_yaml_result: Path | str | None = self._to_raw_github_url(
                        grid_yaml
                    )
                else:
                    grid_yaml_result = grid_yaml if grid_yaml_exists else None
                roms_marbl_blueprint_path_result: Path | str = (
                    self._to_raw_github_url(bp_file) if is_github else bp_file
                )
                records.append(
                    {
                        "model_name": model_name,
                        "grid_name": grid_name,
                        "blueprint_name": roms_marbl_blueprint_name,
                        "description": bp.get("description"),
                        "start_time": bp.get("valid_start_date"),
                        "end_time": bp.get("valid_end_date"),
                        "blueprint_path": roms_marbl_blueprint_path_result,
                        "grid_yaml_path": grid_yaml_result,
                    }
                )
            except Exception as e:
                logger.warning("Could not parse %s: %s", bp_file, e)
                continue

        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    def __repr__(self) -> str:
        return (
            f"DomainCatalog(catalog_root={self.catalog_root}, "
            f"models={self.model_names}, machines={self.machine_names}, "
            f"domains={self.domain_names})"
        )


# Package-default catalog instance (points to cstar_forge/catalog/)
default_catalog = DomainCatalog()
