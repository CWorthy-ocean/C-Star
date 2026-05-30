"""DomainCatalog: manages the catalog directory structure for C-Star forge."""

from __future__ import annotations

import re
import shutil
import fsspec
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


_DEFAULT_CATALOG_ROOT = Path(__file__).parent / "catalog"


def _is_github_catalog_url(catalog_root: str) -> bool:
    """Return True if *catalog_root* looks like a GitHub repository URL."""
    s = catalog_root.strip()
    return s.startswith(("https://github.com/", "http://github.com/")) or s.startswith(
        "git@github.com:"
    )


def _parse_github_catalog_url(url: str) -> Tuple[str, str, str, Path]:
    """Parse a GitHub URL into ``(org, repo, branch, path_within_repo)``."""
    raw = url.strip().rstrip("/")
    if raw.startswith("git@github.com:"):
        path = raw[len("git@github.com:"):]
    elif "github.com/" in raw:
        path = raw.split("github.com/", 1)[1]
    else:
        raise ValueError(f"Not a GitHub catalog URL: {url!r}")

    if path.endswith(".git"):
        path = path[: -len(".git")]

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
        │   ├── MacOS.yml
        │   ├── NERSC_perlmutter.yml
        │   └── RCAC_anvil.yml
        ├── ModelSpec/
        │   ├── cson_roms-marbl_v0.1/
        │   │   ├── model.yml
        │   │   └── templates/
        │   └── cson_roms-no-bgc_v0.1/
        │       ├── model.yml
        │       └── templates/
        ├── DomainSpec/
        │   ├── ccs-12km/
        │   │   ├── Domain.yml
        │   │   └── Assets/
        │   └── PAC_2fth_deg/
        │       ├── Domain.yml
        │       └── Assets/
        ├── Blueprints/  (alias: blueprints/)
        │   └── <machine>/<blueprint-name>/
        │       ├── B_*.yml
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
        catalog_root: Optional[Union[str, Path]] = None,
        initialize_catalog_from: Optional[Union[str, Path]] = None,
        initialize_catalog_clobber: bool = False,
        suppress_validation: bool = False,
        github_token: Optional[str] = None,
    ) -> None:
        _using_default = catalog_root is None

        if catalog_root is None:
            self.catalog_root: Path = _DEFAULT_CATALOG_ROOT
            self._fs = fsspec.filesystem("file")
        elif isinstance(catalog_root, Path):
            expanded = catalog_root.expanduser()
            self.catalog_root = expanded.resolve() if not expanded.is_absolute() else expanded
            self._fs = fsspec.filesystem("file")
        elif isinstance(catalog_root, str):
            if catalog_root.strip().lower() == "local":
                self.catalog_root = _DEFAULT_CATALOG_ROOT
                self._fs = fsspec.filesystem("file")
            elif _is_github_catalog_url(catalog_root):
                import os
                org_name, repo_name, branch, repo_path = _parse_github_catalog_url(catalog_root)
                token = github_token or os.environ.get("GITHUB_TOKEN")
                gh_kwargs: Dict[str, Any] = dict(org=org_name, repo=repo_name, sha=branch)
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
            self._initialize_from(initialize_catalog_from, clobber=initialize_catalog_clobber)

        # Internal registries
        self._models: Dict[str, Path] = {}
        self._machines: Dict[str, Path] = {}
        self._domains: Dict[str, Path] = {}     # domain_name -> DomainSpec/<name>/ dir
        self._blueprints: Dict[str, Path] = {}  # blueprint_name -> blueprints/<machine>/<name>/ dir

        self._scan_machines()
        self._scan_models()
        self._scan_blueprints()
        self._scan_domains()

        # Validate non-default catalogs that weren't just initialized.
        if not _using_default and initialize_catalog_from is None and not suppress_validation:
            self._validate_catalog()

    # ------------------------------------------------------------------
    # Filesystem helpers (local vs. remote)
    # ------------------------------------------------------------------

    @property
    def _is_local(self) -> bool:
        return getattr(self._fs, "protocol", "file") in ("file", "local", ("file", "local"))

    def _fs_exists(self, path: Path) -> bool:
        return path.exists() if self._is_local else self._fs.exists(str(path))

    def _fs_isdir(self, path: Path) -> bool:
        return path.is_dir() if self._is_local else self._fs.isdir(str(path))

    def _fs_glob(self, directory: Path, pattern: str) -> List[Path]:
        if self._is_local:
            return list(directory.glob(pattern))
        return [Path(f) for f in self._fs.glob(str(directory / pattern))]

    def _fs_iterdir(self, path: Path) -> List[Path]:
        if self._is_local:
            return list(path.iterdir())
        return [Path(f) for f in self._fs.ls(str(path), detail=False)]

    def _fs_iterdir_dirs(self, path: Path) -> List[Path]:
        """Return only subdirectories. For remote fs, uses a single detail=True ls call."""
        if self._is_local:
            return [p for p in path.iterdir() if p.is_dir()]
        entries = self._fs.ls(str(path), detail=True)
        return [Path(e["name"]) for e in entries if e.get("type") == "directory"]

    def _fs_open(self, path: Path):
        if self._is_local:
            return path.open("r")
        return self._fs.open(str(path), "r")

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
            for f in sorted(self._fs_glob(machine_dir, "*.yml")):
                self._machines[f.stem] = f
        except Exception:
            pass

    def _scan_models(self) -> None:
        """Scan ModelSpec/ for per-model directories containing model.yml."""
        self._models = {}
        model_dir_root = self.catalog_root / "ModelSpec"
        try:
            for f in sorted(self._fs_glob(model_dir_root, "*/model.yml")):
                model_dir = f.parent
                self._models[model_dir.name] = model_dir  # store dir, not file
        except Exception:
            pass

    def _scan_blueprints(self) -> None:
        """Scan blueprints/ (and Blueprints/) for blueprint directories.

        Expected layout: blueprints/<machine>/<name>/B_*.yml
        Uses _fs_iterdir_dirs to retrieve directory type from a single ls call,
        avoiding a separate isdir API call per entry.
        """
        self._blueprints = {}
        for subdir_name in ("blueprints", "Blueprints"):
            bp_root = self.catalog_root / subdir_name
            if not self._fs_exists(bp_root):
                continue
            try:
                for machine_dir in sorted(self._fs_iterdir_dirs(bp_root)):
                    for bp_dir in sorted(self._fs_iterdir_dirs(machine_dir)):
                        self._blueprints[bp_dir.name] = bp_dir
            except Exception:
                pass

    def _scan_domains(self) -> None:
        """Scan DomainSpec/ for domain directories containing Domain.yml.

        Uses a single glob for */Domain.yml to find all domains in one API call.
        """
        self._domains = {}
        domain_spec_dir = self.catalog_root / "DomainSpec"
        try:
            for domain_yml in sorted(self._fs_glob(domain_spec_dir, "*/Domain.yml")):
                domain_dir = domain_yml.parent
                self._domains[domain_dir.name] = domain_dir
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def _initialize_from(self, source: Union[str, Path], clobber: bool = False) -> None:
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
        pairs: List[Tuple[Path, Path]] = []
        for subdir in ("Machines", "ModelSpec", "DomainSpec"):
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
                missing.append("Machines/ (with at least one .yml)")
            if not self._models:
                missing.append("ModelSpec/ (with at least one <name>/model.yml)")
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
    def model_names(self) -> List[str]:
        """Return a sorted list of available model names."""
        return sorted(self._models.keys())

    @property
    def machine_names(self) -> List[str]:
        """Return a sorted list of available machine names."""
        return sorted(self._machines.keys())

    @property
    def domain_names(self) -> List[str]:
        """Return a sorted list of available domain names."""
        return sorted(self._domains.keys())

    @property
    def blueprint_names(self) -> List[str]:
        """Return a sorted list of available blueprint names."""
        return sorted(self._blueprints.keys())

    @property
    def blueprints_dir(self) -> Path:
        """Path to the blueprints directory (catalog_root/blueprints)."""
        return self.catalog_root / "blueprints"

    # ------------------------------------------------------------------
    # Path helpers (used by CstarSpecBuilder)
    # ------------------------------------------------------------------
    def tree(self) -> None:
        """Print the tree of the catalog."""
        try:
            print(self._fs.tree(str(self.catalog_root)))
        except (AttributeError, NotImplementedError):
            # fsspec GitHub FS has no tree(); fall back to find()
            entries = self._fs.find(str(self.catalog_root))
            print("\n".join(entries))

    def blueprint_dir_for(self, machine_id: str, blueprint_name: str) -> Path:
        """Return the blueprint directory for a given machine and blueprint name."""
        return self.blueprints_dir / machine_id / blueprint_name

    def build_dir_for(self, machine_id: str, blueprint_name: str) -> Path:
        """Return the Build/ directory inside the blueprint folder.

        Build artifacts live at ``blueprints/<machine_id>/<blueprint_name>/Build/``,
        co-located with the blueprint YAML files.
        """
        return self.blueprints_dir / machine_id / blueprint_name / "Build"

    # ------------------------------------------------------------------
    # Path accessors (raise KeyError if not found)
    # ------------------------------------------------------------------

    def model_path(self, model_name: str) -> Path:
        """Return the path to the model.yml file for a named model."""
        if model_name not in self._models:
            raise KeyError(
                f"Model '{model_name}' not found in catalog at {self.catalog_root}. "
                f"Available models: {self.model_names}"
            )
        return self._models[model_name] / "model.yml"

    def model_dir(self, model_name: str) -> Path:
        """Return the directory containing model.yml and templates/ for a named model."""
        if model_name not in self._models:
            raise KeyError(
                f"Model '{model_name}' not found in catalog at {self.catalog_root}. "
                f"Available models: {self.model_names}"
            )
        return self._models[model_name]

    def compile_time_template_dir(self, model_name: str) -> Path:
        """Return the compile-time template directory for a named model."""
        return self.model_dir(model_name) / "templates" / "compile-time"

    def run_time_template_dir(self, model_name: str) -> Path:
        """Return the run-time template directory for a named model."""
        return self.model_dir(model_name) / "templates" / "run-time"

    def compile_time_defaults_path(self, model_name: str) -> Path:
        """Return the path to compile-time-defaults.yml for a named model."""
        return self.model_dir(model_name) / "templates" / "compile-time-defaults.yml"

    def run_time_defaults_path(self, model_name: str) -> Path:
        """Return the path to run-time-defaults.yml for a named model."""
        return self.model_dir(model_name) / "templates" / "run-time-defaults.yml"

    def machine_path(self, machine_name: str) -> Path:
        """Return the path to the YAML file for a named machine."""
        if machine_name not in self._machines:
            raise KeyError(
                f"Machine '{machine_name}' not found in catalog at {self.catalog_root}. "
                f"Available machines: {self.machine_names}"
            )
        return self._machines[machine_name]

    def domain_path(self, domain_name: str) -> Path:
        """Return the directory path for a named domain (contains Domain.yml and Assets/)."""
        if domain_name not in self._domains:
            raise KeyError(
                f"Domain '{domain_name}' not found in catalog at {self.catalog_root}. "
                f"Available domains: {self.domain_names}"
            )
        return self._domains[domain_name]

    def blueprint_path(self, blueprint_name: str) -> Path:
        """Return the directory path for a named blueprint."""
        if blueprint_name not in self._blueprints:
            raise KeyError(
                f"Blueprint '{blueprint_name}' not found in catalog at {self.catalog_root}. "
                f"Available blueprints: {self.blueprint_names}"
            )
        return self._blueprints[blueprint_name]

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
        """Return the raw YAML data dict for a named domain (reads Domain.yml)."""
        path = self.domain_path(domain_name) / "Domain.yml"
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    # ------------------------------------------------------------------
    # Sketch-compatible accessor methods (name or index)
    # ------------------------------------------------------------------

    def domain(self, domain_id: Union[str, int]) -> dict:
        """Return a domain spec dict by name (str) or index (int).

        Parameters
        ----------
        domain_id : str or int
            Domain name or zero-based index into domain_names.

        Returns
        -------
        dict
            Parsed Domain.yml content.
        """
        if isinstance(domain_id, str):
            return self.domain_data(domain_id)
        elif isinstance(domain_id, int):
            return self.domain_data(self.domain_names[domain_id])
        else:
            raise ValueError(f"domain_id must be str or int, got {type(domain_id)}")

    def model(self, model_id: Union[str, int]) -> dict:
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

    def blueprint(self, blueprint_id: Union[str, int]) -> Path:
        """Return a blueprint directory Path by name (str) or index (int).

        Parameters
        ----------
        blueprint_id : str or int
            Blueprint name or zero-based index into blueprint_names.

        Returns
        -------
        Path
            Path to the blueprint's directory (contains B_*.yml stage files).
        """
        if isinstance(blueprint_id, str):
            return self.blueprint_path(blueprint_id)
        elif isinstance(blueprint_id, int):
            return self._blueprints[self.blueprint_names[blueprint_id]]
        else:
            raise ValueError(f"blueprint_id must be str or int, got {type(blueprint_id)}")

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
        from .models import load_models_yaml
        path = self.model_path(model_name)
        return load_models_yaml(path, model_name)

    def to_builder(
        self,
        domain_name: str,
        start_time: Optional[Any] = None,
        end_time: Optional[Any] = None,
        **overrides: Any,
    ) -> Any:
        """Return a CstarSpecBuilder initialised from the named domain.

        All fields come from the domain's ``Domain.yml``; ``start_time`` and
        ``end_time`` (which are often placeholder single-day values in the
        catalog) can be overridden here or via ``**overrides``.  For nested
        domains that reference another domain via ``_parent_grid_name`` or
        ``_child_grid_name``, the cross-references are resolved automatically
        using this catalog.

        Parameters
        ----------
        domain_name : str
            Name of the domain (must exist in ``DomainSpec/``).
        start_time : str or datetime, optional
            Simulation start time.  Overrides the value in ``Domain.yml``.
            Required if ``Domain.yml`` omits ``start_time``.
        end_time : str or datetime, optional
            Simulation end time.  Overrides the value in ``Domain.yml``.
            Required if ``Domain.yml`` omits ``end_time``.
        **overrides
            Any additional ``CstarSpecBuilder`` field values to override.

        Returns
        -------
        CstarSpecBuilder
        """
        from ._core import CstarSpecBuilder
        kw: Dict[str, Any] = {}
        if start_time is not None:
            kw["start_time"] = start_time
        if end_time is not None:
            kw["end_time"] = end_time
        kw.update(overrides)
        return CstarSpecBuilder.from_domain(self.domain_data(domain_name), catalog=self, **kw)

    # ------------------------------------------------------------------
    # Registration / mutation methods
    # ------------------------------------------------------------------

    def register_model(self, model_dir: Union[Path, str]) -> None:
        """Register a new model by copying its directory (containing model.yml) into ModelSpec/ and rescanning.

        Parameters
        ----------
        model_dir : str or Path
            Path to the model directory (which must contain model.yml).
            The directory name is used as the model name.
        """
        src = Path(model_dir).expanduser().resolve()
        if not (src / "model.yml").exists():
            raise ValueError(f"model_dir must contain a model.yml file: {src}")
        dest_dir = self.catalog_root / "ModelSpec" / src.name
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        shutil.copytree(src, dest_dir)
        self._scan_models()

    def register_domain(self, builder: Any) -> None:
        """Create a new DomainSpec entry from a CstarSpecBuilder instance.

        Writes a Domain.yml file and creates an empty Assets/ directory under
        DomainSpec/<grid_name>/. The domain name is taken from builder.grid_name.

        Parameters
        ----------
        builder : CstarSpecBuilder
            A configured builder whose grid_name, model_name, grid_kwargs,
            open_boundaries, and partitioning will be recorded.
        """
        domain_name = builder.grid_name
        domain_dir = self.catalog_root / "DomainSpec" / domain_name
        domain_dir.mkdir(parents=True, exist_ok=True)
        (domain_dir / "Assets").mkdir(exist_ok=True)

        domain_data: Dict[str, Any] = {
            "description": builder.description,
            "model_name": builder.model_name,
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

        with (domain_dir / "Domain.yml").open("w") as f:
            yaml.safe_dump(
                domain_data, f,
                default_flow_style=False, sort_keys=False, allow_unicode=True,
            )

        self._scan_domains()

    def add_asset_to_domain(
        self,
        domain_name: str,
        asset_name: str,
        asset_file: Any,
        asset_metadata: dict,
    ) -> None:
        """Add an asset file to a domain's Assets/ folder and record it in Domain.yml.

        Parameters
        ----------
        domain_name : str
            Name of the existing domain.
        asset_name : str
            Filename to store the asset under in Assets/.
        asset_file : file-like or path-like
            Source of the asset: a file-like object (must have .read()) or a path.
        asset_metadata : dict
            Arbitrary key/value metadata recorded alongside the asset in Domain.yml.
        """
        domain_dir = self.domain_path(domain_name)
        assets_dir = domain_dir / "Assets"
        assets_dir.mkdir(exist_ok=True)

        dest = assets_dir / asset_name
        if hasattr(asset_file, "read"):
            dest.write_bytes(asset_file.read())
        else:
            shutil.copy2(Path(asset_file), dest)

        domain_yml = domain_dir / "Domain.yml"
        with domain_yml.open() as f:
            data = yaml.safe_load(f) or {}
        data.setdefault("assets", {})[asset_name] = {
            "path": f"Assets/{asset_name}",
            **asset_metadata,
        }
        with domain_yml.open("w") as f:
            yaml.safe_dump(
                data, f,
                default_flow_style=False, sort_keys=False, allow_unicode=True,
            )

    def copy_domain(self, domain_name: str, catalog: "DomainCatalog") -> None:
        """Copy a domain spec directory (Domain.yml + Assets/) to another DomainCatalog.

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

    def copy_model(self, model_name: str, catalog: "DomainCatalog") -> None:
        """Copy a model directory (model.yml + templates/) to another DomainCatalog.

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

    def _find_blueprint_stage_files(self, stage: Optional[str] = None) -> List[Path]:
        """Find B_*.yml files across all known blueprint directories."""
        pattern = f"B_*_{stage}.yml" if stage else "B_*.yml"
        files: List[Path] = []
        for bp_dir in self._blueprints.values():
            files.extend(
                f for f in self._fs_glob(bp_dir, pattern)
                if ".ipynb_checkpoints" not in str(f)
            )
        if not stage or stage == "run":
            for bp_dir in self._blueprints.values():
                files.extend(
                    f for f in self._fs_glob(bp_dir, "B_*_run_*.yml")
                    if ".ipynb_checkpoints" not in str(f)
                )
        return sorted(set(files))

    def _load_blueprint_yaml(self, blueprint_path: Path) -> Dict[str, Any]:
        """Load a single B_*.yml file."""
        if not self._fs_exists(blueprint_path):
            raise FileNotFoundError(f"Blueprint file not found: {blueprint_path}")
        with self._fs_open(blueprint_path) as f:
            return yaml.safe_load(f) or {}

    def _load_grid_kwargs(self, grid_yaml_path: Path) -> Dict[str, Any]:
        """Load Grid kwargs from a two-document _grid.yml file."""
        if not self._fs_exists(grid_yaml_path):
            raise FileNotFoundError(f"Grid YAML file not found: {grid_yaml_path}")
        with self._fs_open(grid_yaml_path) as f:
            docs = list(yaml.safe_load_all(f))
        if len(docs) != 2:
            raise ValueError(f"Expected 2 documents in {grid_yaml_path}, found {len(docs)}")
        grid_data = docs[1]
        if "Grid" not in grid_data:
            raise KeyError(f"Grid section not found in {grid_yaml_path}")
        return grid_data["Grid"]

    def _extract_model_and_grid_name(self, blueprint_name: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract (model_name, grid_name) from a blueprint name.

        Strips a trailing _NNprocs suffix, then tries to match against known
        model names (longest first). Falls back to splitting on the last underscore.
        """
        if not blueprint_name:
            return None, None
        name = re.sub(r"_\d+procs$", "", blueprint_name)
        for model_name in sorted(self.model_names, key=len, reverse=True):
            if name.startswith(model_name + "_"):
                return model_name, name[len(model_name) + 1:]
        parts = name.rsplit("_", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None

    def blueprintDF(self, stage: Optional[str] = None) -> "pd.DataFrame":
        """Load all blueprints and return a pandas DataFrame.

        Parameters
        ----------
        stage : str, optional
            Blueprint stage to filter by (preconfig, postconfig, build, run).
            Defaults to None, which returns all stages.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: model_name, grid_name, blueprint_name,
            description, start_time, end_time, blueprint_path, grid_yaml_path, stage.
        """
        import pandas as pd

        records = []
        for bp_file in self._find_blueprint_stage_files(stage=stage):
            try:
                bp = self._load_blueprint_yaml(bp_file)
                blueprint_name = bp.get("name")
                if not blueprint_name:
                    print(f"Warning: skipping {bp_file}: missing 'name' field")
                    continue
                model_name, grid_name = self._extract_model_and_grid_name(blueprint_name)
                if not model_name or not grid_name:
                    print(f"Warning: skipping {bp_file}: could not parse model/grid from '{blueprint_name}'")
                    continue
                is_github = hasattr(self._fs, "org")
                grid_yaml = bp_file.parent / "_grid.yml"
                grid_yaml_exists = self._fs_exists(grid_yaml)
                if grid_yaml_exists and is_github:
                    grid_yaml_result: Optional[Union[Path, str]] = self._to_raw_github_url(grid_yaml)
                else:
                    grid_yaml_result = grid_yaml if grid_yaml_exists else None
                blueprint_path_result: Union[Path, str] = (
                    self._to_raw_github_url(bp_file) if is_github else bp_file
                )
                file_stage = next(
                    (s for s in ("preconfig", "postconfig", "build", "run") if f"_{s}" in bp_file.name),
                    None,
                )
                records.append({
                    "model_name": model_name,
                    "grid_name": grid_name,
                    "blueprint_name": blueprint_name,
                    "description": bp.get("description"),
                    "start_time": bp.get("valid_start_date"),
                    "end_time": bp.get("valid_end_date"),
                    "blueprint_path": blueprint_path_result,
                    "grid_yaml_path": grid_yaml_result,
                    "stage": file_stage,
                })
            except Exception as e:
                print(f"Warning: could not parse {bp_file}: {e}")
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
