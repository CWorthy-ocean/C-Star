"""DomainCatalog: manages the catalog directory structure for C-Star forge."""

from __future__ import annotations

import logging
import os
import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import fsspec
import yaml

from cstar.base.application_names import canonical_application
from cstar.base.deprecation import deprecated
from cstar.base.env import ENV_CSTAR_CATALOG, default_catalog_root, get_env_item

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import pandas as pd

    from cstar.applications.forge.namelist_model import BgcMode
    from cstar.catalog.outputs import BlueprintOutput
    from cstar.orchestration.models import CatalogSpecKind

logger = logging.getLogger(__name__)

_DEFAULT_CATALOG_ROOT = Path(__file__).parent / "bundled"

# Pre-move (cstar-forge repo) catalog location. Never written to or read from
# automatically -- only checked for a one-time hint, see ``user_catalog_root``.
_LEGACY_CATALOG_ROOT = Path.home() / "cstar-forge-data" / "catalog"

# Set once ``user_catalog_root`` has logged its legacy-location hint, so a process
# that calls it repeatedly (it has no other caching) logs the hint only once.
_legacy_catalog_hint_logged = False

# Registered application names the *legacy* blueprint layouts implied (flat
# ``*.forge_blueprint.yaml`` = forge; flat ``B_*.yaml`` and
# ``<machine>/<name>/B_*.yaml`` = the application then called ``roms_marbl``,
# filed under its current name). The catalog stays import-light and does not
# import the applications, so these name the legacy layouts only; the current
# layout takes its application from the directory name.
_FORGE = "forge"
_UCLA_ROMS = canonical_application("roms_marbl")

# Each spec kind (its directory under the catalog root) -> the file name, minus
# its extension, an entry had in the legacy per-entry directory layout
# ``<Kind>/<name>/<File>.yaml``, read until 0.20.0. The current layout is
# ``<Kind>/<name>.yaml``.
_SPEC_KINDS: dict[CatalogSpecKind, str] = {
    "ModelSpec": "model",
    "DomainSpec": "Domain",
    "ForcingSpec": "Forcing",
    "OutputSpec": "Output",
    "CdrSpec": "Cdr",
}


def spec_entry_name(path: Path) -> str:
    """Return the catalog entry name a spec file stands for.

    That is the file's stem, except for a file in the legacy
    ``<Kind>/<name>/<File>.yaml`` layout, which is named by its directory.
    """
    if any(
        path.parent.parent.name == kind and path.stem == stem
        for kind, stem in _SPEC_KINDS.items()
    ):
        return path.parent.name
    return path.stem


def user_catalog_root() -> Path:
    """Return the root of the user-writable catalog layer.

    Deliberately home-anchored (``~/cstar/catalog`` by default -- see
    ``cstar.base.env.default_catalog_root``), NOT on the scratch file system
    ``CSTAR_DATA_HOME`` and the source-data cache may use: catalog entries are
    durable, user-registered content, not job-scoped working data, and must
    survive HPC scratch purges.

    If ``CSTAR_CATALOG`` (C-Star's registered env var, see ``cstar.base.env``)
    is set and non-empty, the first ``os.pathsep``-separated entry is used
    instead (expanded and resolved). Does not create the directory.
    """
    entries = _env_catalog_entries()
    if entries:
        first = entries[0]
        if first.strip().lower() == "local":
            raise ValueError(
                f"{ENV_CSTAR_CATALOG}: the bundled catalog ('local') is read-only "
                "and cannot be the writable top layer; list it after your own "
                "catalog root instead (e.g. '/path/to/mine:local')."
            )
        return Path(first).expanduser().resolve()

    root = (
        Path(default_catalog_root(get_env_item(ENV_CSTAR_CATALOG)))
        .expanduser()
        .resolve()
    )
    global _legacy_catalog_hint_logged
    if (
        not _legacy_catalog_hint_logged
        and ENV_CSTAR_CATALOG not in os.environ
        and not root.exists()
        and _LEGACY_CATALOG_ROOT.exists()
    ):
        logger.info(
            "No catalog found at %s (the default location). A catalog from a "
            "previous cstar-forge install exists at %s -- it is not used "
            "automatically; move it to %s, or set %s to point at it.",
            root,
            _LEGACY_CATALOG_ROOT,
            root,
            ENV_CSTAR_CATALOG,
        )
        _legacy_catalog_hint_logged = True
    return root


def _env_catalog_entries() -> list[str]:
    """Non-empty ``os.pathsep``-separated entries of ``CSTAR_CATALOG``, in order.

    The single parser for the env var: ``user_catalog_root``,
    ``default_catalog_stack``, and the wizard's catalog bar must all agree on
    which entry is the writable top, including when the value carries leading
    or doubled separators (e.g. ``"$UNSET:/opt/shared"`` expanding to
    ``":/opt/shared"``).
    """
    return [e for e in os.environ.get(ENV_CSTAR_CATALOG, "").split(os.pathsep) if e]


def _extract_model_and_grid(
    ucla_roms_blueprint_name: str, model_names: list[str]
) -> tuple[str | None, str | None]:
    """Extract (model_name, grid_name) from a blueprint name.

    Strips a trailing _NNprocs suffix, then tries to match against *model_names*
    (longest first). Falls back to splitting on the last underscore. Shared by
    ``DomainCatalog`` (its own models) and ``LayeredCatalog`` (union of layers).
    """
    if not ucla_roms_blueprint_name:
        return None, None
    name = re.sub(r"_\d+procs$", "", ucla_roms_blueprint_name)
    for model_name in sorted(model_names, key=len, reverse=True):
        if name.startswith(model_name + "_"):
            return model_name, name[len(model_name) + 1 :]
    parts = name.rsplit("_", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, None


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


def _yaml_files(files: Iterable[Path]) -> list[Path]:
    """Return the ``.yaml``/``.yml`` *files*, ``.yaml`` winning when a stem has both
    (as ``DomainCatalog._fs_glob_dual`` does).
    """
    by_stem: dict[Path, Path] = {}
    for f in sorted(files, key=lambda f: f.suffix != ".yaml"):
        if f.suffix in (".yaml", ".yml"):
            by_stem.setdefault(f.parent / f.stem, f)
    return sorted(by_stem.values())


def _in_earlier_name_dir(entry: tuple[str, str, Path]) -> bool:
    """Return whether a legacy ``(application, name, path)`` entry sits in
    ``blueprints/<earlier application name>/``.
    """
    application, _, path = entry
    directory = path.parent
    return (
        directory.parent.name.lower() == "blueprints"
        and directory.name != application
        and canonical_application(directory.name) == application
    )


class DomainCatalog:
    """C-Star DomainCatalog manages the hierarchical system of validated/registered "domains."

    The DomainCatalog holds, inside a folder called 'catalog', models and model domains
    that together describe "validated" C-Star model solutions.

    The base of a given catalog (self.catalog_root) is the *inner* catalog directory that
    directly contains the subdirectories below. It can be a local path or a remote URL;
    file access is mediated by fsspec for portability.

    Catalog structure (one file per entry, named for the entry)::

        catalog/
        ├── ModelSpec/
        │   └── roms-marbl-0.12-default.yaml   (code + model_settings)
        ├── DomainSpec/
        │   ├── ccs-12km.yaml
        │   └── PAC_2fth_deg.yaml
        ├── ForcingSpec/<name>.yaml
        ├── OutputSpec/<name>.yaml
        ├── CdrSpec/<name>.yaml
        ├── Blueprints/  (alias: blueprints/)
        │   └── <application>/<blueprint-name>.yaml
        └── Observations/

    Parameters
    ----------
    catalog_root : str or Path or None
        Root of the catalog (inner directory containing ModelSpec/, etc.).
        Defaults to the package-bundled catalog at ``cstar/catalog/bundled``.
        Pass a github URL string for remote catalogs.
    read_only : bool
        Mark this store non-writable. Non-local stores (remote/github/http)
        are always treated as read-only regardless of this flag.
    label : str or None
        Human-readable name for this store, used in error messages and by
        ``LayeredCatalog``. Defaults to ``"bundled"`` for the packaged catalog
        root, else ``str(catalog_root)``.
    """

    def __init__(
        self,
        catalog_root: str | Path | None = None,
        initialize_catalog_from: str | Path | None = None,
        initialize_catalog_clobber: bool = False,
        suppress_validation: bool = False,
        github_token: str | None = None,
        *,
        read_only: bool = False,
        label: str | None = None,
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

        # Non-local stores (remote/github/http) are never writable, no matter
        # what the caller asked for.
        _root_is_bundled = self._is_local and (
            self.catalog_root.resolve() == _DEFAULT_CATALOG_ROOT.resolve()
        )
        # The packaged catalog lives inside the installed package (site-packages
        # on a real install): never writable through the API, no matter what the
        # caller asked for -- user work must not land somewhere an upgrade wipes.
        self.read_only = bool(read_only) or not self._is_local or _root_is_bundled
        self.label = (
            label
            if label is not None
            else ("bundled" if _root_is_bundled else str(self.catalog_root))
        )

        # Merge catalog skeleton from a source catalog before scanning.
        if initialize_catalog_from is not None:
            self._initialize_from(
                initialize_catalog_from, clobber=initialize_catalog_clobber
            )

        # Internal registries
        # kind -> entry name -> spec file
        self._specs: dict[CatalogSpecKind, dict[str, Path]] = {}
        # kind -> ((name, file) read from the legacy layout, (name, file) ignored
        # there because a current-layout file has the same name)
        self._legacy_specs: dict[
            CatalogSpecKind, tuple[list[tuple[str, Path]], list[tuple[str, Path]]]
        ] = {}
        self._archived_model_names: list[str] | None = None
        # application -> entry name -> blueprints/<application>/<name>.yaml file
        self._blueprints: dict[str, dict[str, Path]] = {}
        # (application, name, file) for entries found in a legacy layout
        self._legacy_blueprints: list[tuple[str, str, Path]] = []

        for kind in _SPEC_KINDS:
            self._scan_specs(kind)
        self._warn_legacy_specs()
        self._scan_blueprints()

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

    def _fs_list(self, path: Path) -> tuple[list[Path], list[Path]]:
        """Return the (sorted) non-hidden ``(directories, files)`` of *path*.

        One ``iterdir`` locally, one ``ls(detail=True)`` remotely; names
        starting with ``.`` (``.ipynb_checkpoints``, dotfiles) are left out.
        """
        dirs: list[Path] = []
        files: list[Path] = []
        if self._is_local:
            for p in path.iterdir():
                if not p.name.startswith("."):
                    (dirs if p.is_dir() else files).append(p)
        else:
            for e in self._fs.ls(str(path), detail=True):
                p = Path(e["name"])
                if not p.name.startswith("."):
                    (dirs if e.get("type") == "directory" else files).append(p)
        return sorted(dirs), sorted(files)

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

    def _scan_specs(self, kind: CatalogSpecKind) -> None:
        """Scan ``<kind>/`` for spec files.

        An entry is ``<kind>/<name>.yaml`` (or ``.yml``; ``.yaml`` wins when a
        name has both). The legacy per-entry directory
        ``<kind>/<name>/<File>.yaml`` (``File`` from ``_SPEC_KINDS``) stays
        readable until 0.20.0 and is recorded in ``_legacy_specs``; one whose
        name a current-layout file already has is ignored, and recorded apart so
        the deprecation warning and the migration report it. Hidden names
        (``.ipynb_checkpoints``, dotfiles) are skipped.
        """
        entries: dict[str, Path] = {}
        found: list[tuple[str, Path]] = []
        ignored: list[tuple[str, Path]] = []
        kind_dir = self.catalog_root / kind
        try:
            for f in self._fs_glob_dual(kind_dir, "*"):
                if not f.name.startswith("."):
                    entries[f.stem] = f
            for f in self._fs_glob_dual(kind_dir, f"*/{_SPEC_KINDS[kind]}"):
                name = f.parent.name
                if name.startswith("."):
                    continue
                if name in entries:
                    ignored.append((name, f))
                else:
                    entries[name] = f
                    found.append((name, f))
        except Exception as exc:
            logger.warning("Failed to scan %s: %s", kind, exc)
        self._specs[kind] = entries
        self._legacy_specs[kind] = (found, ignored)
        if kind == "ModelSpec":
            self._archived_model_names = None

    def _warn_legacy_specs(self) -> None:
        """Announce, in one warning, every spec the scan found in the legacy layout."""
        ignored = {
            path for _, losers in self._legacy_specs.values() for _, path in losers
        }
        if not (legacy := self.legacy_specs):
            return
        listing = ", ".join(
            f"{kind}/{name} ({path}{', ignored: duplicate' if path in ignored else ''})"
            for kind, name, path in legacy
        )
        # On removal, keep this detection and raise an error naming
        # `cstar admin migrate-catalog`: a legacy file must never just vanish.
        deprecated(
            f"The per-entry spec directory layout in catalog {self.catalog_root} "
            f"({listing})",
            "<Kind>/<name>.yaml (`cstar admin "
            f"migrate-catalog {self.catalog_root}` moves the files)",
            removal="0.20.0",
        )

    def _scan_blueprints(self) -> None:
        """Scan blueprints/ (and Blueprints/) for blueprint files.

        The layout is ``blueprints/<application>/<name>.yaml``: the directory
        is the registered application name, the file (minus its final
        ``.yaml``/``.yml``) the entry name. Four legacy forms stay readable
        until 0.20.0 and are recorded in ``legacy_blueprints``: flat
        ``<name>.forge_blueprint.yaml`` (forge), flat ``B_<name>.yaml`` and
        ``<machine>/<name>/B_*.yaml`` (ucla_roms), and
        ``roms_marbl/<name>.yaml`` (ucla_roms, under the application's earlier
        name). One deprecation warning lists the first three, another the last.

        A subdirectory of ``blueprints/`` holding subdirectories is a legacy
        machine directory; any other is an application directory. Hidden names
        are skipped. A current-layout entry shadows a legacy one of the same
        application and name (debug log only). Among legacy forms a flat file
        wins over one in a directory, and of two directories the first by name
        does; a legacy file that loses is still recorded in ``legacy_blueprints``
        (after the winners) so the migration reports it. Each directory is
        listed once and a failure in one directory is logged without abandoning
        its siblings.
        """
        current: dict[str, dict[str, Path]] = {}
        legacy: list[tuple[str, str, Path]] = []
        scanned: list[Path] = []
        for subdir_name in ("blueprints", "Blueprints"):
            bp_root = self.catalog_root / subdir_name
            if not self._fs_exists(bp_root):
                continue
            # `Blueprints/` aliases `blueprints/` on a case-insensitive file system
            if self._is_local and any(bp_root.samefile(r) for r in scanned):
                continue
            scanned.append(bp_root)
            try:
                dirs, files = self._fs_list(bp_root)
            except Exception as exc:
                logger.warning(
                    "Failed to scan blueprints under %s: %s", subdir_name, exc
                )
                continue
            for f in _yaml_files(files):
                if f.stem.endswith(".forge_blueprint"):
                    name = f.stem.removesuffix(".forge_blueprint")
                    application = _FORGE
                elif f.stem.startswith("B_"):
                    name = f.stem.removeprefix("B_")
                    application = _UCLA_ROMS
                else:
                    continue
                if name:
                    legacy.append((application, name, f))
            for app_dir in dirs:
                try:
                    self._scan_blueprint_dir(app_dir, current, legacy)
                except Exception as exc:
                    logger.warning(
                        "Failed to scan blueprints under %s: %s", app_dir, exc
                    )

        self._blueprints = {app: dict(entries) for app, entries in current.items()}
        winners: list[tuple[str, str, Path]] = []
        losers: list[tuple[str, str, Path]] = []
        for application, name, path in legacy:
            if name in current.get(application, {}):
                logger.debug(
                    "Legacy blueprint %s/%s (%s) shadowed by blueprints/%s/%s.yaml",
                    application,
                    name,
                    path,
                    application,
                    name,
                )
            elif name in self._blueprints.get(application, {}):
                losers.append((application, name, path))
            else:
                self._blueprints.setdefault(application, {})[name] = path
                winners.append((application, name, path))
        self._legacy_blueprints = [*winners, *losers]

        def listing(entries: list[tuple[str, str, Path]]) -> str:
            return ", ".join(
                f"{application}/{name} ({path}"
                f"{', ignored: duplicate' if (application, name, path) in losers else ''})"
                for application, name, path in entries
            )

        # On removal, keep this detection and raise an error naming
        # `cstar admin migrate-catalog`: a legacy file must never just vanish.
        renamed = [e for e in self._legacy_blueprints if _in_earlier_name_dir(e)]
        if others := [e for e in self._legacy_blueprints if e not in renamed]:
            deprecated(
                f"The legacy blueprint layout in catalog {self.catalog_root} "
                f"({listing(others)})",
                "blueprints/<application>/<name>.yaml (`cstar admin "
                f"migrate-catalog {self.catalog_root}` moves the files)",
                removal="0.20.0",
            )
        if renamed:
            deprecated(
                f"An application's earlier name as a blueprint directory in "
                f"catalog {self.catalog_root} ({listing(renamed)})",
                "blueprints/<application>/ under its current name (`cstar admin "
                f"migrate-catalog {self.catalog_root}` moves the files)",
                removal="0.20.0",
            )

    def _scan_blueprint_dir(
        self,
        directory: Path,
        current: dict[str, dict[str, Path]],
        legacy: list[tuple[str, str, Path]],
    ) -> None:
        """Classify one ``blueprints/<dir>/`` and record its entries.

        A directory with subdirectories is a legacy ``<machine>/`` of
        ``<name>/B_*.yaml`` directories (files sitting directly in it are
        ignored); any other is an application directory. One named for an
        application's earlier name is legacy too: its entries are recorded as
        the current application's.
        """
        subdirs, files = self._fs_list(directory)
        if not subdirs:
            application = canonical_application(directory.name)
            entries = [(f.name.removesuffix(f.suffix), f) for f in _yaml_files(files)]
            if application != directory.name:  # an earlier name: a legacy form
                legacy.extend((application, name, f) for name, f in entries)
                return
            for name, f in entries:
                current.setdefault(application, {}).setdefault(name, f)
            return
        if files:
            logger.debug(
                "Ignoring files in legacy machine directory %s: %s",
                directory,
                ", ".join(f.name for f in files),
            )
        for bp_dir in subdirs:
            try:
                found = [
                    f
                    for f in _yaml_files(self._fs_list(bp_dir)[1])
                    if f.name.startswith("B_")
                ]
            except Exception as exc:
                logger.warning("Failed to scan blueprints under %s: %s", bp_dir, exc)
                continue
            if len(found) == 1:
                legacy.append((_UCLA_ROMS, bp_dir.name, found[0]))
            else:
                legacy.extend((_UCLA_ROMS, f.stem.removeprefix("B_"), f) for f in found)

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def _initialize_from(self, source: str | Path, clobber: bool = False) -> None:
        """Merge ModelSpec/ and DomainSpec/ from a source catalog into self.catalog_root.

        Files that do not already exist at the destination are always copied.
        Files that exist at both source and destination are "conflicts":
        - ``clobber=True``: overwrite conflicts silently.
        - ``clobber=False``: raise ``ValueError`` listing all conflicts and
          suggest re-running with ``initialize_catalog_clobber=True``.

        Parameters
        ----------
        source : str or Path
            Inner catalog directory to merge from, or ``'local'`` to use the
            package-bundled catalog (``cstar/catalog/bundled``).
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
            "ModelSpec",
            "DomainSpec",
            "ForcingSpec",
            "OutputSpec",
            "CdrSpec",
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
        """Raise ValueError if ModelSpec/ is missing or empty.

        Only called for non-default catalog roots that were not just initialized.
        Uses the already-populated registry so remote filesystems work.
        """
        if not self._specs["ModelSpec"]:
            raise ValueError(
                f"No valid catalog found at '{self.catalog_root}'. "
                f"Missing: ModelSpec/ (with at least one <name>.yaml).\n"
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
        return sorted(self._specs["ModelSpec"])

    @property
    def archived_model_names(self) -> list[str]:
        """Return a sorted list of model names whose spec is marked archived.

        Read lazily on first access (a GitHub-backed catalog pays one fetch per
        spec) and cached until the next model scan. An unreadable spec counts
        as not archived, so it still fails loudly when selected.
        """
        if self._archived_model_names is None:
            archived = []
            for name in self.model_names:
                try:
                    if self.load_model_spec(name).archived:
                        archived.append(name)
                except Exception as exc:
                    logger.debug("Could not read model spec '%s': %s", name, exc)
            self._archived_model_names = archived
        return list(self._archived_model_names)

    @property
    def domain_names(self) -> list[str]:
        """Return a sorted list of available domain names."""
        return sorted(self._specs["DomainSpec"])

    @property
    def forcing_names(self) -> list[str]:
        """Return a sorted list of available forcing-spec names."""
        return sorted(self._specs["ForcingSpec"])

    @property
    def output_names(self) -> list[str]:
        """Return a sorted list of available output-spec names."""
        return sorted(self._specs["OutputSpec"])

    @property
    def cdr_names(self) -> list[str]:
        """Return a sorted list of available CDR-spec names."""
        return sorted(self._specs["CdrSpec"])

    @property
    def blueprint_applications(self) -> list[str]:
        """Return a sorted list of applications that have blueprint entries."""
        return sorted(self._blueprints)

    def blueprint_names(self, application: str) -> list[str]:
        """Return a sorted list of blueprint entry names for *application*.

        An earlier name of an application is read as its current name. Empty
        for an application with no entries.
        """
        return sorted(self._blueprints.get(canonical_application(application), {}))

    @property
    def legacy_blueprints(self) -> list[tuple[str, str, Path]]:
        """Return ``(application, name, path)`` for each entry found in a legacy layout."""
        return list(self._legacy_blueprints)

    @property
    def legacy_specs(self) -> list[tuple[CatalogSpecKind, str, Path]]:
        """Return ``(kind, name, path)`` for each spec found in the legacy
        ``<Kind>/<name>/<File>.yaml`` layout: those read from it, then those
        ignored because a ``<Kind>/<name>.yaml`` has the same name.
        """
        found = [
            (kind, name, path)
            for kind, (entries, _) in self._legacy_specs.items()
            for name, path in entries
        ]
        ignored = [
            (kind, name, path)
            for kind, (_, entries) in self._legacy_specs.items()
            for name, path in entries
        ]
        return [*found, *ignored]

    @property
    def ucla_roms_blueprint_names(self) -> list[str]:
        """Return ``blueprint_names("ucla_roms")``."""
        return self.blueprint_names(_UCLA_ROMS)

    @property
    def forge_blueprint_names(self) -> list[str]:
        """Return ``blueprint_names("forge")``."""
        return self.blueprint_names(_FORGE)

    @property
    def blueprints_dir(self) -> Path:
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

    # ------------------------------------------------------------------
    # Path accessors (raise KeyError if not found)
    # ------------------------------------------------------------------

    def model_path(self, model_name: str) -> Path:
        """Return the spec file of a named model."""
        if model_name not in self._specs["ModelSpec"]:
            raise KeyError(
                f"Model '{model_name}' not found in catalog at {self.catalog_root}. "
                f"Available models: {self.model_names}"
            )
        return self._specs["ModelSpec"][model_name]

    def domain_path(self, domain_name: str) -> Path:
        """Return the spec file of a named domain."""
        if domain_name not in self._specs["DomainSpec"]:
            raise KeyError(
                f"Domain '{domain_name}' not found in catalog at {self.catalog_root}. "
                f"Available domains: {self.domain_names}"
            )
        return self._specs["DomainSpec"][domain_name]

    def blueprint_path(self, application: str, name: str) -> Path:
        """Return the blueprint file for *name* under *application*.

        An earlier name of an application is read as its current name.
        """
        application = canonical_application(application)
        entries = self._blueprints.get(application, {})
        if name not in entries:
            raise KeyError(
                f"Blueprint '{name}' for application '{application}' not found in "
                f"catalog at {self.catalog_root}. "
                f"Available: {', '.join(sorted(entries)) or '(none)'}"
            )
        return entries[name]

    def blueprint_dir(self, application: str) -> Path:
        """Return where *application*'s blueprint files live (not created).

        An earlier name of an application is read as its current name: new files
        belong in the current layout.
        """
        return self.blueprints_dir / canonical_application(application)

    def ucla_roms_blueprint_path(self, ucla_roms_blueprint_name: str) -> Path:
        """Return ``blueprint_path("ucla_roms", name)``."""
        return self.blueprint_path(_UCLA_ROMS, ucla_roms_blueprint_name)

    def forge_blueprint_path(self, forge_blueprint_name: str) -> Path:
        """Return ``blueprint_path("forge", name)``."""
        return self.blueprint_path(_FORGE, forge_blueprint_name)

    # ------------------------------------------------------------------
    # Data accessors (return raw dicts)
    # ------------------------------------------------------------------

    def model_data(self, model_name: str) -> dict:
        """Return the raw YAML data dict for a named model."""
        path = self.model_path(model_name)
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    def domain_data(self, domain_name: str) -> dict:
        """Return the raw YAML data dict for a named domain."""
        path = self.domain_path(domain_name)
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    def forcing_data(self, forcing_name: str) -> dict:
        """Return the raw YAML data dict for a named forcing spec."""
        if forcing_name not in self._specs["ForcingSpec"]:
            raise KeyError(
                f"ForcingSpec '{forcing_name}' not found in catalog at {self.catalog_root}. "
                f"Available: {self.forcing_names}"
            )
        path = self._specs["ForcingSpec"][forcing_name]
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

    def output_data(self, output_name: str) -> dict:
        """Return the raw YAML data dict for a named output spec."""
        if output_name not in self._specs["OutputSpec"]:
            raise KeyError(
                f"OutputSpec '{output_name}' not found in catalog at {self.catalog_root}. "
                f"Available: {self.output_names}"
            )
        path = self._specs["OutputSpec"][output_name]
        with self._fs_open(path) as f:
            data = yaml.safe_load(f) or {}
        data.pop("description", None)
        return data

    def cdr_data(self, cdr_name: str) -> dict:
        """Return the raw YAML data dict for a named CDR spec."""
        if cdr_name not in self._specs["CdrSpec"]:
            raise KeyError(
                f"CdrSpec '{cdr_name}' not found in catalog at {self.catalog_root}. "
                f"Available: {self.cdr_names}"
            )
        path = self._specs["CdrSpec"][cdr_name]
        with self._fs_open(path) as f:
            return yaml.safe_load(f) or {}

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
            Parsed DomainSpec content.
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

    def ucla_roms_blueprint(self, ucla_roms_blueprint_id: str | int) -> Path:
        """Return a ucla_roms blueprint file Path by name (str) or index (int).

        Parameters
        ----------
        ucla_roms_blueprint_id : str or int
            Blueprint name or zero-based index into ucla_roms_blueprint_names.
        """
        if isinstance(ucla_roms_blueprint_id, str):
            return self.ucla_roms_blueprint_path(ucla_roms_blueprint_id)
        elif isinstance(ucla_roms_blueprint_id, int):
            return self.ucla_roms_blueprint_path(
                self.ucla_roms_blueprint_names[ucla_roms_blueprint_id]
            )
        else:
            raise ValueError(
                f"ucla_roms_blueprint_id must be str or int, got {type(ucla_roms_blueprint_id)}"
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
        from cstar.applications.forge.models import load_models_yaml

        path = self.model_path(model_name)
        return load_models_yaml(path, model_name)

    # ------------------------------------------------------------------
    # Registration / mutation methods
    # ------------------------------------------------------------------

    def _check_writable(self) -> None:
        """Guard called first by every mutator. Also closes a latent bug: a
        GitHub-backed (or other non-local) catalog used to fall through to a
        bare ``Path.mkdir``/``open`` and silently write under the CWD instead
        of failing.
        """
        if self.read_only:
            raise PermissionError(
                f"Catalog store '{self.label}' at {self.catalog_root} is read-only"
            )

    def _write_spec(
        self, kind: CatalogSpecKind, name: str, data: dict[str, Any]
    ) -> None:
        """Write a new *kind* entry *name* as ``<kind>/<name>.yaml`` and rescan *kind*.

        Refuses a name this catalog already has, in either layout.
        """
        path = self.catalog_root / kind / f"{name}.yaml"
        existing = self._specs[kind].get(name)
        if existing is not None or path.exists():
            raise FileExistsError(
                f"{kind} '{name}' already exists at {existing or path}"
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            yaml.safe_dump(
                data, f, default_flow_style=False, sort_keys=False, allow_unicode=True
            )
        self._scan_specs(kind)

    def _put_spec_file(self, kind: CatalogSpecKind, name: str, src: Path) -> None:
        """Copy the spec file *src* in as ``<kind>/<name>.yaml``, replacing the
        file of that name, and rescan *kind*.
        """
        self._check_writable()
        dest = self.catalog_root / kind / f"{name}.yaml"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        self._scan_specs(kind)

    def register_model(self, model_path: Path | str) -> None:
        """Register a model by copying its spec file into ModelSpec/ and rescanning.

        Parameters
        ----------
        model_path : str or Path
            The model's spec file (``.yaml`` or ``.yml``). The entry is named
            after the file (see ``spec_entry_name``) and written as
            ``ModelSpec/<name>.yaml``, replacing a file of that name.
        """
        self._check_writable()
        src = Path(model_path).expanduser()
        if not (src.is_file() and src.suffix in (".yaml", ".yml")):
            raise ValueError(f"model_path must be a model spec .yaml file: {src}")
        self._put_spec_file("ModelSpec", spec_entry_name(src), src)

    def register_domain(self, builder: Any) -> None:
        """Create a new DomainSpec entry from a ForgeExecutor instance.

        Writes ``DomainSpec/<grid_name>.yaml``, the domain name taken from
        builder.grid_name. Refuses to overwrite an existing entry.

        Parameters
        ----------
        builder : ForgeExecutor
            A configured builder whose grid_name, grid_kwargs, open_boundaries,
            and partitioning will be recorded. ``ForgeExecutor`` no longer tracks
            a bare ``model_name`` (post naming-refactor it only carries the
            combined blueprint ``name``), so the written spec omits it;
            ``register_domain_from_dict`` accepts an explicit ``model_name`` when
            the caller has one (e.g. the wizard's composition).
        """
        self._check_writable()

        # n_procs_x/y are None under auto_tiling (mutually exclusive with
        # n_cores) -- write whichever pair the builder actually carries rather
        # than a null placeholder, mirroring v_sponge/dt's omit-when-absent
        # convention below.
        partitioning: dict[str, Any] = {}
        if builder.partitioning.n_procs_x is not None:
            partitioning["n_procs_x"] = builder.partitioning.n_procs_x
        if builder.partitioning.n_procs_y is not None:
            partitioning["n_procs_y"] = builder.partitioning.n_procs_y
        if builder.partitioning.auto_tiling:
            partitioning["auto_tiling"] = True
            partitioning["n_cores"] = builder.partitioning.n_cores

        domain_data: dict[str, Any] = {
            "description": builder.description,
            "grid_name": builder.grid_name,
            "start_time": builder.start_date.isoformat(),
            "end_time": builder.end_date.isoformat(),
            "grid_kwargs": builder.grid_kwargs,
            "open_boundaries": builder.open_boundaries.model_dump(),
            "partitioning": partitioning,
        }
        if builder.grid_kwargs_parent:
            domain_data["grid_kwargs_parent"] = builder.grid_kwargs_parent
        if builder.grid_kwargs_child:
            domain_data["grid_kwargs_child"] = builder.grid_kwargs_child
        # v_sponge: best-effort -- this executor-driven path predates the
        # wizard's touched/derived distinction (see forge_blueprint_wizard's
        # _domain_spec_data, the authoritative save path), so it always
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

        self._write_spec("DomainSpec", builder.grid_name, domain_data)

    def register_domain_from_dict(self, name: str, domain_data: dict[str, Any]) -> None:
        """Create a new DomainSpec entry from a plain dict (the wizard's save path).

        Writes ``domain_data`` verbatim as ``DomainSpec/<name>.yaml``, then
        rescans. Refuses to overwrite an existing entry -- catalog entries are
        named, shared resources; pick a different name or delete the old one
        first.
        """
        self._check_writable()
        self._write_spec("DomainSpec", name, domain_data)

    def register_output(
        self, name: str, output_settings: dict[str, Any], description: str = ""
    ) -> None:
        """Create a new OutputSpec entry (``OutputSpec/<name>.yaml``).

        ``output_settings`` is the output-owned subset of ``model_settings`` (see
        ``forge_blueprint_resolve.extract_output_settings``). Refuses to overwrite
        an existing entry.
        """
        self._check_writable()
        self._write_spec(
            "OutputSpec", name, {"description": description, **output_settings}
        )

    def register_forcing(
        self,
        name: str,
        forcing_inputs: dict[str, Any],
        description: str = "",
    ) -> None:
        """Create a new ForcingSpec entry (``ForcingSpec/<name>.yaml``).

        ``forcing_inputs`` is the ``{initial_conditions, forcing}`` shape returned
        by the wizard's forcing editor ``gather()``. Refuses to overwrite an
        existing entry.
        """
        self._check_writable()
        self._write_spec(
            "ForcingSpec", name, {"description": description, **forcing_inputs}
        )

    def register_cdr(
        self,
        name: str,
        *,
        description: str = "",
        mode: str,
        cdr_forcing: dict[str, Any] | None = None,
        cdr_forcing_file: dict[str, Any] | None = None,
    ) -> None:
        """Create a new CdrSpec entry (``CdrSpec/<name>.yaml``).

        ``mode`` selects how CDR forcing is configured for a domain that picks
        this spec:

          - ``"none"`` / ``"upscaled"``: no CDR-forcing configuration is stored
            (``upscaled`` reads CDR forcing supplied at runtime from a parent
            domain); neither ``cdr_forcing`` nor ``cdr_forcing_file`` may be given.
          - ``"simple"`` / ``"yaml"``: a roms-tools ``CDRForcing`` built from
            ``cdr_forcing`` kwargs (wizard-entered, or imported from a
            roms-tools YAML); ``cdr_forcing`` is required and
            ``cdr_forcing_file`` must be omitted.
          - ``"netcdf"``: a user-provided CDR-forcing NetCDF file, recorded as
            ``cdr_forcing_file`` (``{location, content_hash}``);
            ``cdr_forcing_file`` is required and ``cdr_forcing`` must be omitted.

        Refuses to overwrite an existing entry.
        """
        self._check_writable()
        # Deferred import, mirroring load_models_yaml below -- keeps the catalog
        # module import-light for callers that never register.
        from cstar.applications.forge.models import CDR_MODES, CdrSpec

        if mode not in CDR_MODES:
            raise ValueError(f"mode must be one of {sorted(CDR_MODES)}, got {mode!r}")
        # Field/mode coherence is validated through CdrSpec itself (the single
        # source of those rules -- pydantic's ValidationError is a ValueError),
        # so a spec written here can never fail to load later via
        # cdr_data() -> CdrSpec.
        CdrSpec(mode=mode, cdr_forcing=cdr_forcing, cdr_forcing_file=cdr_forcing_file)

        self._write_spec(
            "CdrSpec",
            name,
            {
                "description": description,
                "mode": mode,
                "cdr_forcing": cdr_forcing,
                "cdr_forcing_file": cdr_forcing_file,
            },
        )

    def register_model_from_settings(
        self,
        name: str,
        model_settings: dict[str, Any],
        base_model_path: Path | str,
        description: str = "",
        *,
        bgc_mode: BgcMode | None = None,
        use_pio: bool | None = None,
        roms_ref: str | None = None,
        marbl_ref: str | None = None,
    ) -> None:
        """Create a new ModelSpec entry (``ModelSpec/<name>.yaml``).

        Clones ``code``/``templates_commit`` verbatim from the spec file
        ``base_model_path`` and swaps in ``model_settings`` (the model-owned subset --
        see the wizard's ``_model_owned_settings``). ``bgc_mode``/``use_pio``/
        ``roms_ref``/``marbl_ref`` clone the base value when left ``None``, or
        take the given override when not -- these are the live per-run toggles
        the wizard's widgets carry (``use_pio`` and ``code.marbl``/``bgc_mode``
        don't survive in ``model_settings`` since they're resolver-derived
        leaves stripped by ``_model_owned_settings``, and ``code.roms.commit``/
        ``code.marbl.commit`` aren't part of ``model_settings`` at all).
        Refuses to overwrite an existing entry.
        """
        self._check_writable()
        base = yaml.safe_load(Path(base_model_path).read_text()) or {}
        code = dict(base.get("code") or {})

        def _override_ref(repo_key: str, ref: str | None) -> None:
            # Mirror forge_blueprint_resolve.py's roms_ref/marbl_ref handling:
            # commit/branch/tag are all valid checkout targets, so store the
            # override in `commit` and drop `branch` (C-Star requires exactly one
            # of the two). Keep every other repo verbatim -- even if currently
            # unused -- so the saved spec stays toggleable for use_pio/bgc_mode
            # later.
            if ref and code.get(repo_key):
                repo = dict(code[repo_key])
                repo["commit"] = ref
                repo.pop("branch", None)
                code[repo_key] = repo

        _override_ref("roms", roms_ref)
        _override_ref("marbl", marbl_ref)
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
        self._write_spec("ModelSpec", name, data)

    def copy_domain(self, domain_name: str, catalog: DomainCatalog) -> None:
        """Copy a domain's spec file to another DomainCatalog, replacing the
        file of that name there.

        Parameters
        ----------
        domain_name : str
            Name of the domain to copy from this catalog.
        catalog : DomainCatalog
            Target catalog to copy the domain into.
        """
        catalog._put_spec_file("DomainSpec", domain_name, self.domain_path(domain_name))

    def copy_model(self, model_name: str, catalog: DomainCatalog) -> None:
        """Copy a model's spec file to another DomainCatalog, replacing the
        file of that name there.

        Parameters
        ----------
        model_name : str
            Name of the model to copy from this catalog.
        catalog : DomainCatalog
            Target catalog to copy the model into.
        """
        catalog._put_spec_file("ModelSpec", model_name, self.model_path(model_name))

    # ------------------------------------------------------------------
    # Blueprint DataFrame methods
    # ------------------------------------------------------------------

    def _find_ucla_roms_blueprint_files(self) -> list[Path]:
        """Return the ucla_roms blueprint files across all known entries."""
        return sorted(
            {
                f
                for f in self._blueprints.get(_UCLA_ROMS, {}).values()
                if ".ipynb_checkpoints" not in str(f)
            }
        )

    def _load_ucla_roms_blueprint_yaml(
        self, ucla_roms_blueprint_path: Path
    ) -> dict[str, Any]:
        """Load a single B_*.yaml (or legacy B_*.yml) file."""
        if not self._fs_exists(ucla_roms_blueprint_path):
            raise FileNotFoundError(
                f"Blueprint file not found: {ucla_roms_blueprint_path}"
            )
        with self._fs_open(ucla_roms_blueprint_path) as f:
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
        self, ucla_roms_blueprint_name: str
    ) -> tuple[str | None, str | None]:
        """Extract (model_name, grid_name) from a blueprint name.

        Strips a trailing _NNprocs suffix, then tries to match against known
        model names (longest first). Falls back to splitting on the last underscore.
        """
        return _extract_model_and_grid(ucla_roms_blueprint_name, self.model_names)

    def ucla_roms_blueprint_df(self) -> pd.DataFrame:
        """Load all blueprints and return a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: model_name, grid_name, blueprint_name,
            description, start_time, end_time, blueprint_path, grid_yaml_path.
        """
        import pandas as pd

        records = []
        for bp_file in self._find_ucla_roms_blueprint_files():
            try:
                bp = self._load_ucla_roms_blueprint_yaml(bp_file)
                ucla_roms_blueprint_name = bp.get("name")
                if not ucla_roms_blueprint_name:
                    logger.warning("Skipping %s: missing 'name' field", bp_file)
                    continue
                model_name, grid_name = self._extract_model_and_grid_name(
                    ucla_roms_blueprint_name
                )
                if not model_name or not grid_name:
                    logger.warning(
                        "Skipping %s: could not parse model/grid from '%s'",
                        bp_file,
                        ucla_roms_blueprint_name,
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
                ucla_roms_blueprint_path_result: Path | str = (
                    self._to_raw_github_url(bp_file) if is_github else bp_file
                )
                records.append(
                    {
                        "model_name": model_name,
                        "grid_name": grid_name,
                        "blueprint_name": ucla_roms_blueprint_name,
                        "description": bp.get("description"),
                        "start_time": bp.get("valid_start_date"),
                        "end_time": bp.get("valid_end_date"),
                        "blueprint_path": ucla_roms_blueprint_path_result,
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
            f"models={self.model_names}, domains={self.domain_names})"
        )


class LayeredCatalog:
    """Facade over an ordered stack of :class:`DomainCatalog` stores.

    The stack is searched top-first: the first (index 0) store is the single
    writable layer new entries are registered into; every store beneath it
    is read-only and typically holds either the packaged catalog or a
    shared/site catalog. Duck-types ``DomainCatalog``'s public read surface
    plus its ``register_*`` mutators.

    Collision policy is deliberately hybrid: writers enforce stack-wide name
    uniqueness (a ``register_*`` call fails loudly if the name exists
    *anywhere* in the stack, not just the top), but collisions that arrive
    out-of-band -- a package upgrade or a shared-layer pull that happens to
    reuse a name -- are tolerated on read with deterministic top-first
    precedence, logged once at construction via ``logger.warning``.

    Parameters
    ----------
    stores : list of DomainCatalog
        Ordered top-to-bottom. ``stores[0]`` (``self.top``) must be a
        writable local store.
    """

    # Each spec kind's collision / ``entry_source`` key -> the catalog spec kind
    # (the store's registry key, and the name shown in messages).
    _SPEC_KIND: ClassVar[dict[str, CatalogSpecKind]] = {
        "model": "ModelSpec",
        "domain": "DomainSpec",
        "forcing": "ForcingSpec",
        "output": "OutputSpec",
        "cdr": "CdrSpec",
    }
    # Blueprint kinds are derived per application (see ``_blueprint_kind``), not
    # listed above, since the applications are open-ended.
    _BLUEPRINT_KIND_SUFFIX: ClassVar[str] = "_blueprint"

    def __init__(self, stores: list[DomainCatalog]) -> None:
        if not stores:
            raise ValueError("LayeredCatalog requires at least one store")
        top = stores[0]
        if not (top._is_local and not top.read_only):
            raise ValueError(
                "LayeredCatalog's top store must be a writable local catalog "
                f"(got catalog_root={top.catalog_root}, read_only={top.read_only})"
            )
        self.stores = stores
        self.top = top
        self._collisions = self._compute_collisions()
        for key, labels in self._collisions.items():
            logger.warning(
                "Catalog collision: %s present in multiple layers (%s); "
                "the topmost wins",
                key,
                ", ".join(labels),
            )

    # ------------------------------------------------------------------
    # Properties delegating to the top store
    # ------------------------------------------------------------------

    @property
    def catalog_root(self) -> Path:
        return self.top.catalog_root

    @property
    def _is_local(self) -> bool:
        return self.top._is_local

    @property
    def blueprints_dir(self) -> Path:
        return self.top.blueprints_dir

    def blueprint_dir(self, application: str) -> Path:
        return self.top.blueprint_dir(application)

    @property
    def workplans_dir(self) -> Path:
        return self.top.workplans_dir

    @property
    def read_only(self) -> bool:
        """Always False: a LayeredCatalog is only constructible with a writable top."""
        return False

    @property
    def label(self) -> str:
        return self.top.label

    # ------------------------------------------------------------------
    # Union name properties
    # ------------------------------------------------------------------

    @property
    def model_names(self) -> list[str]:
        return sorted({n for store in self.stores for n in store.model_names})

    @property
    def archived_model_names(self) -> list[str]:
        return sorted(
            n
            for n in self.model_names
            if n in self._store_for("model", n).archived_model_names
        )

    @property
    def domain_names(self) -> list[str]:
        return sorted({n for store in self.stores for n in store.domain_names})

    @property
    def forcing_names(self) -> list[str]:
        return sorted({n for store in self.stores for n in store.forcing_names})

    @property
    def output_names(self) -> list[str]:
        return sorted({n for store in self.stores for n in store.output_names})

    @property
    def cdr_names(self) -> list[str]:
        return sorted({n for store in self.stores for n in store.cdr_names})

    @property
    def blueprint_applications(self) -> list[str]:
        return sorted(
            {a for store in self.stores for a in store.blueprint_applications}
        )

    def blueprint_names(self, application: str) -> list[str]:
        return sorted(
            {n for store in self.stores for n in store.blueprint_names(application)}
        )

    @property
    def legacy_blueprints(self) -> list[tuple[str, str, Path]]:
        return [entry for store in self.stores for entry in store.legacy_blueprints]

    @property
    def legacy_specs(self) -> list[tuple[CatalogSpecKind, str, Path]]:
        return [entry for store in self.stores for entry in store.legacy_specs]

    @property
    def ucla_roms_blueprint_names(self) -> list[str]:
        """Return ``blueprint_names("ucla_roms")``."""
        return self.blueprint_names(_UCLA_ROMS)

    @property
    def forge_blueprint_names(self) -> list[str]:
        """Return ``blueprint_names("forge")``."""
        return self.blueprint_names(_FORGE)

    # ------------------------------------------------------------------
    # Top-first resolution
    # ------------------------------------------------------------------

    @classmethod
    def _blueprint_kind(cls, application: str) -> str:
        """Return the kind (collision / ``entry_source`` key) of *application*'s blueprints.

        An earlier name of an application gives its current name's kind.
        """
        return f"{canonical_application(application)}{cls._BLUEPRINT_KIND_SUFFIX}"

    @classmethod
    def _blueprint_application(cls, kind: str) -> str | None:
        """Inverse of :meth:`_blueprint_kind`; ``None`` if *kind* is not a blueprint kind.

        A kind built from an earlier name of an application gives the current name.
        """
        return (
            canonical_application(kind.removesuffix(cls._BLUEPRINT_KIND_SUFFIX))
            if kind.endswith(cls._BLUEPRINT_KIND_SUFFIX)
            else None
        )

    def _registry(self, store: DomainCatalog, kind: str) -> Mapping[str, Path]:
        """Return *store*'s ``name -> path`` registry for *kind*."""
        application = self._blueprint_application(kind)
        if application is not None:
            return store._blueprints.get(application, {})
        return store._specs[self._SPEC_KIND[kind]]

    def _kind_display(self, kind: str) -> str:
        application = self._blueprint_application(kind)
        if application is not None:
            return f"Blueprint ({application})"
        return self._SPEC_KIND[kind]

    def _store_for(self, kind: str, name: str) -> DomainCatalog:
        """Return the topmost store whose *kind* registry contains *name*."""
        for store in self.stores:
            if name in self._registry(store, kind):
                return store
        available = sorted(
            {n for store in self.stores for n in self._registry(store, kind)}
        )
        raise KeyError(
            f"{self._kind_display(kind)} '{name}' not found in any catalog layer. "
            f"Available: {available}"
        )

    def entry_source(self, kind: str, name: str) -> str:
        """Return the label of the winning (topmost) store containing *name*."""
        return self._store_for(kind, name).label

    def _compute_collisions(self) -> dict[str, list[str]]:
        kinds = [
            *self._SPEC_KIND,
            *(self._blueprint_kind(a) for a in self.blueprint_applications),
        ]
        collisions: dict[str, list[str]] = {}
        for kind in kinds:
            labels_by_name: dict[str, list[str]] = {}
            for store in self.stores:
                for name in self._registry(store, kind):
                    labels_by_name.setdefault(name, []).append(store.label)
            for name, labels in labels_by_name.items():
                if len(labels) > 1:
                    collisions[f"{kind}:{name}"] = labels
        return collisions

    def collisions(self) -> dict[str, list[str]]:
        """Return ``"kind:name"`` -> ordered (top-first) store labels for every
        name present in 2+ stores. For status display.
        """
        return dict(self._collisions)

    # ------------------------------------------------------------------
    # Keyed reads (top-first)
    # ------------------------------------------------------------------

    def model_path(self, model_name: str) -> Path:
        return self._store_for("model", model_name).model_path(model_name)

    def domain_path(self, domain_name: str) -> Path:
        return self._store_for("domain", domain_name).domain_path(domain_name)

    def blueprint_path(self, application: str, name: str) -> Path:
        return self._store_for(self._blueprint_kind(application), name).blueprint_path(
            application, name
        )

    def ucla_roms_blueprint_path(self, ucla_roms_blueprint_name: str) -> Path:
        """Return ``blueprint_path("ucla_roms", name)``."""
        return self.blueprint_path(_UCLA_ROMS, ucla_roms_blueprint_name)

    def forge_blueprint_path(self, forge_blueprint_name: str) -> Path:
        """Return ``blueprint_path("forge", name)``."""
        return self.blueprint_path(_FORGE, forge_blueprint_name)

    def model_data(self, model_name: str) -> dict:
        return self._store_for("model", model_name).model_data(model_name)

    def domain_data(self, domain_name: str) -> dict:
        return self._store_for("domain", domain_name).domain_data(domain_name)

    def forcing_data(self, forcing_name: str) -> dict:
        return self._store_for("forcing", forcing_name).forcing_data(forcing_name)

    def output_data(self, output_name: str) -> dict:
        return self._store_for("output", output_name).output_data(output_name)

    def cdr_data(self, cdr_name: str) -> dict:
        return self._store_for("cdr", cdr_name).cdr_data(cdr_name)

    def load_model_spec(self, model_name: str) -> Any:
        return self._store_for("model", model_name).load_model_spec(model_name)

    def domain(self, domain_id: str | int) -> dict:
        name = self.domain_names[domain_id] if isinstance(domain_id, int) else domain_id
        return self._store_for("domain", name).domain(name)

    def model(self, model_id: str | int) -> dict:
        name = self.model_names[model_id] if isinstance(model_id, int) else model_id
        return self._store_for("model", name).model(name)

    def ucla_roms_blueprint(self, ucla_roms_blueprint_id: str | int) -> Path:
        name = (
            self.ucla_roms_blueprint_names[ucla_roms_blueprint_id]
            if isinstance(ucla_roms_blueprint_id, int)
            else ucla_roms_blueprint_id
        )
        return self.ucla_roms_blueprint_path(name)

    # ------------------------------------------------------------------
    # Writers: stack-wide uniqueness, then delegate to the top store
    # ------------------------------------------------------------------

    def _check_unique(self, kind: str, name: str) -> None:
        for store in self.stores:
            if name in self._registry(store, kind):
                raise FileExistsError(
                    f"{self._kind_display(kind)} '{name}' already exists in the "
                    f"'{store.label}' catalog layer — pick a new name"
                )

    def register_domain_from_dict(self, name: str, domain_data: dict[str, Any]) -> None:
        self._check_unique("domain", name)
        self.top.register_domain_from_dict(name, domain_data)

    def register_output(
        self, name: str, output_settings: dict[str, Any], description: str = ""
    ) -> None:
        self._check_unique("output", name)
        self.top.register_output(name, output_settings, description)

    def register_forcing(
        self,
        name: str,
        forcing_inputs: dict[str, Any],
        description: str = "",
    ) -> None:
        self._check_unique("forcing", name)
        self.top.register_forcing(name, forcing_inputs, description=description)

    def register_cdr(
        self,
        name: str,
        *,
        description: str = "",
        mode: str,
        cdr_forcing: dict[str, Any] | None = None,
        cdr_forcing_file: dict[str, Any] | None = None,
    ) -> None:
        self._check_unique("cdr", name)
        self.top.register_cdr(
            name,
            description=description,
            mode=mode,
            cdr_forcing=cdr_forcing,
            cdr_forcing_file=cdr_forcing_file,
        )

    def register_model_from_settings(
        self,
        name: str,
        model_settings: dict[str, Any],
        base_model_path: Path | str,
        description: str = "",
        *,
        bgc_mode: BgcMode | None = None,
        use_pio: bool | None = None,
        roms_ref: str | None = None,
        marbl_ref: str | None = None,
    ) -> None:
        self._check_unique("model", name)
        self.top.register_model_from_settings(
            name,
            model_settings,
            base_model_path,
            description,
            bgc_mode=bgc_mode,
            use_pio=use_pio,
            roms_ref=roms_ref,
            marbl_ref=marbl_ref,
        )

    def register_model(self, model_path: Path | str) -> None:
        """Register a model spec file into the top store.

        The model name (``spec_entry_name`` of ``model_path``, mirroring
        ``DomainCatalog.register_model``) is checked for stack-wide uniqueness
        before delegating.
        """
        self._check_unique("model", spec_entry_name(Path(model_path).expanduser()))
        self.top.register_model(model_path)

    def register_domain(self, builder: Any) -> None:
        """Register a domain built by a ``ForgeExecutor``-like builder into the top store.

        The domain name (``builder.grid_name``, mirroring
        ``DomainCatalog.register_domain``) is checked for stack-wide
        uniqueness before delegating.
        """
        self._check_unique("domain", builder.grid_name)
        self.top.register_domain(builder)

    def _check_writable(self) -> None:
        """Delegate the write guard to the top store (always writable by
        construction; present so a LayeredCatalog can be the *target* of
        ``DomainCatalog.copy_domain``/``copy_model``).
        """
        self.top._check_writable()

    # ------------------------------------------------------------------
    # Copy / path helpers (top store)
    # ------------------------------------------------------------------

    def copy_domain(
        self, domain_name: str, catalog: DomainCatalog | LayeredCatalog
    ) -> None:
        """Copy a domain from whichever layer owns it into *catalog*.

        A ``LayeredCatalog`` target receives the copy in its writable top
        store, subject to that stack's stack-wide name uniqueness (same-name
        copies would deliberately shadow a lower layer, which writers reject).
        """
        src = self._store_for("domain", domain_name)
        if isinstance(catalog, LayeredCatalog):
            catalog._check_unique("domain", domain_name)
            src.copy_domain(domain_name, catalog.top)
        else:
            src.copy_domain(domain_name, catalog)

    def copy_model(
        self, model_name: str, catalog: DomainCatalog | LayeredCatalog
    ) -> None:
        """Copy a model from whichever layer owns it into *catalog* (see
        ``copy_domain`` for LayeredCatalog-target semantics).
        """
        src = self._store_for("model", model_name)
        if isinstance(catalog, LayeredCatalog):
            catalog._check_unique("model", model_name)
            src.copy_model(model_name, catalog.top)
        else:
            src.copy_model(model_name, catalog)

    # ------------------------------------------------------------------
    # Blueprint DataFrame / misc
    # ------------------------------------------------------------------
    # Union-semantics counterparts of DomainCatalog's private _find/_load/_extract
    # helpers, so a LayeredCatalog duck-types the single-store surface.

    def _find_ucla_roms_blueprint_files(self) -> list[Path]:
        files: list[Path] = []
        for store in self.stores:
            files.extend(store._find_ucla_roms_blueprint_files())
        return sorted(set(files))

    def _load_ucla_roms_blueprint_yaml(
        self, ucla_roms_blueprint_path: Path
    ) -> dict[str, Any]:
        for store in self.stores:
            if store._fs_exists(ucla_roms_blueprint_path):
                return store._load_ucla_roms_blueprint_yaml(ucla_roms_blueprint_path)
        # Not in any store: the top store's loader raises the usual
        # FileNotFoundError with its message.
        return self.top._load_ucla_roms_blueprint_yaml(ucla_roms_blueprint_path)

    def _load_grid_kwargs(self, grid_yaml_path: Path) -> dict[str, Any]:
        for store in self.stores:
            if store._fs_exists(grid_yaml_path):
                return store._load_grid_kwargs(grid_yaml_path)
        return self.top._load_grid_kwargs(grid_yaml_path)

    def _extract_model_and_grid_name(
        self, ucla_roms_blueprint_name: str
    ) -> tuple[str | None, str | None]:
        return _extract_model_and_grid(ucla_roms_blueprint_name, self.model_names)

    def ucla_roms_blueprint_df(self) -> pd.DataFrame:
        import pandas as pd

        frames = [store.ucla_roms_blueprint_df() for store in self.stores]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------
    # Emitted blueprints (read-only discovery)
    # ------------------------------------------------------------------

    async def blueprint_outputs(self, application: str) -> list[BlueprintOutput]:
        """Find the blueprints emitted by runs of this catalog's ``application`` entries.

        Read-only discovery: a standalone run is found through each entry's
        working directory, a workplan run through run tracking. Never writes to
        the catalog. See :func:`cstar.catalog.outputs.find_blueprint_outputs`.
        """
        # Deferred: ``import cstar.catalog`` must stay light and must not import
        # the applications, which ``cstar.catalog.outputs`` reaches.
        from cstar.catalog.outputs import find_blueprint_outputs

        return await find_blueprint_outputs(self, application)

    def tree(self) -> None:
        for store in self.stores:
            print(f"== {store.label} ({store.catalog_root}) ==")
            store.tree()

    def __repr__(self) -> str:
        stores_repr = ", ".join(f"{s.label}@{s.catalog_root}" for s in self.stores)
        return f"LayeredCatalog([{stores_repr}])"


def default_catalog_stack() -> LayeredCatalog:
    """Build the default layered catalog: the user layer over the packaged catalog.

    If ``CSTAR_CATALOG`` is set, it is split on ``os.pathsep`` into an
    ordered list of catalog roots; the first is the writable top (label
    ``"user"``), every subsequent entry is a read-only store (a literal
    ``"local"`` entry resolves to the packaged catalog via
    ``DomainCatalog``'s own constructor logic, and is only valid after the
    first position -- the bundled catalog is never writable). Otherwise the
    top store is
    ``user_catalog_root()`` (``~/cstar/catalog`` by default, may
    not exist yet -- constructing it here creates nothing).

    The packaged catalog is always appended at the bottom (label
    ``"bundled"``), unless a store with the same resolved root is already in
    the stack.
    """
    entries = _env_catalog_entries() or [str(user_catalog_root())]
    return build_catalog_stack(entries)


def build_catalog_stack(entries: list[str]) -> LayeredCatalog:
    """Build a :class:`LayeredCatalog` from ordered catalog-root entries (top first).

    The first entry becomes the writable top store (label ``"user"``); every
    subsequent entry is read-only. The packaged catalog is appended at the
    bottom (label ``"bundled"``) unless a store with the same resolved root is
    already present. The single builder behind ``default_catalog_stack`` (env
    var) and the wizard's catalog bar, so the two cannot drift.

    Raises ``ValueError`` if the first entry resolves to the bundled catalog
    (which is never writable) or is otherwise not a writable local store.
    """
    if not entries:
        raise ValueError("build_catalog_stack requires at least one catalog root")
    bundled_root = _DEFAULT_CATALOG_ROOT.resolve()

    stores: list[DomainCatalog] = []
    for i, entry in enumerate(entries):
        is_top = i == 0
        kwargs: dict[str, Any] = dict(
            catalog_root=entry, suppress_validation=True, read_only=not is_top
        )
        if is_top:
            kwargs["label"] = "user"
        stores.append(DomainCatalog(**kwargs))

    top = stores[0]
    if top._is_local and top.catalog_root.resolve() == bundled_root:
        raise ValueError(
            "The bundled catalog ('local') is read-only and cannot be the writable "
            "top layer of a catalog stack; list it after your own catalog root "
            "instead (e.g. '/path/to/mine:local')."
        )

    already_bundled = any(
        store._is_local and store.catalog_root.resolve() == bundled_root
        for store in stores
    )
    if not already_bundled:
        stores.append(DomainCatalog(read_only=True))

    return LayeredCatalog(stores)


_default_catalog: LayeredCatalog | None = None


def __getattr__(name: str) -> Any:
    """Lazily construct the module-level ``default_catalog`` singleton (PEP 562).

    ``from cstar.catalog.domain_catalog import default_catalog`` still triggers
    construction at import of the *importing* module -- this only protects
    ``import cstar.catalog`` itself from an eager filesystem scan.
    """
    global _default_catalog
    if name == "default_catalog":
        if _default_catalog is None:
            _default_catalog = default_catalog_stack()
        return _default_catalog
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
