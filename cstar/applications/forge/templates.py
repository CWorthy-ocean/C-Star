"""Single source of the mapping from a ModelSpec/blueprint template ``directory``
(e.g. ``templates/compile-time``, written relative to the standalone cstar-forge
repo root that ``resolve.DEFAULT_TEMPLATE_REPO`` still serves from) onto the
bundled copy shipped in this C-Star build
(``cstar/additional_files/templates/forge/<stage>``), plus content hashing of
that bundled copy.

Used by the two consumers that must never duplicate this mapping: ``models.py``
(``ModelSpec._validate_template_files_exist``, a dev-time existence check) and
``executor.py`` (the local fast path, the staging cache, and fetched-copy
verification in ``ForgeExecutor._stage_templates``). The hashes a blueprint
carries (``TemplateRepo.file_hashes``) are authored in each ModelSpec for its
pinned ``templates_commit`` and copied verbatim by ``resolve.py`` -- never
computed here.
"""

import hashlib
import os
import shutil
import tempfile
from collections.abc import Iterable
from importlib.resources import files
from pathlib import Path

_BUNDLED_TEMPLATES_ROOT = (
    Path(str(files("cstar"))) / "additional_files" / "templates" / "forge"
)


BUNDLED_TEMPLATES_DIRECTORY = "cstar/additional_files/templates/forge"
"""Repo-relative ``directory`` prefix the bundled ModelSpecs pin (a stage is
``<prefix>/compile-time`` or ``<prefix>/run-time``)."""

_LEGACY_PREFIX = ("templates",)
"""Directory prefix used by blueprints and ModelSpecs written against the
standalone cstar-forge repository, where the templates lived at ``templates/``."""


def bundled_template_dir(directory: str | None) -> Path | None:
    """Map a template stage's ``directory`` onto its bundled copy.

    Parameters
    ----------
    directory : str | None
        A ``code.templates_*.directory`` value, written relative to the root of
        the repository that serves the templates: either the current form,
        ``cstar/additional_files/templates/forge/<stage>``, or the legacy form
        from the standalone cstar-forge repository, ``templates/<stage>``. The
        repository prefix is stripped and the stage resolves under the bundled
        root.

    Returns
    -------
    Path | None
        The bundled directory (e.g.
        ``cstar/additional_files/templates/forge/compile-time``), or ``None`` if
        ``directory`` is falsy or the mapped directory doesn't exist (e.g. an
        installed-package-only environment, or a stage this C-Star build doesn't
        ship).
    """
    if not directory:
        return None
    parts = Path(directory).parts
    bundled_prefix = Path(BUNDLED_TEMPLATES_DIRECTORY).parts
    if parts[: len(bundled_prefix)] == bundled_prefix:
        parts = parts[len(bundled_prefix) :]
    elif parts[: len(_LEGACY_PREFIX)] == _LEGACY_PREFIX:
        parts = parts[len(_LEGACY_PREFIX) :]
    template_dir = _BUNDLED_TEMPLATES_ROOT.joinpath(*parts)
    return template_dir if template_dir.exists() else None


def hash_template_files(directory: Path, files: Iterable[str]) -> dict[str, str]:
    """sha256 hex digest of each of ``files`` under ``directory``, keyed by filename.

    Parameters
    ----------
    directory : Path
        Directory the files live in flat (e.g. a ``bundled_template_dir()``
        result, or a staged-from-git destination).
    files : Iterable[str]
        Filenames (no subdirectories) to hash.

    Returns
    -------
    dict[str, str]
        ``{filename: sha256_hexdigest}``.

    Raises
    ------
    FileNotFoundError
        If any of ``files`` doesn't exist under ``directory`` -- names every
        missing file.
    """
    files = list(files)
    missing = [f for f in files if not (directory / f).is_file()]
    if missing:
        raise FileNotFoundError(
            f"Template files missing from {directory}: {sorted(missing)}"
        )
    return {f: hashlib.sha256((directory / f).read_bytes()).hexdigest() for f in files}


def copy_template_files(src_dir: Path, dest_dir: Path, files: Iterable[str]) -> None:
    """Copy each of ``files`` flat from ``src_dir`` into ``dest_dir``.

    ``dest_dir`` is created (with parents) if it doesn't already exist. The one
    place ``_stage_templates``'s three copy paths -- the bundled fast path, a
    staging-cache hit, and populating the staging cache -- share, so a change to
    how templates are copied only has to happen once.

    Each file is written to a uniquely-named temp file in ``dest_dir`` and
    ``os.replace``'d into place, rather than copied directly onto the final
    name: the staging cache (``ForgeExecutor._template_cache_dir``) can be
    populated and read concurrently by more than one run pinned at the same
    commit, and this keeps a reader from ever seeing a partially-written file
    instead of a clean miss-then-hit.

    Parameters
    ----------
    src_dir : Path
        Directory the files live in flat.
    dest_dir : Path
        Directory to copy the files into.
    files : Iterable[str]
        Filenames (no subdirectories) to copy.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        fd, tmp_name = tempfile.mkstemp(dir=dest_dir, prefix=f".{f}.", suffix=".tmp")
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            shutil.copy2(src_dir / f, tmp_path)
            os.replace(tmp_path, dest_dir / f)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise


def template_cache_key(location: str, commit: str, directory: str | None) -> str:
    """Filesystem-safe cache key for a fetched ``(location, commit, directory)``
    template pin.

    Used by ``executor.py``'s staging cache (``ForgeExecutor._stage_templates``):
    a commit pin's fetched, hash-verified files are cached under this key so a
    later run pinned at the same commit can copy from the cache instead of
    re-fetching. Only ever called for a commit pin -- a branch pin's content can
    move without the blueprint changing, so ``_stage_templates`` never caches one.

    Parameters
    ----------
    location : str
        The template repository's git location.
    commit : str
        The pinned commit.
    directory : str | None
        The stage's in-repo directory (``code.templates_*.directory``).

    Returns
    -------
    str
        A sha256 hex digest of the triple -- content-addressed and safe as a
        directory name regardless of how ``location``/``directory`` are spelled.
    """
    payload = f"{location}\n{commit}\n{directory or ''}"
    return hashlib.sha256(payload.encode()).hexdigest()
