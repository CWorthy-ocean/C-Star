"""Single source of the mapping from a ModelSpec/blueprint template ``directory``
(e.g. ``templates/compile-time``, written relative to the standalone cstar-forge
repo root that ``resolve.DEFAULT_TEMPLATE_REPO`` still serves from) onto the
bundled copy shipped in this C-Star build
(``cstar/additional_files/templates/forge/<stage>``), plus content hashing of
that bundled copy.

Used by the two consumers that must never duplicate this mapping: ``models.py``
(``ModelSpec._validate_template_files_exist``, a dev-time existence check) and
``executor.py`` (the local fast path and fetched-copy verification in
``ForgeExecutor._stage_templates``). The hashes a blueprint carries
(``TemplateRepo.file_hashes``) are authored in each ModelSpec for its pinned
``templates_commit`` and copied verbatim by ``resolve.py`` -- never computed here.
"""

import hashlib
from collections.abc import Iterable
from pathlib import Path

_BUNDLED_TEMPLATES_ROOT = (
    Path(__file__).resolve().parents[2] / "additional_files" / "templates" / "forge"
)


def bundled_template_dir(directory: str | None) -> Path | None:
    """Map a template stage's ``directory`` onto its bundled copy.

    Parameters
    ----------
    directory : str | None
        A ``code.templates_*.directory`` value (e.g. ``templates/compile-time``).
        The leading ``templates`` path segment -- an artifact of the standalone
        cstar-forge repo layout that ``directory`` is still written relative to
        -- is stripped so the remainder (e.g. ``compile-time``) resolves under
        the bundled root instead.

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
    if parts and parts[0] == "templates":
        parts = parts[1:]
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
