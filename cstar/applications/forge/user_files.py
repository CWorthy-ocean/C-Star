"""
User-provided-netCDF contract: hashing, verification, and staging for pre-made
grid / river / CDR-forcing files a user supplies instead of letting Forge
generate them.

A :class:`~cstar.applications.forge.blueprint.UserProvidedFile` records two
things about such a file: ``location`` (a path on the machine that will run
the executor -- host/transport, excluded from ``ForgeBlueprint.content_hash``)
and ``content_hash`` (a digest of the file's structure and data, pinned into the
blueprint at authoring time). At processing time the executor:

1. **verifies existence** (:func:`verify_user_file` raises ``FileNotFoundError``
   if the file is missing -- a hard error, since there is nothing to fall back
   to); and
2. **verifies the hash** (a mismatch is a warning, not an error -- the file may
   have been legitimately regenerated/re-encoded upstream; processing proceeds
   with whatever is on disk).

The hash is computed over the file's structure (dims, variables, dtypes,
attributes), every small variable in full and the first and last megabyte of each
larger one in two contiguous reads
(:func:`~cstar.base.digest.hash_netcdf_contents`), not file bytes, so it survives
benign re-encoding (``nccopy`` to a different format/chunking, NETCDF4 <-> classic) that changes the bytes on disk without changing the data,
and costs two reads per variable however large the file. A digest carries a
scheme prefix so the verifier recomputes the same scheme.
:func:`stage_user_netcdf` then copies the verified file into the executor's
working tree, mirroring how generated inputs land there; under ParallelIO a
non-classic (NETCDF4) file is converted to CDF-5 on the copy.

Module top stays light (xarray/numpy are imported inside the functions that
need them; only stdlib and ``cstar.base`` are imported eagerly) -- this
module is imported by the lightweight authoring/wizard layer, where import cost
matters.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

from cstar.base.digest import HASH_SCHEME, hash_netcdf_contents, short_digest
from cstar.base.utils import convert_to_cdf5, netcdf_format

if TYPE_CHECKING:
    from cstar.applications.forge.blueprint import UserProvidedFile

logger = logging.getLogger(__name__)


def verify_user_file(f: UserProvidedFile, label: str) -> Path:
    """Resolve ``f.location`` and verify it against ``f.content_hash``.

    Raises ``FileNotFoundError`` (hard error -- there is nothing to fall back to)
    if the resolved path does not exist, naming ``label`` and the path so the
    user knows exactly which input is missing and that it must be present at
    this exact path on the machine running the executor.

    A ``content_hash`` whose scheme prefix is not :data:`~cstar.base.digest.HASH_SCHEME` cannot be
    recomputed here -- no prefix is the full-content digest of an earlier C-Star,
    another prefix a scheme this C-Star does not know -- so it is logged as
    unverified and the file is accepted.

    On a hash mismatch (the file no longer matches what was
    recorded in the blueprint), emits both a ``logger.warning`` and a
    ``warnings.warn`` -- a warning, not an error, since the file may have been
    legitimately regenerated upstream; processing proceeds with what's on disk.

    Returns the resolved ``Path`` in all non-missing cases.
    """
    resolved = Path(f.location).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(
            f"{label}: user-provided file not found at {resolved} -- it must be "
            "present at this exact path on the machine running the executor."
        )

    scheme, _, _ = f.content_hash.partition(":")
    if scheme != HASH_SCHEME:
        recorded_with = (
            f"hash scheme {scheme!r}"
            if scheme
            else "the full-content hash scheme of an earlier C-Star"
        )
        logger.warning(
            f"{label}: user-provided file at {resolved} was recorded with "
            f"{recorded_with} and is not verified (this C-Star computes "
            f"{HASH_SCHEME}); re-author the blueprint to refresh it."
        )
        return resolved

    actual = hash_netcdf_contents(resolved)
    if actual != f.content_hash:
        message = (
            f"{label}: user-provided file at {resolved} has changed since it was "
            f"recorded in the blueprint (recorded {short_digest(f.content_hash)}…, "
            f"computed {short_digest(actual)}…). Processing will continue with the "
            "file as found."
        )
        logger.warning(message)
        warnings.warn(message, UserWarning, stacklevel=2)

    return resolved


def stage_user_netcdf(src: Path, dest: Path, *, use_pio: bool, label: str) -> Path:
    """Place a verified user file at its executor working-tree destination.

    Detects ``src``'s format. If ``src`` is already ``dest`` (e.g. authored
    directly into the input directory) nothing is copied, but under ``use_pio`` a
    non-classic file raises ``RuntimeError`` since it cannot be converted in place.
    Otherwise creates ``dest``'s parent directories and, when ``use_pio`` and
    ``src`` is not classic-format (CDF-1/2/5), warns and converts a copy with
    ``cstar.base.utils.convert_to_cdf5`` (keeping the original); classic files,
    and every file without ``use_pio``, are plain ``shutil.copy2`` copies. A
    failed conversion (non-zero ``nccopy`` exit, or ``nccopy`` missing) raises
    ``RuntimeError``; ``convert_to_cdf5`` has already removed any partial
    ``dest``. Returns ``dest``.
    """
    fmt = netcdf_format(src)
    needs_conversion = use_pio and not fmt.is_classic

    if src.resolve() == dest.resolve():
        if needs_conversion:
            raise RuntimeError(
                f"{label}: user-provided file {src} is already at its destination "
                f"but is {fmt}; ParallelIO requires classic-format (CDF-5) input and "
                "it cannot be converted in place without overwriting the original. "
                f"Convert it yourself (nccopy -k cdf5 in out) or point {label} at a "
                "file outside the input directory."
            )
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    if needs_conversion:
        logger.warning(
            f"{label}: user-provided file {src} is {fmt}; ParallelIO requires "
            f"classic-format input, so a copy is being converted to CDF-5 at {dest} "
            "(the original is left untouched)."
        )
        try:
            convert_to_cdf5(src, dest, remove_source=False)
        except (subprocess.CalledProcessError, OSError) as exc:
            hint = (
                " `nccopy` (from netcdf-c) must be on PATH."
                if isinstance(exc, OSError)
                else ""
            )
            raise RuntimeError(
                f"{label}: converting user-provided file {src} to CDF-5 at {dest} "
                f"failed: {exc}.{hint}"
            ) from exc
    else:
        shutil.copy2(src, dest)
    return dest
