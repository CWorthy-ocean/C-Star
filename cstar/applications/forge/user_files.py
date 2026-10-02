"""
User-provided-netCDF contract: hashing, verification, and staging for pre-made
grid / river / CDR-forcing files a user supplies instead of letting Forge
generate them.

A :class:`~cstar.applications.forge.blueprint.UserProvidedFile` records two
things about such a file: ``location`` (a path on the machine that will run
the executor -- host/transport, excluded from ``ForgeBlueprint.content_hash``)
and ``content_hash`` (a digest of the file's *data content*, pinned into the
blueprint at authoring time). At processing time the executor:

1. **verifies existence** (:func:`verify_user_file` raises ``FileNotFoundError``
   if the file is missing -- a hard error, since there is nothing to fall back
   to); and
2. **verifies the hash** (a mismatch is a warning, not an error -- the file may
   have been legitimately regenerated/re-encoded upstream; processing proceeds
   with whatever is on disk).

The hash is computed over decoded netCDF *values* (:func:`hash_netcdf_contents`),
not file bytes, so it survives benign re-encoding (``nccopy`` to a different
format/chunking, NETCDF4 <-> classic) that changes the bytes on disk without
changing the data. :func:`stage_user_netcdf` then copies the verified file into
the executor's working tree, mirroring how generated inputs land there; under
ParallelIO a non-classic (NETCDF4) file is converted to CDF-5 on the copy.

Module top stays light (xarray/numpy are imported inside the functions that
need them; only stdlib and ``cstar.base.utils`` are imported eagerly) -- this
module is imported by the lightweight authoring/wizard layer, where import cost
matters.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import subprocess
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

from cstar.base.utils import netcdf_format

if TYPE_CHECKING:
    from cstar.applications.forge.blueprint import UserProvidedFile

logger = logging.getLogger(__name__)


def hash_netcdf_contents(path: str | Path) -> str:
    r"""sha256 hex digest of a netCDF file's *data content*, independent of its
    on-disk encoding (format, chunking, compression).

    Opens with ``decode_cf=False, decode_times=False`` so raw stored values are
    hashed (no CF-decoding drift between roms-tools/xarray versions), loads them
    into memory, then closes the file. The digest folds in, in this order:

    1. the sorted global attributes;
    2. every variable in ``ds.variables``, sorted by name, each contributing its
       name, dim-name tuple, shape, dtype string, raw value bytes, and sorted
       variable attributes.

    Value bytes: for numeric dtypes, ``np.ascontiguousarray(var.values).tobytes()``;
    for object/string/bytes dtypes (e.g. a ``river_name`` string coordinate),
    each element UTF-8-encoded and joined with a ``b"\\x00"`` separator (numpy has
    no stable raw byte layout for ``object`` arrays).

    Attribute values may be numpy scalars/arrays; each is normalized via
    ``np.asarray(v).tolist()`` then ``repr(...)`` before encoding, so the digest
    doesn't depend on numpy's own attribute dtype. Every field is fed through one
    ``hashlib.sha256`` with unambiguous ``b"|"``/``b"\\x00"`` separators between
    fields, so no encoding of two adjacent fields collides.
    """
    import numpy as np
    import xarray as xr

    def _attr_bytes(attrs: dict) -> bytes:
        parts = []
        for key in sorted(attrs):
            value = attrs[key]
            normalized = repr(np.asarray(value).tolist())
            parts.append(f"{key}={normalized}".encode())
        return b"|".join(parts)

    def _value_bytes(var: xr.Variable) -> bytes:
        values = var.values
        if values.dtype.kind in "OUS":
            flat = np.asarray(values).ravel()
            encoded = [
                (x if isinstance(x, bytes) else str(x).encode()) for x in flat.tolist()
            ]
            return b"\x00".join(encoded)
        return np.ascontiguousarray(values).tobytes()

    with xr.open_dataset(path, decode_cf=False, decode_times=False) as ds:
        ds.load()
        hasher = hashlib.sha256()
        hasher.update(_attr_bytes(ds.attrs))
        for name in sorted(ds.variables):
            var = ds.variables[name]
            hasher.update(b"||VAR||")
            hasher.update(name.encode())
            hasher.update(repr(tuple(var.dims)).encode())
            hasher.update(repr(tuple(var.shape)).encode())
            hasher.update(str(var.dtype).encode())
            hasher.update(_value_bytes(var))
            hasher.update(_attr_bytes(var.attrs))

    return hasher.hexdigest()


def verify_user_file(f: UserProvidedFile, label: str) -> Path:
    """Resolve ``f.location`` and verify it against ``f.content_hash``.

    Raises ``FileNotFoundError`` (hard error -- there is nothing to fall back to)
    if the resolved path does not exist, naming ``label`` and the path so the
    user knows exactly which input is missing and that it must be present at
    this exact path on the machine running the executor.

    On a hash mismatch (the file's data content no longer matches what was
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

    actual = hash_netcdf_contents(resolved)
    if actual != f.content_hash:
        message = (
            f"{label}: user-provided file at {resolved} has changed since it was "
            f"recorded in the blueprint (recorded {f.content_hash[:12]}…, computed "
            f"{actual[:12]}…). Processing will continue with the file as found."
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
    ``nccopy -k cdf5`` (mirrors ``RomsMarblInputData._pio_finalize``'s conversion);
    classic files, and every file without ``use_pio``, are plain ``shutil.copy2``
    copies. A failed conversion (non-zero ``nccopy`` exit, or ``nccopy`` missing)
    removes the partial ``dest`` and raises ``RuntimeError``. Returns ``dest``.
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
        converted = False
        try:
            subprocess.run(["nccopy", "-k", "cdf5", str(src), str(dest)], check=True)
            converted = True
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
        finally:
            if not converted:
                # a partial dest (failure OR interrupt) would otherwise be reused by
                # the executor's no-clobber logic on the next run
                dest.unlink(missing_ok=True)
    else:
        shutil.copy2(src, dest)
    return dest
