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
attributes), every small variable in full and a bounded sample of rows of each
larger one (:func:`hash_netcdf_contents`), not file bytes, so it survives benign
re-encoding (``nccopy`` to a different format/chunking, NETCDF4 <-> classic)
that changes the bytes on disk without changing the data, and costs a few MB of
I/O per variable however large the file. A digest carries a scheme prefix so the verifier
recomputes the same scheme. :func:`stage_user_netcdf` then copies the verified file into
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

from cstar.base.utils import convert_to_cdf5, netcdf_format

if TYPE_CHECKING:
    import numpy as np
    import xarray as xr

    from cstar.applications.forge.blueprint import UserProvidedFile

logger = logging.getLogger(__name__)


HASH_SCHEME = "nc-sample-v1"
"""Prefix of every digest :func:`hash_netcdf_contents` returns."""

FULL_HASH_LIMIT = 1 << 20
"""Bytes at or below which a variable is hashed in full."""

SAMPLE_BUDGET = 2 << 20
"""Most bytes read from any one larger variable."""

SAMPLE_ROWS = 64
"""Most rows sampled from any one larger variable."""


def short_digest(content_hash: str) -> str:
    """The first 12 hex characters of a ``content_hash``, scheme tag stripped, for
    log and status messages (an untagged legacy digest is sliced as is).
    """
    return content_hash.partition(":")[2][:12] or content_hash[:12]


def _array_bytes(values: np.ndarray) -> bytes:
    r"""Raw bytes of ``values``: C-order bytes for numeric dtypes; for
    object/string/bytes dtypes each element UTF-8-encoded and joined with
    ``b"\x00"`` (numpy has no stable raw byte layout for ``object`` arrays).
    """
    import numpy as np

    if values.dtype.kind in "OUS":
        flat = np.asarray(values).ravel()
        encoded = [
            (x if isinstance(x, bytes) else str(x).encode()) for x in flat.tolist()
        ]
        return b"\x00".join(encoded)
    return np.ascontiguousarray(values).tobytes()


def _sampled_bytes(var: xr.Variable) -> bytes:
    """Bytes of an evenly spaced sample of ``var``'s rows (full innermost-axis
    vectors), read lazily so at most about ``SAMPLE_BUDGET`` bytes come off disk.

    The sampled row indices are folded in ahead of the data. The first row (record
    0) is always sampled, and the last whenever more than one row is. When the
    sampled rows would not fit the budget, each contributes only its first and
    last ``SAMPLE_BUDGET // (2 * k * itemsize)`` elements (``k`` rows sampled).
    """
    import numpy as np

    *lead, row_len = var.shape
    n_rows = int(np.prod(lead))
    itemsize = var.dtype.itemsize
    row_bytes = max(1, row_len * itemsize)
    k = max(min(n_rows, 2), min(SAMPLE_ROWS, SAMPLE_BUDGET // row_bytes))
    rows = np.unique(np.round(np.linspace(0, n_rows - 1, k)).astype(int))

    parts = [repr(tuple(rows.tolist())).encode()]
    for i in rows.tolist():
        prefix = np.unravel_index(i, lead) if lead else ()
        if k * row_len * itemsize > SAMPLE_BUDGET:
            half = SAMPLE_BUDGET // (2 * k * itemsize)
            spans = [slice(0, half), slice(row_len - half, row_len)]
        else:
            spans = [slice(None)]
        parts.extend(_array_bytes(var[(*prefix, span)].values) for span in spans)
    return b"".join(parts)


def hash_netcdf_contents(path: str | Path) -> str:
    r"""Scheme-prefixed sha256 digest (``nc-sample-v1:<hex>``) of a netCDF file's
    structure and data, independent of its on-disk encoding (format, chunking,
    compression) and costing a bounded read however large the file.

    Opens with ``decode_cf=False, decode_times=False`` so raw stored values are
    hashed (no CF-decoding drift between roms-tools/xarray versions) and with
    ``create_default_indexes=False`` so dimension coordinates stay lazy too (an
    index would read them whole at open). Variable-length string variables are
    the one exception to the bound: the netCDF4 backend reads them whole to
    infer their dtype. The digest folds in, in this order:

    1. the sorted global attributes;
    2. every variable in ``ds.variables``, sorted by name, each contributing its
       name, dim-name tuple, shape, dtype string, value bytes, and sorted
       variable attributes.

    Value bytes: a variable of at most ``FULL_HASH_LIMIT`` bytes is hashed in full;
    a larger one contributes a sample of up to ``SAMPLE_ROWS`` evenly spaced rows
    (a row is one innermost-axis vector, always including the first and last
    record) totalling at most ``SAMPLE_BUDGET`` bytes, preceded by the row indices
    (see :func:`_sampled_bytes`). Either way ndarray -> bytes is
    :func:`_array_bytes`: numeric dtypes as raw C-order bytes, object/string/bytes
    dtypes UTF-8-encoded and joined with ``b"\\x00"``.

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

    with xr.open_dataset(
        path, decode_cf=False, decode_times=False, create_default_indexes=False
    ) as ds:
        hasher = hashlib.sha256()
        hasher.update(_attr_bytes(ds.attrs))
        for name in sorted(ds.variables):
            var = ds.variables[name]
            hasher.update(b"||VAR||")
            hasher.update(name.encode())
            hasher.update(repr(tuple(var.dims)).encode())
            hasher.update(repr(tuple(var.shape)).encode())
            hasher.update(str(var.dtype).encode())
            if var.size * var.dtype.itemsize <= FULL_HASH_LIMIT:
                hasher.update(_array_bytes(var.values))
            else:
                hasher.update(_sampled_bytes(var))
            hasher.update(_attr_bytes(var.attrs))

    return f"{HASH_SCHEME}:{hasher.hexdigest()}"


def verify_user_file(f: UserProvidedFile, label: str) -> Path:
    """Resolve ``f.location`` and verify it against ``f.content_hash``.

    Raises ``FileNotFoundError`` (hard error -- there is nothing to fall back to)
    if the resolved path does not exist, naming ``label`` and the path so the
    user knows exactly which input is missing and that it must be present at
    this exact path on the machine running the executor.

    A ``content_hash`` whose scheme prefix is not :data:`HASH_SCHEME` cannot be
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
