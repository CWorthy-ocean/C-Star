"""
Content digests of run products and user-supplied files: ``<scheme>:<hex>``.

Two schemes: :data:`HASH_SCHEME` (``nc-sample-v2``), a bounded-read digest of a
netCDF file's structure and data that survives re-encoding, and
:data:`SHA256_SCHEME` (``sha256``), the sha256 of a file's bytes. The prefix lets a
verifier recompute the scheme a digest was recorded with.

Module top stays light (xarray/numpy are imported inside the functions that need
them), since the authoring/wizard layer imports it.
"""

from __future__ import annotations

import hashlib
import typing as t
from pathlib import Path

from cstar.base.utils import _get_sha256_hash

if t.TYPE_CHECKING:
    import numpy as np
    import xarray as xr

SHA256_SCHEME: t.Final[str] = "sha256"
"""Prefix of every digest :func:`sha256_digest` returns."""


def sha256_digest(path: str | Path) -> str:
    """Scheme-prefixed sha256 digest (``sha256:<hex>``) of a file's bytes.

    Reads the whole file: for logs, YAML and plots, not for model output.
    """
    return f"{SHA256_SCHEME}:{_get_sha256_hash(Path(path))}"


HASH_SCHEME = "nc-sample-v2"
"""Prefix of every digest :func:`hash_netcdf_contents` returns."""

KNOWN_SCHEMES: t.Final[tuple[str, ...]] = (SHA256_SCHEME, HASH_SCHEME)
"""The schemes this build computes."""

FULL_HASH_LIMIT = 1 << 20
"""Bytes at or below which a variable is hashed in full."""

SAMPLE_BUDGET = 2 << 20
"""Most bytes read from any one larger variable."""


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
    """Bytes of ``var``'s leading and trailing slabs, two contiguous reads.

    Each slab is one hyperslab contiguous in C order holding at most
    ``SAMPLE_BUDGET // (2 * itemsize)`` elements: whole slices of the first axis
    ``j`` whose trailing axes fit that count, taken from the start (index 0 of the
    axes before ``j``) and from the end (their last index). A variable no larger
    than ``SAMPLE_BUDGET`` is read whole instead. The slab indices are folded in
    ahead of the data, so the first and last elements are always covered.
    """
    import numpy as np

    shape = var.shape
    itemsize = var.dtype.itemsize
    if var.size * itemsize <= SAMPLE_BUDGET:
        return _array_bytes(var.values)

    m = max(1, SAMPLE_BUDGET // (2 * itemsize))  # an element may exceed the budget
    j = next(j for j in range(len(shape)) if int(np.prod(shape[j + 1 :])) <= m)
    r = max(1, min(shape[j], m // int(np.prod(shape[j + 1 :]))))
    rest = (slice(None),) * (len(shape) - j - 1)
    leading = (0,) * j + (slice(0, r),) + rest
    tail = slice(shape[j] - r, shape[j])
    trailing = (*(n - 1 for n in shape[:j]), tail, *rest)

    spans = [
        tuple((i.start, i.stop) if isinstance(i, slice) else i for i in idx)
        for idx in (leading, trailing)
    ]
    parts = [repr(spans).encode()]
    parts.extend(_array_bytes(var[idx].values) for idx in (leading, trailing))
    return b"|".join(parts)


def hash_netcdf_contents(path: str | Path) -> str:
    r"""Scheme-prefixed sha256 digest (``nc-sample-v2:<hex>``) of a netCDF file's
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
    a larger one contributes its first and last contiguous slab of at most
    ``SAMPLE_BUDGET // 2`` bytes each, two reads that always cover the first and
    last element, preceded by the slab indices (one whole read when it fits
    ``SAMPLE_BUDGET``; see :func:`_sampled_bytes`). An edit confined to the
    interior of a large variable therefore leaves the digest unchanged: the
    digest identifies the file cheaply, it does not certify every value (a
    mismatch is only a warning at staging). Either way ndarray -> bytes is
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
