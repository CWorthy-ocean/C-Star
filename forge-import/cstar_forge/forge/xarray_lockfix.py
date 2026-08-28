"""Workaround for an xarray lock-leak bug that deadlocks dask-threaded saves.

Bug (present in xarray 2025.7.1 and still on xarray main as of 2026-08-28):

- ``CachingFileManager.__del__`` (``xarray/backends/file_manager.py``) calls
  ``acquire(self._lock, blocking=False)`` when a file manager is
  garbage-collected while its file is still in the global file cache.
- For netCDF4 stores that lock is a ``CombinedLock``, whose non-blocking
  acquire is ``all(acquire(lock, blocking=False) for lock in self.locks)``
  (``xarray/backends/locks.py``). ``all()`` short-circuits on the first busy
  constituent and returns False **without releasing the constituents it
  already acquired** -- and ``__del__`` skips its release on False, so a
  *global* lock (``NETCDFC_LOCK``/``HDF5_LOCK``) is orphaned forever.
- The race window is intrinsic: ``CombinedLock.__exit__`` releases
  constituents in forward order, so an earlier constituent is briefly free
  while a later one is still held. A GC pass (any thread, any allocation)
  collecting a dropped manager in that window leaks the early constituent.

Once leaked, every netCDF4 read and write blocks forever at
``CombinedLock.__enter__``: the forge input-generation stall (dask progress
bar frozen at a fixed percentage during ``save_mfdataset``, all worker
threads parked on the lock, no thread holding it).

The patch below makes ``CombinedLock.acquire`` all-or-nothing: on a failed
non-blocking constituent acquire it releases the constituents already taken
before returning False. The blocking path is behaviorally unchanged
(blocking acquires cannot return False). Remove once an xarray release
carries the upstream fix.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

_PATCHED_FLAG = "_cstar_forge_all_or_nothing_acquire"


def apply_combinedlock_leak_fix() -> bool:
    """Patch ``xarray.backends.locks.CombinedLock.acquire`` to be all-or-nothing.

    Idempotent, and defensive against xarray internals moving: if the
    expected attributes are missing the patch is skipped with a warning
    (the bug may equally be gone in that version). Returns True when the
    patch is in place (already applied counts), False when skipped.
    """
    try:
        from xarray.backends import locks as xr_locks

        combined_lock = xr_locks.CombinedLock
        acquire_one = xr_locks.acquire
    except (ImportError, AttributeError):  # pragma: no cover - future xarray
        log.warning(
            "xarray internals changed; skipping CombinedLock leak fix "
            "(verify the partial-acquire bug is fixed in this xarray version)"
        )
        return False

    if getattr(combined_lock, _PATCHED_FLAG, False):
        return True

    def _acquire_all_or_nothing(self, blocking=True):
        acquired = []
        for lock in self.locks:
            if not acquire_one(lock, blocking=blocking):
                for held in reversed(acquired):
                    held.release()
                return False
            acquired.append(lock)
        return True

    combined_lock.acquire = _acquire_all_or_nothing
    setattr(combined_lock, _PATCHED_FLAG, True)
    return True
