"""
Tests for the xarray_lockfix module.

Tests cover:
- The leak scenario the patch exists for: a non-blocking CombinedLock.acquire
  that fails on a later constituent must not leave earlier constituents held.
- Blocking acquire/release still round-trips.
- Idempotency of apply_combinedlock_leak_fix.
"""

from xarray.backends.locks import CombinedLock, SerializableLock

from cstar_forge.forge.xarray_lockfix import apply_combinedlock_leak_fix


def _fresh_combined_lock() -> CombinedLock:
    return CombinedLock([SerializableLock(), SerializableLock()])


def test_apply_is_idempotent():
    assert apply_combinedlock_leak_fix() is True
    assert apply_combinedlock_leak_fix() is True


def test_nonblocking_failure_releases_partial_acquisition():
    """The upstream bug: with a later constituent busy, acquire(blocking=False)
    returned False but left the earlier constituent(s) held forever, wedging
    every subsequent netCDF4 read/write (the forge input-generation stall).
    """
    apply_combinedlock_leak_fix()
    combined = _fresh_combined_lock()

    # Simulate CombinedLock.__exit__'s forward-order release window: the last
    # constituent is still held by "another thread" while earlier ones are free.
    blocker = combined.locks[-1]
    assert blocker.acquire(blocking=False)
    try:
        assert combined.acquire(blocking=False) is False
        for lock in combined.locks[:-1]:
            assert not lock.locked(), "constituent leaked by failed acquire"
    finally:
        blocker.release()

    # With the blocker gone the same CombinedLock must be acquirable again
    # (the pre-patch behavior left it permanently deadlocked here).
    assert combined.acquire(blocking=False) is True
    combined.release()


def test_blocking_acquire_release_roundtrip():
    apply_combinedlock_leak_fix()
    combined = _fresh_combined_lock()
    assert combined.acquire() is True
    assert all(lock.locked() for lock in combined.locks)
    combined.release()
    assert not any(lock.locked() for lock in combined.locks)
