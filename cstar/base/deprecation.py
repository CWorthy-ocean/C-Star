"""The one way C-Star announces that a spelling, key or layout is going away.

`deprecated` is called wherever an old form is detected, at load or
`cstar workplan check` time rather than only where it is finally used, so the
user hears about it before anything is submitted. Each call names the release
that removes the form as a string literal: ``ci/check_deprecations.py`` reads
every call and refuses to finalize that release until the form is removed, so a
forgotten removal stops the release rather than the user. The policy itself is
in ``docs/contributing.rst`` ("Deprecations and compatibility").
"""

import warnings

from cstar.base.log import get_logger

log = get_logger(__name__)


class CstarDeprecationWarning(FutureWarning):
    """A form C-Star still accepts and will stop accepting in a named release.

    A `FutureWarning`, so Python shows it by default; the CLI ignores it because
    `deprecated` also logs the same message, and one line is enough there.
    """


_WARNED: set[str] = set()
"""Messages `deprecated` has already emitted: a workplan check, a catalog
rescan or a wizard rebuild can meet the same old form many times, and the user
needs to hear about it once per process."""


def deprecation_message(what: str, replacement: str, *, removal: str) -> str:
    """Return the sentence announcing that `what` is deprecated.

    For text C-Star prints without running Python, such as the shell function
    from `cstar env shell-init`; everything else calls `deprecated`. The
    parameters are those of `deprecated`, and `removal` is checked the same way.
    """
    return (
        f"{what} is deprecated and stops working in cstar-ocean {removal}; "
        f"use {replacement} instead."
    )


def deprecated(what: str, replacement: str, *, removal: str) -> None:
    """Announce, once per process, that `what` is deprecated.

    Logs the message, then emits it as a `CstarDeprecationWarning` attributed to
    the line that detected the old form. A later call with the same arguments
    does nothing.

    Parameters
    ----------
    what : str
        The deprecated form as the user wrote it, e.g. "The 'nest-from' key
        'bry_path'".
    replacement : str
        What to use instead, completing "use ... instead".
    removal : str
        The release that removes `what`, e.g. "0.20.0". Pass a string literal:
        ``ci/check_deprecations.py`` reads it from the source.
    """
    message = deprecation_message(what, replacement, removal=removal)
    if message in _WARNED:
        return
    _WARNED.add(message)
    log.warning(message)
    warnings.warn(message, CstarDeprecationWarning, stacklevel=2)
