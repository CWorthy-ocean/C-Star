"""Running a command from a widget handler and streaming its output into the page.

The Save and Next steps cards' "Run Forge here" and the Workplan page's run actions
use these. Like the rest of :mod:`cstar.wizard`, nothing here imports ``ipywidgets``.
"""

from __future__ import annotations

import re


def _schedule_coroutine(coro):
    """Schedule a coroutine on the running loop (returns a Task), or run it to
    completion directly if there is no running loop. Mirrors
    ``cstar.applications.forge.executor._schedule_coroutine`` -- needed for seamless
    execution of async code (like streaming a subprocess) from a synchronous
    ipywidgets ``on_click`` handler, both inside and outside Jupyter.
    """
    import asyncio

    try:
        return asyncio.get_running_loop().create_task(coro)
    except RuntimeError:
        return asyncio.run(coro)


_STREAM_READ_SIZE = 2**16  # bytes per subprocess read; also bounds the flush below
# A run whose child never emits a newline for a long stretch -- classically a
# ``\r``-redrawn progress bar (git clone, tqdm, download progress) -- would grow an
# unbounded "line". Flush an unterminated run once it reaches this so memory stays
# bounded and the log still updates. (This is also what a line-oriented reader could
# not survive: asyncio's StreamReader.readline() raises ValueError, "Separator is not
# found, and chunk exceed the limit", at its 64 KiB default -- the bug this avoids.)
_STREAM_MAX_LINE = 2**16


def _drain_stream_buffer(
    buf: bytes, *, at_eof: bool = False
) -> tuple[list[str], bytes]:
    r"""Split accumulated subprocess bytes into display lines, returning
    ``(lines, remainder)``.

    Segments are cut on ``\r\n``, ``\r`` or ``\n`` and each terminator is normalised
    to a single trailing ``\n``, so ``\r``-only progress redraws surface as successive
    log lines instead of one ever-growing line. A lone trailing ``\r`` is held back in
    the remainder (unless at EOF) so a ``\r\n`` split across two reads is not mistaken
    for a bare ``\r`` plus a spurious blank line. When not at EOF, an unterminated
    remainder that reaches ``_STREAM_MAX_LINE`` is emitted as a partial line to keep
    memory bounded; at EOF any remainder is flushed.
    """
    held = b""
    if not at_eof and buf.endswith(b"\r"):
        buf, held = buf[:-1], b"\r"
    tokens = re.split(rb"(\r\n|\r|\n)", buf)
    remainder = tokens.pop() + held
    lines = [
        tokens[i].decode(errors="replace") + "\n" for i in range(0, len(tokens), 2)
    ]
    if at_eof:
        if remainder:
            lines.append(remainder.decode(errors="replace"))
        remainder = b""
    elif len(remainder) >= _STREAM_MAX_LINE:
        lines.append(remainder.decode(errors="replace"))
        remainder = b""
    return lines, remainder
