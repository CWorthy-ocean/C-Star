"""Tests for running a command from the wizard and streaming its output
(cstar.wizard.streaming).
"""

from cstar.wizard.streaming import _drain_stream_buffer


def test_drain_stream_buffer_crlf_split_across_reads():
    r"""A \r\n straddling a read boundary is one line break, not \r + blank line:
    the trailing \r is held back for the next chunk.
    """
    lines, rem = _drain_stream_buffer(b"abc\ndef\r")
    assert lines == ["abc\n"]
    assert rem == b"def\r"  # \r held, not emitted as a terminator yet
    lines2, rem2 = _drain_stream_buffer(rem + b"\nghi")
    assert lines2 == ["def\n"]  # the held \r + \n = a single CRLF break
    assert rem2 == b"ghi"


def test_drain_stream_buffer_mixed_terminators_and_eof():
    r"""\r, \n and \r\n all cut lines (normalised to \n); EOF flushes the
    unterminated remainder.
    """
    lines, rem = _drain_stream_buffer(b"a\rb\nc\r\nd")
    assert lines == ["a\n", "b\n", "c\n"]
    assert rem == b"d"
    flushed, rem2 = _drain_stream_buffer(rem, at_eof=True)
    assert flushed == ["d"]  # no trailing newline added at EOF
    assert rem2 == b""
