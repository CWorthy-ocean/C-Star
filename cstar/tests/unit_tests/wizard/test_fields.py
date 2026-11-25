"""Tests for the helpers the wizard's modules share (cstar.wizard.fields)."""

from cstar.wizard.fields import (
    _base_type,
    _drain_stream_buffer,
    _is_optional_numeric,
    _make_field_widget,
)


def test_non_optional_int_field_keeps_number_box():
    """Only ``Optional[int]`` / ``Optional[float]`` get the blank-able box; a union
    of several types keeps following the field's value (``list[str] | str``).
    """
    import ipywidgets as W

    assert isinstance(_make_field_widget(W, "n", int, 3), W.IntText)
    assert not _is_optional_numeric(int)
    assert _is_optional_numeric(int | None)
    assert _is_optional_numeric(float | None)
    assert not _is_optional_numeric(str | None)
    assert _base_type(list[str] | str, "all") is str
    assert _base_type(list[str] | str, ["PH"]) is list


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
