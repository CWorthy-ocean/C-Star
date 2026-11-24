"""Tests for the field-widget helpers the wizard shares (cstar.wizard.fields)."""

from cstar.wizard.fields import _base_type, _is_optional_numeric, _make_field_widget


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
