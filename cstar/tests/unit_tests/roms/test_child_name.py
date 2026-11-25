import pytest

from cstar.roms.outputs import child_name_problems


@pytest.mark.parametrize("name", ["monterey", "salish-sea", "a" * 20])
def test_child_name_accepted(name):
    assert child_name_problems(name) == []


@pytest.mark.parametrize("name", ["salish_sea", "a" * 21, "-x", "1x", ""])
def test_child_name_rejected(name):
    assert len(child_name_problems(name)) == 1
