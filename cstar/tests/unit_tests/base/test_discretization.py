from cstar.base.discretization import Discretization


def test_is_instantiable_marker_with_no_attributes():
    """Discretization is a minimal marker ABC: it can be instantiated directly
    (no abstract methods) and carries no attributes of its own.
    """
    discretization = Discretization()
    assert discretization.__dict__ == {}
