from typing import ClassVar

import pytest

from cstar.base.exceptions import CstarError
from cstar.system.manager import (
    _DerechoSystemContext,
    _ExpanseSystemContext,
    _get_system_context,
    _LinuxSystemContext,
    _MacOSSystemContext,
    _PerlmutterSystemContext,
    _registry,
    _SystemContext,
    register_sys_context,
)
from cstar.system.scheduler import Scheduler

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"


def test_unique_context_names() -> None:
    """Verify that known contexts have a unique key."""
    context_types = (
        _PerlmutterSystemContext,
        _MacOSSystemContext,
        _DerechoSystemContext,
        _ExpanseSystemContext,
        _LinuxSystemContext,
    )

    registered_names = set(_registry.keys())

    assert len({ctx.name for ctx in context_types}) == len(registered_names)


@pytest.mark.parametrize(
    "cls_",
    [
        _PerlmutterSystemContext,
        _MacOSSystemContext,
        _DerechoSystemContext,
        _ExpanseSystemContext,
        _LinuxSystemContext,
    ],
)
def test_context_registry(cls_: type[_SystemContext]) -> None:
    """Verify that all known system contexts are registered."""
    ctx = _get_system_context(cls_.name)

    # confirm all properties of the factory produced context match
    assert ctx.name == cls_.name
    assert ctx.compiler == cls_.compiler
    assert ctx.mpi_prefix == cls_.mpi_prefix
    assert isinstance(ctx.create_scheduler(), type(cls_.create_scheduler()))


@pytest.mark.parametrize(
    ("expected_name", "cls_"),
    [
        ("perlmutter", _PerlmutterSystemContext),
        ("darwin_arm64", _MacOSSystemContext),
        ("derecho", _DerechoSystemContext),
        ("expanse", _ExpanseSystemContext),
        ("linux_x86_64", _LinuxSystemContext),
    ],
)
def test_registry_keys(expected_name: str, cls_: type[_SystemContext]) -> None:
    """Verify that the system contexts have the names expected from using
    HostNameEvaluator for all known systems."""
    assert expected_name == cls_.name


def test_new_registration() -> None:
    """Verify that registrations of new context types are immediately available."""

    @register_sys_context
    class MockSystemContext(_SystemContext):
        """Mock system context to test the context registry."""

        name: ClassVar[str] = "mock-system-name"
        compiler: ClassVar[str] = "mock-compiler"
        mpi_prefix: ClassVar[str] = "mock-mpi-prefix"

        @classmethod
        def create_scheduler(cls) -> Scheduler | None:
            """Mock scheduler creation."""
            raise NotImplementedError

    ctx = _get_system_context(MockSystemContext.name)

    assert ctx
    assert ctx.name == MockSystemContext.name


def test_unknown_context_name() -> None:
    """Verify that requesting an unregistered context fails."""
    with pytest.raises(CstarError):
        _ = _get_system_context("invalid-name")
