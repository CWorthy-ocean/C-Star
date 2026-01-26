from typing import ClassVar
from unittest.mock import PropertyMock, patch

import pytest

from cstar.base.exceptions import CstarError
from cstar.system.manager import (
    _AnvilSystemContext,
    _DerechoSystemContext,
    _ExpanseSystemContext,
    _get_system_context,
    _LinuxARM64SystemContext,
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
        _LinuxARM64SystemContext,
        _LinuxSystemContext,
        _AnvilSystemContext,
    )

    registered_names = set(_registry.keys())

    assert len({ctx.name for ctx in context_types}) == len(registered_names)


@pytest.mark.parametrize(
    "wrapped_class",
    [
        _PerlmutterSystemContext,
        _MacOSSystemContext,
        _DerechoSystemContext,
        _ExpanseSystemContext,
        _LinuxSystemContext,
        _LinuxARM64SystemContext,
    ],
)
def test_context_registry(wrapped_class: type[_SystemContext]) -> None:
    """Verify that all known system contexts are registered."""
    with patch(
        "cstar.system.manager.HostNameEvaluator.name",
        new_callable=PropertyMock,
        return_value=wrapped_class.name,
    ):
        ctx = _get_system_context()

    # confirm all properties of the factory produced context match
    assert ctx.name == wrapped_class.name
    assert ctx.compiler == wrapped_class.compiler
    assert ctx.mpi_prefix == wrapped_class.mpi_prefix
    assert isinstance(ctx.create_scheduler(), type(wrapped_class.create_scheduler()))


@pytest.mark.parametrize(
    ("expected_name", "wrapped_class"),
    [
        ("perlmutter", _PerlmutterSystemContext),
        ("darwin_arm64", _MacOSSystemContext),
        ("derecho", _DerechoSystemContext),
        ("expanse", _ExpanseSystemContext),
        ("linux_x86_64", _LinuxSystemContext),
    ],
)
def test_registry_keys(expected_name: str, wrapped_class: type[_SystemContext]) -> None:
    """Verify that the system contexts have the names expected from using
    HostNameEvaluator for all known systems.
    """
    assert expected_name == wrapped_class.name


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

    with patch(
        "cstar.system.manager.HostNameEvaluator.name",
        new_callable=PropertyMock,
        return_value=MockSystemContext.name,
    ):
        ctx = _get_system_context()

    assert ctx
    assert ctx.name == MockSystemContext.name


def test_unknown_context_name() -> None:
    """Verify that requesting an unregistered context fails."""
    with (
        pytest.raises(CstarError),
        patch(
            "cstar.system.manager.HostNameEvaluator.name",
            new_callable=PropertyMock,
            return_value="invalid-name",
        ),
    ):
        _ = _get_system_context()
