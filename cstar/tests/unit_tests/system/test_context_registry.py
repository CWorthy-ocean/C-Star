from typing import ClassVar
from unittest.mock import PropertyMock, patch

import pytest

from cstar.system.manager import (
    CTX_REGISTRY,
    SystemContext,
    SystemSource,
    get_registered_sys_contexts,
    get_system_context,
    register_sys_context,
    registered_context,
    registered_names,
    select_system,
)
from cstar.system.scheduler import Scheduler

DEFAULT_MOCK_MACHINE_NAME = "mock_machine"
DEFAULT_MOCK_HOST_NAME = "mock_system"

BUNDLED_NAMES = (
    "anvil",
    "bouchet",
    "darwin_arm64",
    "derecho",
    "elja",
    "expanse",
    "linux_aarch64",
    "linux_x86_64",
    "oscar",
    "perlmutter",
)


def test_unique_context_names() -> None:
    """Verify that known contexts have a unique key."""
    context_types = get_registered_sys_contexts()

    assert len({ctx.name for ctx in context_types}) == len(context_types)
    assert {ctx.name for ctx in context_types} == set(registered_names())
    assert set(BUNDLED_NAMES) <= set(registered_names())


@pytest.mark.parametrize("name", BUNDLED_NAMES)
def test_context_registry(name: str) -> None:
    """Verify that all known system contexts are registered."""
    wrapped_class = registered_context(name)
    with patch(
        "cstar.system.manager.HostNameEvaluator.name",
        new_callable=PropertyMock,
        return_value=wrapped_class.name,
    ):
        ctx = get_system_context()

    # confirm all properties of the factory produced context match
    assert ctx.name == wrapped_class.name
    assert ctx.compiler == wrapped_class.compiler
    assert ctx.mpi_prefix == wrapped_class.mpi_prefix
    assert isinstance(ctx.create_scheduler(), type(wrapped_class.create_scheduler()))


@pytest.mark.parametrize(
    ("expected_name", "registered"),
    [
        ("perlmutter", "perlmutter"),
        ("darwin_arm64", "darwin_arm64"),
        ("derecho", "derecho"),
        ("elja", "elja"),
        ("expanse", "expanse"),
        ("linux_x86_64", "linux_x86_64"),
    ],
)
def test_registry_keys(expected_name: str, registered: str) -> None:
    """Verify that the system contexts have the names expected from using
    HostNameEvaluator for all known systems.
    """
    assert expected_name == registered_context(registered).name


def test_new_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that registrations of new context types are immediately available."""
    monkeypatch.setattr("cstar.system.manager.CTX_REGISTRY", dict(CTX_REGISTRY))

    @register_sys_context
    class MockSystemContext(SystemContext):
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
        ctx = get_system_context()

    assert ctx
    assert ctx.name == MockSystemContext.name


def test_unknown_context_name() -> None:
    """Verify that a detected name that is not registered falls back to the generic
    context of the platform instead of failing.
    """
    with (
        patch(
            "cstar.system.manager.HostNameEvaluator.name",
            new_callable=PropertyMock,
            return_value="invalid-name",
        ),
        patch(
            "cstar.system.manager.HostNameEvaluator.platform_hostname",
            new_callable=PropertyMock,
            return_value="linux_x86_64",
        ),
    ):
        selection = select_system()
        ctx = get_system_context()

    assert selection.context is registered_context("linux_x86_64")
    assert selection.source is SystemSource.FALLBACK
    assert selection.detected == "invalid-name"
    assert isinstance(ctx, registered_context("linux_x86_64"))
