import typing as t
from contextvars import ContextVar

from pydantic import BaseModel, ConfigDict, Field

from cstar.base.exceptions import CstarExpectationFailed


class WorkplanRuntimeContext(BaseModel):
    """Enable access to cross-cutting concerns throughout Workplan orchestration."""

    run_id: t.Annotated[str, Field(frozen=True)]
    """The unique run-id of the current run."""

    user_variables: t.Annotated[t.Mapping[str, str], Field(frozen=True)]
    """The runtime variables available for the run."""

    model_config = ConfigDict(str_strip_whitespace=True)


_TRuntimeContextKey: t.TypeAlias = t.Literal["ctx.runtime"]
_TWorkplanContextKey: t.TypeAlias = t.Literal["ctx.workplan"]
_TRunidContextKey: t.TypeAlias = t.Literal["ctx.runid"]

CTX_RUNTIME_CONTEXT: _TRuntimeContextKey = "ctx.runtime"
"""Key identifying the ContextVar holding workplan runtime configuration."""

CTX_WORKPLAN_ID: _TWorkplanContextKey = "ctx.workplan"
"""Key identifying the ContextVar holding the run ID."""

CTX_RUN_ID: _TRunidContextKey = "ctx.runid"
"""Key identifying the ContextVar holding the run ID."""


@t.overload
def _put(
    key: _TRuntimeContextKey,
    value: WorkplanRuntimeContext,
) -> None: ...


@t.overload
def _put(
    key: _TRunidContextKey,
    value: str,
) -> None: ...


def _put(
    key: str,
    value: object,
) -> None:
    """Make the runtime context globally accessible.

    Parameters
    ----------
    key : str
        The key for the context object that will be stored.
    value : object
        The object to be stored in context.
    """
    var: ContextVar = ContextVar(key)
    var.set(value)


@t.overload
def _get(key: _TRuntimeContextKey) -> WorkplanRuntimeContext: ...


@t.overload
def _get(key: _TRunidContextKey) -> str: ...


def _get(key: str) -> object:
    """Make the runtime context globally accessible.

    Parameters
    ----------
    key : str
        The key of the context object to retrieve.

    Returns
    -------
    object
    """
    var: ContextVar = ContextVar(key)
    if ctx := var.get(None):
        return ctx

    msg = f"Unable to retrieve context object: {key}"
    raise CstarExpectationFailed(msg)


def get_context() -> WorkplanRuntimeContext:
    return _get(CTX_RUNTIME_CONTEXT)


def get_runid() -> str:
    return _get(CTX_RUN_ID)


def put_workplan_context(ctx: WorkplanRuntimeContext) -> None:
    _put(CTX_RUNTIME_CONTEXT, ctx)


def put_runid(run_id: str) -> None:
    _put(CTX_RUN_ID, run_id)
