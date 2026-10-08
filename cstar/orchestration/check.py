import typing as t

from pydantic import ValidationError

from cstar.base.exceptions import CstarError, CstarExpectationFailed
from cstar.orchestration.formatting import format_validation_errors
from cstar.orchestration.models import UserDefinedVariables, Workplan
from cstar.orchestration.transforms import (
    TemplateFillTransform,
    WorkplanTransformer,
    resolve_external_runs,
)

if t.TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from cstar.orchestration.orchestration import Launcher


async def deep_check(
    wp: Workplan,
    user_vars: "Mapping[str, str] | None",
    launcher_factory: "Callable[[], Launcher[t.Any]]",
    wp_path: "Path | None" = None,
) -> tuple[Workplan | None, list[str]]:
    """Resolve a workplan the same way `run` does before submitting anything.

    Builds the runtime variable mapping, then runs `WorkplanTransformer` over
    the workplan in memory: this imports each step's application, loads and
    validates its blueprint, merges `blueprint_overrides`, and validates
    directives. Nothing is written to disk.

    Parameters
    ----------
    wp : Workplan
        The schema-valid workplan to resolve.
    user_vars : Mapping[str, str] | None
        Runtime variable replacements.
    launcher_factory : Callable[[], Launcher[Any]]
        Produces the launcher used to look up the runs a workplan references.
        Only called when the workplan declares external runs.
    wp_path : Path | None
        The file the workplan was read from; a step's relative blueprint path
        is resolved against its directory, as `run` does.

    Returns
    -------
    tuple[Workplan | None, list[str]]
        The resolved workplan and no problems on success, otherwise `None` and
        a description of each problem found.
    """
    named_config = UserDefinedVariables(
        keys=set(wp.runtime_vars),
        mapping=user_vars or {},
        require_coverage=True,
    )
    if named_config.error:
        return None, [named_config.error]

    fill = TemplateFillTransform(variable_resolver=lambda name: named_config[name])
    try:
        external = await resolve_external_runs(wp, fill, launcher_factory)
        return WorkplanTransformer(wp, fill, external, wp_path=wp_path).apply(), []
    except ValidationError as ex:
        return None, [format_validation_errors(ex)]
    except KeyError as ex:
        # an undeclared `{{placeholder}}`; the lookup wraps a readable message
        return None, [str(ex.args[0])]
    except (
        ValueError,
        FileNotFoundError,
        CstarError,
        CstarExpectationFailed,
    ) as ex:
        return None, [str(ex)]
