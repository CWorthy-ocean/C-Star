import typing as t
from pathlib import Path

import typer
from pydantic import ValidationError

from cstar.base.exceptions import CstarExpectationFailed
from cstar.cli.common import format_validation_errors
from cstar.cli.workplan.shared import preprocess_varfile, preprocess_vars
from cstar.entrypoint.utils import (
    ARG_SCHEMA_ONLY,
    ARG_SCHEMA_ONLY_HELP,
    ARG_VAR_HELP,
    ARG_VAR_LONG,
    ARG_VAR_SHORT,
    ARG_VARFILE_HELP,
    ARG_VARFILE_LONG,
    ARG_VARFILE_SHORT,
)
from cstar.orchestration.models import UserDefinedVariables, Workplan
from cstar.orchestration.orchestration import LiveWorkplan
from cstar.orchestration.serialization import validate_serialized_entity
from cstar.orchestration.transforms import TemplateFillTransform, WorkplanTransformer

if t.TYPE_CHECKING:
    from collections.abc import Mapping

app = typer.Typer()


def _deep_check(wp: Workplan, user_vars: "Mapping[str, str] | None") -> t.NoReturn:
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
        Runtime variable replacements captured from `--var`/`--varfile`.

    Raises
    ------
    typer.Exit
        Code 0 if every step resolves cleanly, otherwise 1.
    """
    named_config = UserDefinedVariables(
        keys=set(wp.runtime_vars),
        mapping=user_vars or {},
        require_coverage=True,
    )

    problems: list[str] = []
    if named_config.error:
        problems.append(named_config.error)
    else:
        fill = TemplateFillTransform(variable_resolver=lambda name: named_config[name])
        try:
            transformed = WorkplanTransformer(wp, fill).apply()
        except ValidationError as ex:
            problems.append(format_validation_errors(ex))
        except KeyError as ex:
            # an undeclared `{{placeholder}}`; the lookup wraps a readable message
            problems.append(str(ex.args[0]))
        except (ValueError, FileNotFoundError, CstarExpectationFailed) as ex:
            problems.append(str(ex))
        else:
            step_count = len(transformed.steps)
            print(
                "Applications, blueprints, overrides and directives resolved "
                f"for {step_count} step(s)"
            )

    if problems:
        print("Resolution problems:")
        for problem in problems:
            print(problem)
        raise typer.Exit(1)

    raise typer.Exit(0)


@app.command(
    name="check",
    help="Perform content validation on a user-supplied workplan.",
)
def check(
    ctx: typer.Context,
    path: t.Annotated[str, typer.Argument(help="Path to the workplan")],
    schema_only: t.Annotated[
        bool,
        typer.Option(ARG_SCHEMA_ONLY, help=ARG_SCHEMA_ONLY_HELP),
    ] = False,
    user_variables: t.Annotated[
        list[str],
        typer.Option(
            ARG_VAR_LONG,
            ARG_VAR_SHORT,
            help=ARG_VAR_HELP,
            callback=preprocess_vars,
        ),
    ] = [],
    user_variables_path: t.Annotated[
        Path | None,
        typer.Option(
            ARG_VARFILE_LONG,
            ARG_VARFILE_SHORT,
            help=ARG_VARFILE_HELP,
            callback=preprocess_varfile,
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
) -> None:
    """Perform content validation on the workplan supplied by the user.

    By default, also runs the same in-memory schedule-time resolution `run`
    performs before submitting anything -- application lookup, blueprint
    loading, override merging and directive validation -- so a workplan that
    passes `check` is one `run` will accept. Pass `--schema-only` to skip
    that pass and only validate the file's structure. Nothing is written to
    disk either way.

    Parameters
    ----------
    ctx : typer.Context
        The typer context; carries the runtime variables captured by the
        `--var`/`--varfile` callbacks.
    path : str
        Path to the workplan.
    schema_only : bool
        Skip the deep resolution pass.
    user_variables : list[str]
        Runtime variable replacements in `key=value` form.
    user_variables_path : Path | None
        Path to a file of `key=value` runtime variable replacements, one per
        line.

    Raises
    ------
    typer.BadParameter
        If `--var` and `--varfile` are both supplied, or the workplan fails
        schema validation.
    """
    if user_variables and user_variables_path is not None:
        msg = f"{ARG_VAR_LONG} and {ARG_VARFILE_LONG} must not be supplied together"
        raise typer.BadParameter(msg)

    schema_result = validate_serialized_entity(path, Workplan)
    if schema_result.item is not None:
        wp = schema_result.item
        print(f"The workplan `{wp.name}` is valid")
        if schema_only:
            raise typer.Exit(0)
        _deep_check(wp, t.cast("Mapping[str, str] | None", ctx.obj))

    error = schema_result.error_msg

    live_result = validate_serialized_entity(path, LiveWorkplan)
    if live_result.item is not None:
        print(f"The workplan `{live_result.item.name}` is valid")
        if not schema_only:
            print(
                "Skipping deep resolution: this workplan was already "
                "transformed by a prior run; re-resolving it would "
                "double-package its overrides."
            )
        raise typer.Exit(0)

    raise typer.BadParameter(error)
