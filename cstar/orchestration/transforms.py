import re
import typing as t
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from pydantic import (
    BaseModel,
)

from cstar.applications.core import ApplicationDefinition, Transform, get_application
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.feature import (
    ENV_FF_ORCH_TRX_TIMESPLIT,
    is_feature_enabled,
)
from cstar.base.log import LoggingMixin
from cstar.base.utils import deep_merge
from cstar.execution.file_system import local_copy
from cstar.orchestration.models import (
    Application,
    Blueprint,
    KeyValueStore,
    Workplan,
)
from cstar.orchestration.orchestration import LiveStep
from cstar.orchestration.serialization import deserialize, serialize

if t.TYPE_CHECKING:
    from cstar.entrypoint.runner import XBlueprintRunner

TRANSFORMS: dict[str, list[Transform[LiveStep]]] = defaultdict(list)
"""Storage for transform registrations."""


def register_transform(application: str, transform: Transform[t.Any]) -> None:
    """Register a transform for an application.

    Parameters
    ----------
    application : str
        The application name.
    transform : Transform
        The transform instance to register for the application.
    """
    TRANSFORMS[application].append(transform)


def get_transforms(application: str) -> list[Transform[t.Any]]:
    """Retrieve a list of transforms to be applied for an application.

    Parameters
    ----------
    application : str
        The application name.

    Returns
    -------
    list[Transform]
        A list containing transforms
    """
    return TRANSFORMS.get(application, [])


PLACEHOLDER_RE = re.compile(r"\{\{([^}]+)\}\}")
"""Pattern matching double-brace template placeholders.

Captures the full content between braces so dispatch logic can distinguish
plain variable references (``{{my_var}}``) from path references
(``{{path: step_name}}``).
"""


class TemplateFillTransform:
    """Fill ``{{placeholder}}`` template strings in a step's blueprint_overrides.

    Recursively traverses the nested blueprint_overrides structure and
    dispatches each placeholder to one of two resolvers:

    - **variable resolver** — handles plain ``{{name}}`` tokens by looking up
      *name* in the caller-supplied mapping (e.g. user-defined runtime variables).
    - **path resolver** — handles ``{{path: step_name}}`` tokens by returning
      the working-directory path of the named step.  Must be bound via
      :meth:`with_path_resolver` before any step that uses this syntax is
      processed.

    The transform yields a single updated step; the original step's
    ``blueprint_overrides`` is not mutated.
    """

    def __init__(
        self,
        variable_resolver: Callable[[str], str] | None = None,
        path_resolver: Callable[[str], Path] | None = None,
    ) -> None:
        """Initialize the transform.

        Parameters
        ----------
        variable_resolver : Callable[[str], str] | None
            Maps a plain placeholder name to its replacement string.
        path_resolver : Callable[[str], Path] | None
            Maps a step name to its working-directory path.  Required only
            when ``blueprint_overrides`` contains ``{{path: …}}`` tokens.
        """
        self._variable_resolver = variable_resolver
        self._path_resolver = path_resolver

    def with_path_resolver(
        self, path_resolver: Callable[[str], Path]
    ) -> "TemplateFillTransform":
        """Return a new instance with the given path resolver bound.

        Parameters
        ----------
        path_resolver : Callable[[str], Path]
            Maps a step name to its working-directory path.

        Returns
        -------
        TemplateFillTransform
        """
        return TemplateFillTransform(self._variable_resolver, path_resolver)

    @staticmethod
    def suffix() -> str:
        """Return the suffix used when persisting a resource modified by this transform."""
        return "tmpl"

    def _resolve(self, content: str) -> str:
        """Dispatch a single placeholder's inner content to the correct resolver.

        Parameters
        ----------
        content : str
            The text captured between ``{{`` and ``}}``, stripped of
            surrounding whitespace.

        Returns
        -------
        str
            The resolved replacement string.

        Raises
        ------
        ValueError
            If the appropriate resolver has not been provided.
        """
        if content.startswith("path:"):
            step_name = content[len("path:") :].strip()
            if self._path_resolver is None:
                raise ValueError(
                    f"No path resolver provided for placeholder '{{{{path: {step_name}}}}}'"
                )
            return str(self._path_resolver(step_name))

        if self._variable_resolver is None:
            raise ValueError(
                f"No variable resolver provided for placeholder '{{{{{content}}}}}'"
            )
        return self._variable_resolver(content)

    def _fill(self, step: LiveStep) -> LiveStep:
        content = step.model_dump_json(by_alias=True)
        matches = PLACEHOLDER_RE.findall(content)
        if not matches:
            return step

        # use set to replace all occurrences at once
        for match in set(matches):
            content = content.replace(f"{{{{{match}}}}}", self._resolve(match))

        if PLACEHOLDER_RE.findall(content):
            raise CstarExpectationFailed(
                "Some templated values were not filled or placeholders were malformed."
            )

        return step.model_validate_json(content)

    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Apply template filling to a step's blueprint_overrides.

        Parameters
        ----------
        step : LiveStep
            The step whose blueprint_overrides will be traversed.

        Returns
        -------
        Iterable[LiveStep]
            A single-element iterable containing the updated step.
        """
        return (LiveStep.from_step(self._fill(step)),)


class WorkplanTransformer(LoggingMixin):
    """Transform a workplan by applying transforms to its steps."""

    original: Workplan
    """The original, pre-transformation workplan."""

    _transformed: Workplan | None = None
    """The post-transformation workplan."""

    DERIVED_PATH_SUFFIX: t.Literal["_trx"] = "_trx"
    """Suffix appended to the original workplan path when generating a derived path."""

    def __init__(
        self,
        wp: Workplan,
        transform: Transform[t.Any],
        fill_transform: TemplateFillTransform | None = None,
    ) -> None:
        """Initialize the instance."""
        self.original = Workplan(**wp.model_dump(by_alias=True))
        self.transform_fn = transform
        self.fill_transform = fill_transform
        self._transformed: Workplan | None = None

    @property
    def is_modified(self) -> bool:
        """Return `True` if the transformed workplan differs from the original.

        Returns
        -------
        bool
        """
        dump_a = self.original.model_dump()
        dump_b = self.transformed.model_dump()

        return dump_a != dump_b

    @property
    def transformed(self) -> Workplan:
        """Return the transformed workplan.

        Returns
        -------
        Workplan
        """
        if self._transformed is None:
            self._transformed = self.apply()
        return self._transformed

    @staticmethod
    def derived_path(
        source: Path,
        target_dir: Path | None = None,
        suffix: str = DERIVED_PATH_SUFFIX,
        extension: str | None = None,
    ) -> Path:
        """Generate a new path name derived from the source path.

        If no target directory is specified, the derived path will be in the
        same directory as the source path.

        Parameters
        ----------
        source : Path
            The source path.
        target_dir : Path | None, optional
            An alternate parent directory to place the file
        suffix : str, optional
            A suffix to append to the source file name, by default "_trx"

        Returns
        -------
        Path

        Raises
        ------
        ValueError
            If the source and target paths are identical
        """
        if not target_dir and not suffix:
            msg = f"Identical source and target will destroy the source: `{source}`"
            raise ValueError(msg)

        directory = target_dir or source.parent
        filename = Path(source.name).with_stem(f"{source.stem}{suffix}")
        if extension:
            filename = filename.with_suffix(extension)

        return directory / filename

    def apply(self) -> Workplan:
        """Create a new workplan with appropriate transforms applied.

        Returns
        -------
        Workplan
        """
        if self._transformed:
            return self._transformed

        apply_to = {Application.ROMS_MARBL, Application.SLEEP, Application.NEST_IC}

        # ensure consistent output targets for all steps in the workplan
        live_steps = [LiveStep.from_step(s) for s in self.original.steps]

        # fill template placeholders before any other transform operates on overrides
        if self.fill_transform is not None:
            step_index = {s.name: s for s in live_steps}
            fill = self.fill_transform.with_path_resolver(
                lambda name: step_index[name].get_working_dir
            )
            live_steps = [filled for step in live_steps for filled in fill(step)]

        # apply user blueprint_overrides and ensure consistent output targets;
        # must happen before time-splitting so the splitter reads correct blueprint values
        steps: list[LiveStep] = []

        for i in range(len(live_steps)):
            step = live_steps[i]

            if step.application in apply_to:
                step = override_output_directory(step)
            steps.append(step)

        if is_feature_enabled(ENV_FF_ORCH_TRX_TIMESPLIT):
            split_steps: list[LiveStep] = []
            named_dep_map: dict[str, str] = {}

            for step in steps:
                if step.application in apply_to:
                    transformed_steps = self.transform_fn(step)
                    named_dep_map[step.name] = transformed_steps[-1].name
                    split_steps.extend(transformed_steps)
                else:
                    split_steps.append(step)

            # remap dependency references to point to the last child of each split parent
            steps = []
            for step in split_steps:
                if step.application in apply_to:
                    depends_on = {str(d) for d in step.depends_on}
                    if to_update := depends_on.intersection(named_dep_map):
                        depends_on.update(named_dep_map[x] for x in to_update)
                        depends_on.difference_update(to_update)
                        step.depends_on.clear()
                        step.depends_on.extend(depends_on)
                steps.append(step)

        # apply any blueprint_overrides accumulated by prior transforms (e.g. per-child
        # date/IC overrides set by the time-splitter); skipped when overrides are empty
        override_transform = OverrideTransform()
        final_steps: list[LiveStep] = []
        for step in steps:
            if step.blueprint_overrides or step.directives:
                final_steps.extend(override_transform(step))
            else:
                final_steps.append(step)
        steps = final_steps

        self._transformed = self.original.model_copy(
            update={
                "steps": steps,
                "name": f"{self.original.name} (transformed)",
            },
        )

        return self._transformed


class OverrideTransform(Transform[LiveStep]):
    """Transform that overrides a step by returning a blueprint with all overridden attributes applied."""

    _system_overrides: dict[str, t.Any]

    def __init__(self, sys_overrides: dict[str, t.Any] | None = None) -> None:
        """Initialize the instance.

        Parameters
        ----------
        sys_overrides : dict[str, t.Any] | None
            System-level blueprint overrides that will be applied after
            the user-supplied values.
        """
        self._system_overrides = sys_overrides or {}

    def apply(
        self,
        bp: Blueprint,
        overrides: dict[str, t.Any] | None = None,
    ) -> Blueprint:
        """Apply all overrides from a blueprint.

        Generate a new blueprint with overrides applied and an empty set of overrides.
        Store the newly generated blueprint in the output directory.

        Parameters
        ----------
        bp : Blueprint
            The blueprint to apply overrides to
        overrides : dict[str, t.Any] | None
            A dictionary containing overrides for attributes of a blueprint.

        Returns
        -------
        Blueprint
            The blueprint with all overrides applied.
        """
        overrides = overrides or {}
        overrides = overrides.copy()

        model = bp.model_dump(exclude_defaults=True, exclude_unset=True)

        # system-level overrides take precedence over step-level overrides
        changeset = deep_merge(overrides, self._system_overrides)
        merged = deep_merge(model, changeset)

        description = (
            f"{bp.description}; overridden keys [{', '.join(changeset.keys())}]"
        )
        merged.update(description=description)
        bp_type = type(bp)
        return bp_type(**merged)

    def __call__(self, step: LiveStep) -> Sequence[LiveStep]:
        """Apply the transform to a step.

        Parameters
        ----------
        step : Step
            The step to be transformed

        Returns
        -------
        Sequence[Step]
            Zero-to-many steps resulting from applying the transform.
        """
        bp_path = Path(step.blueprint_path)

        app: ApplicationDefinition[Blueprint, XBlueprintRunner[Blueprint]] = (
            get_application(step.application)
        )
        bp_type = app.blueprint

        blueprint: Blueprint = deserialize(bp_path, bp_type)

        updated_bp = self.apply(blueprint, step.blueprint_overrides)
        update: dict[str, t.Any] = {"blueprint_overrides": {}}

        live_step = LiveStep.from_step(step, update=update)

        bp_renamed = bp_path.with_stem(f"{bp_path.stem}.{self.suffix()}").name
        live_step.blueprint_path = live_step.fsm.work_dir / bp_renamed

        serialize(live_step.blueprint_path, updated_bp)
        return (live_step,)

    @staticmethod
    def suffix() -> str:
        """Return a suffix used when persisting a resource modified by this transform.

        Returns
        -------
        str
        """
        return "ovrd"


def override_output_directory(step: LiveStep) -> LiveStep:
    """Automatically override the output directory specified in a blueprint
    to write to the C-Star home directories.

    See `cstar.execution.file_system.DirectoryManager` for more detail
    on the available set of home directories.

    Returns
    -------
    LiveStep
        The transformed step.
    """
    sys_overrides = (
        {"runtime_params": {"output_dir": step.fsm.root}}
        if step.application == Application.ROMS_MARBL
        else {"output_dir": step.fsm.root}  # type: ignore[dict-item]
    )
    override_transform = OverrideTransform(sys_overrides)
    overridden_step_result = override_transform(step)

    return overridden_step_result[0]


class Directive(Transform[LiveStep], t.Protocol):
    _config: Mapping[str, t.Any]
    """Contract of a transform that can be used as a directive."""

    def __init__(self, config: dict[str, t.Any]) -> None:
        """Initialize the instance.

        Parameters
        ----------
        config : dict[str, t.Any] | None
            A dictionary containing configuration for the directive.
        """
        if not config:
            msg = "Configuration must be provided"
            raise ValueError(msg)

        self._config = config


class RestartFile(BaseModel):
    """Reference to a path that contains restart checkpoints."""

    path: Path
    """The path to a restart file."""
    _base: str = PrivateAttr()
    """The base name of the file."""
    _segment: str | None = PrivateAttr(default=None)
    """The segment identifier of the file."""
    _ts: datetime = PrivateAttr()
    """The timestamp parsed from the file name."""

    EXT: t.ClassVar[t.Literal["nc"]] = "nc"
    """The expected file extension for a restart file."""
    FMT_TS: t.ClassVar[t.Literal["%Y%m%d%H%M%S"]] = "%Y%m%d%H%M%S"
    """The expected timestamp format in the restart file name"""
    PATTERN_RST: t.ClassVar[t.Literal[r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"]] = (
        r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"
    )
    """A regex identifying full restart or partitioned files."""
    SUFFIX: t.ClassVar[t.Literal["_rst"]] = "_rst"
    """A unique suffix found in the name of restart files"""

    @classmethod
    def find(cls, search_path: Path, notfound_ok: bool = True) -> "RestartFile | None":
        """Search for a restart file in the specified location.

        Parameters
        ----------
        search_path : Path
            The path to search
        notfound_ok : bool
            If False, raise an exception if no restart files are found.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If the search path does not exist or contains no recognizable restart files.
        """
        search_path = search_path.expanduser().resolve()

        if not search_path.exists():
            msg = f"No directory found at path: {search_path!r}"
            raise ValueError(msg)

        if matches := sorted(search_path.rglob(f"*{cls.SUFFIX}.*.*.{cls.EXT}")):
            # prefer use of pre-partitioned data when available
            return RestartFile(path=matches[0])

        matches = sorted(search_path.rglob(f"*{cls.SUFFIX}*.{cls.EXT}"), reverse=True)
        if matches:
            return RestartFile(path=matches.pop(0))

        if not notfound_ok:
            msg = f"No restart files located. Unable to continue from {search_path!r}"
            raise CstarExpectationFailed(msg)
        return None

    @classmethod
    def from_parts(
        cls,
        base: str,
        timestamp: datetime,
        segment: str | None = None,
        directory: Path | None = None,
    ) -> "RestartFile":
        """Create a ResetFile from components.

        Parameters
        ----------
        base : str
            The base name for the restart file.
        timestamp : datetime
            The timestamp for the restart file.
        segment : str | None
            The 0-padded segment number if partitioned, otherwise `None`.
        directory : Path | None
            The directory to contain the file. If not specified, defaults to cwd.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If the search path does not exist or contains no recognizable restart files.
        """
        ts = timestamp.strftime(cls.FMT_TS)
        parted_clause = f".{segment}" if segment is not None else ""
        filename = f"{base}{cls.SUFFIX}.{ts}{parted_clause}.{cls.EXT}"

        if directory:
            return RestartFile(path=directory / filename)

        return RestartFile(path=Path(filename))

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: Path, _info: "ValidationInfo") -> Path:
        """Verify the supplied path meets the restart file naming convention.

        Parameters
        ----------
        value : str
            The value of the checkpoint frequency property
        _info : ValidationInfo
            Metadata for the current validation context
        """
        if value.suffix != f".{RestartFile.EXT}":
            msg = f"File extension does not match expected naming convention: {value.suffix}"
            raise ValueError(msg)

        if re.fullmatch(RestartFile.PATTERN_RST, value.name, flags=re.ASCII):
            return value

        msg = f"File name does not match expected naming convention: {value}"
        raise ValueError(msg)

    @model_validator(mode="after")
    def _model_validate(self) -> "RestartFile":
        """Perform post-processing on the restart file path.

        Returns
        -------
        ResetFile
        """
        matches = re.fullmatch(
            RestartFile.PATTERN_RST, self.path.as_posix(), flags=re.ASCII
        )
        if not matches:
            msg = f"File name does not match expected naming convention: {self.path}"
            raise ValueError(msg)

        self._base = matches.group(1)
        self._ts = datetime.strptime(matches.group(2), RestartFile.FMT_TS)
        # look for segment for partition number, e.g. <base>.<ts>.000.nc vs. <base>.<ts>.nc
        self._segment = matches.group(3)
        return self

    @property
    def timestamp(self) -> datetime:
        """Return a datetime derived from the timestamp in the restart file name.

        Returns
        -------
        datetime
        """
        return self._ts

    @property
    def is_partitioned(self) -> bool:
        """Return `True` if the restart file belongs to a partitioned dataset.

        Returns
        -------
        datetime
        """
        return self._segment is not None

    @property
    def partition(self) -> int | None:
        if self._segment:
            return int(self._segment)
        return None

    @property
    def formatted_timestamp(self) -> str:
        return self._ts.strftime(self.FMT_TS)


class RestartFileTrxAdapter:
    """Convert a restart file into a dictionary useful for use in an OverrideTransform."""

    @classmethod
    def adapt(cls, rst_file: RestartFile) -> dict[str, t.Any]:
        """Given a restart file, create a dictionary containing the overrides necessary to
        execute a simulation with the restart file specified in the initial conditions.

        Parameters
        ----------
        restart_file : ResetFile
            The restart file metadata used to convert into an override mapping.

        Returns
        -------
        Mapping[str, t.Any]
        """
        return {
            "runtime_params": {
                "start_date": rst_file.timestamp,
            },
            "initial_conditions": {
                "data": [
                    {
                        "location": rst_file.path.as_posix(),
                        "partitioned": rst_file.is_partitioned,
                    },
                ],
            },
        }


class BoundaryFile(BaseModel):
    path: Path
    """The path to a boundary file."""
    _base: str = PrivateAttr()
    """The base name of the file."""
    _segment: str | None = PrivateAttr(default=None)
    """The segment identifier of the file."""
    _ts: datetime = PrivateAttr()
    """The timestamp parsed from the file name."""

    EXT: t.ClassVar[t.Literal["nc"]] = "nc"
    """The expected file extension for a boundary file."""
    FMT_TS: t.ClassVar[t.Literal["%Y%m%d%H%M%S"]] = "%Y%m%d%H%M%S"
    """The expected timestamp format in the boundary file name"""
    PATTERN_RST: t.ClassVar[t.Literal[r"^(.*?)_bry\.(\d{14})(?:\.(\d{1,9}))?\.nc$"]] = (
        r"^(.*?)_bry\.(\d{14})(?:\.(\d{1,9}))?\.nc$"
    )
    """A regex identifying full boundary or partitioned files."""
    SUFFIX: t.ClassVar[t.Literal["_bry"]] = "_bry"
    """A unique suffix found in the name of boundary files"""

    @classmethod
    def find(
        cls, search_path: Path, notfound_ok: bool = True
    ) -> Sequence["BoundaryFile"] | None:
        """Search for boundary files in the specified location.

        Parameters
        ----------
        search_path : Path
            The path to search
        notfound_ok : bool
            If False, raise an exception if no boundary files are found.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If the search path does not exist or contains no recognizable boundary files.
        """
        search_path = search_path.expanduser().resolve()

        if not search_path.exists():
            msg = f"No directory found at path: {search_path!r}"
            raise ValueError(msg)

        matches = sorted(search_path.rglob(f"*{cls.SUFFIX}*.{cls.EXT}"))
        if matches:
            return tuple(BoundaryFile(path=m) for m in matches)

        if not notfound_ok:
            msg = f"No boundary files located. Unable to continue from {search_path!r}"
            raise CstarExpectationFailed(msg)

        return None

    @classmethod
    def from_parts(
        cls,
        base: str,
        timestamp: datetime,
        segment: str | None = None,
        directory: Path | None = None,
    ) -> "BoundaryFile":
        """Create a BoundaryFile from components.

        Parameters
        ----------
        base : str
            The base name for the boundary file.
        timestamp : datetime
            The timestamp for the boundary file.
        segment : str | None
            The 0-padded segment number if partitioned, otherwise `None`.
        directory : Path | None
            The directory to contain the file. If not specified, defaults to cwd.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If the search path does not exist or contains no recognizable boundary files.
        """
        ts = timestamp.strftime(cls.FMT_TS)
        parted_clause = f".{segment}" if segment is not None else ""
        filename = f"{base}{cls.SUFFIX}.{ts}{parted_clause}.{cls.EXT}"

        if directory:
            return BoundaryFile(path=directory / filename)

        return BoundaryFile(path=Path(filename))

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: Path, _info: "ValidationInfo") -> Path:
        """Verify the supplied path meets the boundary file naming convention.

        Parameters
        ----------
        value : str
            The value of the checkpoint frequency property
        _info : ValidationInfo
            Metadata for the current validation context
        """
        if value.suffix != f".{BoundaryFile.EXT}":
            msg = f"File extension does not match expected naming convention: {value.suffix}"
            raise ValueError(msg)

        if re.fullmatch(BoundaryFile.PATTERN_RST, value.name, flags=re.ASCII):
            return value

        msg = f"File name does not match expected naming convention: {value}"
        raise ValueError(msg)

    @model_validator(mode="after")
    def _model_validate(self) -> "BoundaryFile":
        """Perform post-processing on the boundary file path.

        Returns
        -------
        ResetFile
        """
        matches = re.fullmatch(
            BoundaryFile.PATTERN_RST, self.path.as_posix(), flags=re.ASCII
        )
        if not matches:
            msg = f"File name does not match expected naming convention: {self.path}"
            raise ValueError(msg)

        self._base = matches.group(1)
        self._ts = datetime.strptime(matches.group(2), BoundaryFile.FMT_TS)
        # look for segment for partition number, e.g. <base>.<ts>.000.nc vs. <base>.<ts>.nc
        self._segment = matches.group(3)
        return self

    @property
    def timestamp(self) -> datetime:
        """Return a datetime derived from the timestamp in the boundary file name.

        Returns
        -------
        datetime
        """
        return self._ts

    @property
    def is_partitioned(self) -> bool:
        """Return `True` if the boundary file belongs to a partitioned dataset.

        Returns
        -------
        datetime
        """
        return self._segment is not None

    @property
    def partition(self) -> int | None:
        if self._segment:
            return int(self._segment)
        return None


class BoundaryFileTrxAdapter:
    """Convert a boundary file into a dictionary useful for use in an OverrideTransform."""

    @classmethod
    def adapt(cls, bry_files: Sequence[BoundaryFile]) -> dict[str, t.Any]:
        """Given a tuple of boundary files, create a dictionary containing the overrides necessary to
        execute a simulation with the restart file specified in the initial conditions.

        Parameters
        ----------
        restart_file : ResetFile
            The restart file metadata used to convert into an override mapping.

        Returns
        -------
        Mapping[str, t.Any]
        """
        return {
            "forcing": {
                "boundary": {
                    "data": [
                        {
                            "location": bry.path.as_posix(),
                            "partitioned": bry.is_partitioned,
                        }
                        for bry in bry_files
                    ],
                },
            },
        }


class ContinuanceTransform(Directive, OverrideTransform):
    """A transform that locates a restart file with an unknown path at the
    time the task was scheduled.
    """

    def __init__(self, config: dict[str, t.Any]) -> None:
        """Initialize the instance.


        Parameters
        ----------
        config : dict[str, t.Any] | None
            A dictionary containing configuration for the directive.
        """
        Directive.__init__(self, config)
        OverrideTransform.__init__(self, self._create_initial_condition_overrides())

    def _create_initial_condition_overrides(
        self,
    ) -> dict[str, t.Any]:
        """Create an overrides dictionary that will result in the modified blueprint
        using a

        Returns
        -------
        dict[str, t.Any]

        Raises
        ------
        ValueError
            If unknown or invalid configuration is supplied.
        """
        if "path" not in self._config:
            msg = "Invalid continuance transform configuration. Only restart paths are supported."
            raise NotImplementedError(msg)

        search_path = Path(self._config["path"])
        if restart_file := RestartFile.find(search_path, notfound_ok=False):
            return RestartFileTrxAdapter.adapt(restart_file)

        msg = f"No restart file located in search path: {search_path!r}"
        raise ValueError(msg)

    @t.override
    @staticmethod
    def suffix() -> str:
        """Return a suffix used when persisting a resource modified by this transform.

        Returns
        -------
        str
        """
        return "cfrom"


class NestingTransform(Directive, OverrideTransform):
    """A transform that uses a restart file and boundary conditions from a previous parent simulation."""

    def __init__(self, config: dict[str, t.Any]) -> None:
        """Initialize the instance.


        Parameters
        ----------
        config : dict[str, t.Any] | None
            A dictionary containing configuration for the directive.
        """
        Directive.__init__(self, config)
        OverrideTransform.__init__(
            self, self._create_initial_condition_and_bc_overrides()
        )

    def _create_initial_condition_and_bc_overrides(self) -> dict[str, t.Any]:
        """Create an overrides dictionary that will result in the modified blueprint"""
        if "rst_path" not in self._config:
            msg = "Invalid nesting transform configuration. Must include rst_path."
            raise NotImplementedError(msg)

        if "bry_path" not in self._config:
            msg = "Invalid nesting transform configuration. Must include bry_path."
            raise NotImplementedError(msg)

        search_path = Path(self._config["rst_path"])
        if restart_file := RestartFile.find(search_path, notfound_ok=False):
            rst_override_dict = RestartFileTrxAdapter.adapt(restart_file)

        else:
            msg = f"No restart file located in search path: {search_path!r}"
            raise ValueError(msg)

        search_path = Path(self._config["bry_path"])
        if boundary_files := BoundaryFile.find(search_path, notfound_ok=False):
            bry_override_dict = BoundaryFileTrxAdapter.adapt(boundary_files)
        else:
            msg = f"No boundary files located in search path: {search_path!r}"
            raise ValueError(msg)

        return {**rst_override_dict, **bry_override_dict}

    @t.override
    @staticmethod
    def suffix() -> str:
        """Return a suffix used when persisting a resource modified by this transform.

        Returns
        -------
        str
        """
        return "nfrom"


class DirectiveConfig(BaseModel):
    directive_map: t.ClassVar[dict[str, type[Directive]]] = {}
    """Lookup for all registered directives."""

    directives: KeyValueStore
    """Generic configuration container for an instance of a directive."""

    @classmethod
    def apply_directives(
        cls,
        directive_uri: str,
        blueprint_uri: str,
    ) -> str:
        """Apply the specified directives to the blueprint and
        return the path to the final, transformed blueprint.

        Parameters
        ----------
        directive_uri : str
            The URI to configuration for directives the runner must execute.
        blueprint_uri : str
            The user-supplied blueprint URI specifying the blueprint to preprocess.

        Returns
        -------
        str
        """
        with (
            local_copy(directive_uri) as local_path,
            local_copy(blueprint_uri) as local_bp,
        ):
            model = deserialize(local_path, DirectiveConfig)

            directives = model.directives
            if not directives:
                return blueprint_uri

            step = LiveStep(
                name="directive-step",
                application=Application.ROMS_MARBL,
                blueprint=local_bp,
            )
            directive_map = DirectiveConfig.directive_map
            transforms = {
                directive_map[key](config=t.cast("dict[str, dict[str, t.Any]]", config))
                for key, config in directives.items()
            }
            for transform in transforms:
                step = transform(step)[0]

        return str(step.blueprint_path)

    @classmethod
    def register(cls, key: str, directive: type[Directive]) -> None:
        DirectiveConfig.directive_map[key] = directive
