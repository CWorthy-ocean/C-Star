import asyncio
import re
import typing as t

from pydantic import BaseModel, ConfigDict, Field
from rich.console import Console
from rich.table import Column, Table

from cstar.base.log import get_logger
from cstar.orchestration.dag_runner import DagStatus
from cstar.orchestration.orchestration import Status
from cstar.orchestration.tracking import TrackingRepository

console = Console()
log = get_logger(__name__)


def list_runs(incomplete: str) -> list[tuple[str, str]]:
    """Retrieve a list of all recorded run-ids.

    Parameters
    ----------
    incomplete : str
        Any value from the user is provided to autocompletion.

    Returns
    -------
    t.Iterable[str]
    """
    repo = TrackingRepository()
    run_list = asyncio.run(repo.list_latest_runs(incomplete))

    if not run_list and incomplete:
        return [(incomplete, "no results found")]
    elif not run_list:
        return [("run-id", "no results found")]

    return [(r.run_id, f"Workplan path: {r.workplan_path}") for r in run_list if r]


def checkmark(color: str) -> str:
    return f"[{color}]:heavy_check_mark:"


def display_summary(
    run_id: str,
    dag_status: DagStatus,
) -> None:
    """Display a summary describing the current state of
    a DAG executed by the orchestrator.

    Parameters
    ----------
    open_set : Iterable[str] | None
        The names of jobs that are unstarted or incomplete.
    open_set : Iterable[str] | None
        The names of jobs that have completed.
    """
    # don't pad the top and bottom but give some horizontal space
    padding = (0, 1)

    table = Table(
        Column(header="Step", justify="right"),
        Column(header="Ready", justify="center"),
        Column(header="Running", justify="center"),
        Column(header="Done", justify="center"),
        Column(header="Failed", justify="center"),
        Column(header="Cancelled", justify="center"),
        title=f"Run [yellow]{run_id}[/yellow] Results",
        show_lines=True,
        padding=padding,
        pad_edge=False,
    )

    for task_name, status in sorted(dag_status.details.items()):
        table.add_row(
            task_name,
            checkmark("green") if Status.is_ready(status) else "",
            checkmark("cyan") if Status.is_running(status) else "",
            checkmark("green") if status == Status.Done else "",
            checkmark("red") if status == Status.Failed else "",
            checkmark("yellow") if status == Status.Cancelled else "",
        )

    console.print(table)


class DelimitedKeyValuePair(BaseModel):
    """Model a delimited key-value pair in the form <key>=<value>, ensuring
    the key and value are both non-empty.
    """

    RE_KVP: t.ClassVar[t.Literal[r"(\w+)=(\w+)"]] = r"(\w+)=(\w+)"
    """Regex used to parse key and value groups from raw input."""

    raw: t.Annotated[str, Field(kw_only=False, min_length=3, pattern=RE_KVP)]
    """The raw key value pair, including delimiter."""

    _kvp: tuple[str, str] | None = None
    """The key-value pair parsed from the raw input."""

    model_config: t.ClassVar[ConfigDict] = ConfigDict(str_strip_whitespace=True)
    """Model configuration ensuring keys and values have whitespace stripped."""

    def _to_tuple(self) -> tuple[str, str]:
        """Split the raw value into a key and value.

        Returns
        -------
        tuple[str, str]
            Tuple containing key and value.
        """
        if match := re.search(DelimitedKeyValuePair.RE_KVP, self.raw):
            return match.group(1), match.group(2)

        raise ValueError

    @property
    def kvp(self) -> tuple[str, str]:
        """Return the key-value pair parsed from an input value as a [key, value] tuple.

        Returns
        -------
        tuple[str, str].
        """
        if not self._kvp:
            self._kvp = self._to_tuple()
        return self._kvp

    @staticmethod
    def to_map(value: str | list[str], kvp_delimiter: str = ",") -> t.Mapping[str, str]:
        """Convert the input into 1-to-many key-value pairs.

        Parameters
        ----------
        value : str | list[str]
            A delimited string (e.g. "a=A,b=B") or a list of key-value pairs.
        """
        if isinstance(value, str):
            value = value.strip().split(kvp_delimiter)
        if not value:
            return {}

        parsed = [DelimitedKeyValuePair(raw=value).kvp for value in value or []]
        return {kvp[0]: kvp[1] for kvp in parsed}
