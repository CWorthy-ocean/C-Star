import typing as t

from cstar.orchestration.orchestration import RunRequest

TFormattable = t.TypeVar("TFormattable", contravariant=True)


class ModelFormatter(t.Protocol, t.Generic[TFormattable]):
    """Formats a `RunRequest` as a string."""

    def format(self, value: TFormattable) -> str:
        """Format the value.

        Parameters
        ----------
        request : RunRequest
            The request to be formatted.

        Returns
        -------
        str
        """
        ...


class RunRequestCommandFormatter(ModelFormatter[RunRequest]):
    """Format a `RunRequest` as a request as a CLI command."""

    def format(self, value: RunRequest) -> str:
        variables = " ".join(f"{k}='{v}'" for k, v in value.environment.items())
        cmd = " ".join(value.command)

        return f"{variables} {cmd}".strip()


class RunRequestScriptFormatter(ModelFormatter[RunRequest]):
    """Format a `RunRequest` as script content."""

    def format(self, value: RunRequest) -> str:
        command = " ".join(value.command)
        exports = ";".join(f"export {k}='{v}'" for k, v in value.environment.items())

        if exports:
            return f"{exports}; {command};"

        return f"{command};"
