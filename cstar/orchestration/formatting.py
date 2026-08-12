import typing as t

TFormattable = t.TypeVar("TFormattable", contravariant=True)


class ModelFormatter(t.Protocol, t.Generic[TFormattable]):
    """Formats a `RunRequest` as a string."""

    def _apply_replacements(
        self,
        value: str,
        replacements: dict[str, str] | None = None,
    ) -> str:
        if not replacements:
            return value

        for k, v in replacements.items():
            value = value.replace(k, v)
        return value

    def _to_string(self, value: TFormattable) -> str: ...

    def format(
        self,
        value: TFormattable,
        updates: dict[str, str] | None = None,
    ) -> str:
        """Format the value.

        Parameters
        ----------
        value : TFormattable
            The value to be formatted.
        updates : dict[str, str]
            A mapping of string replacements to perform prior to final formatting.

        Returns
        -------
        str
        """
        s = self._to_string(value)

        if updates:
            s = self._apply_replacements(s, updates)
        return s
