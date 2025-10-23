import re


def slugify(value: str) -> str:
    """Collapse and replace whitespace characters."""
    stripped = value.strip()
    return re.sub(r"\s+", "-", stripped)
