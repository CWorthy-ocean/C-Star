A GitHub or http(s) URL in ``CSTAR_CATALOG`` is read as one catalog location; on Linux and macOS C-Star split it at its ``:``, so the catalog it named was never read.
- A catalog served over http(s) from somewhere other than GitHub can be read; before, C-Star found nothing in it, from ``CSTAR_CATALOG`` or from the wizard's catalog bar.
- A single URL entered in the wizard's catalog bar loads that catalog again.
