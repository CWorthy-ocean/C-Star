A GitHub or http(s) URL in ``CSTAR_CATALOG`` is read as one catalog location; on Linux and macOS C-Star split it at its ``:``, so the catalog it named was never read.
- An http(s) catalog that is not on GitHub is read too, from a server that lists its directories (as ``python -m http.server`` does); before, C-Star found nothing in it, from ``CSTAR_CATALOG`` or from the wizard's catalog bar.
- A single URL entered in the wizard's catalog bar loads that catalog again.
- A URL as the first ``CSTAR_CATALOG`` entry is refused with an error, as ``local`` is, since the first entry is the catalog you write to; ``cstar forge run`` used to create a directory named ``https`` for it in the current directory.
