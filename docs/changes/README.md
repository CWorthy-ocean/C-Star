# Release-note fragments

Every pull request with a user-visible change adds a file here holding its
release note, named `+<name>.<type>.rst` (any short `<name>`, for example
`+river-count.bugfix.rst`) and committed with the code. The note is
reStructuredText, like the rest of the docs (double backticks for
``` ``code`` ```): one line for the release reader, optionally followed by `- `
sub-bullets. At release time each `+<name>` file is renamed after the pull
request that added it, then `towncrier build` turns the fragments into
[`docs/releases/<version>.rst`](../releases) and deletes them.

| Type              | Section in the release notes |
|-------------------|------------------------------|
| `breaking`        | Breaking Changes             |
| `breaking-python` | Python API Changes           |
| `output`          | Changes to Output            |
| `deprecation`     | Deprecations                 |
| `feature`         | New features                 |
| `bugfix`          | Bug Fixes                    |
| `improvement`     | Improvements                 |
| `misc`            | Miscellaneous                |

The [contributor guide](../contributing.rst) has the details and examples.
