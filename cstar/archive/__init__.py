"""Archive the data files of a finished run: detect their types, compress them.

`detect` groups a run's data files into file types from their headers; `policy`
holds the per-type answers (the `compress` blueprint's content) and applies them.
"""
