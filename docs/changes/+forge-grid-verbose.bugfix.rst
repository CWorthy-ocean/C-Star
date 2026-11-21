Forge runs on the bundled ``hvalfjordur-0`` and ``ccs-12km`` domains, or any domain whose ``grid_kwargs`` holds ``verbose``, no longer fail with "got multiple values for keyword argument 'verbose'".
- Only ``cstar forge run --verbose`` sets roms-tools verbosity; a ``verbose`` key in ``grid_kwargs`` is ignored.
