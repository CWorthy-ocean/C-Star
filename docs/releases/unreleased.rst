.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- Remote netCDF sources that were previously misclassified as text are now retrieved as binary and sha256-verified against file_hash when one is given. A stale file_hash on such a source can now fail with a hash mismatch where it previously passed silently. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)

New features
~~~~~~~~~~~~


- ``build_forge_blueprint`` emits a ``UserWarning`` under ``use_pio=True`` listing every river ``custom_file`` or ``cdr_forcing_file`` that is not classic-format netCDF, explaining the conversion Forge will perform and how to pre-convert by hand. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- When staging a user-provided file under ``use_pio``, Forge converts netCDF-4 files to CDF-5 with ``nccopy -k cdf5``, keeps the original untouched, and logs a warning that the conversion happened. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- A workplan step may set ``blueprint: inline``; its blueprint is then built from the application's model plus the step's ``blueprint_overrides``, so no blueprint file is needed for fully-overridden steps. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- Inline steps are validated at ``cstar workplan check`` and at ``cstar workplan run`` preflight, reporting every missing required field in one message; the ``blueprint`` field itself stays required, so a forgotten line is still a schema error. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- The merged blueprint is written to the step's work directory (``tasks/<step>/work/blueprint.yaml``) when the workplan is scheduled, so the run artifacts record exactly what ran and reload/resume need no special handling. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)

Bug Fixes
~~~~~~~~~


- Classic-format netCDF inputs (CDF-1/2/5, e.g. forge use_pio output) were often misclassified as text, and validate() then rejected them with a "text file (e.g. a roms-tools YAML)" TypeError. Before that check existed, the same misclassification also made local files get copied with shutil.copy2 instead of symlinked, and made remote files get read whole into memory and skip sha256 verification. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- A 0-byte local input file now raises a clear "is empty" ValueError from validate() instead of being reported as a text/YAML file. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- Local binary inputs given by a relative path are now symlinked by their resolved path, so the staged symlink no longer dangles. This already affected netCDF4/HDF5 inputs, and the classification fix above would otherwise have extended it to classic-format files that used to be copied as text. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- A failed or interrupted ``nccopy`` conversion no longer leaves a partial output file behind that the no-clobber reuse logic would pick up on the next run. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- A user-provided file that was already classic-format is now copied as-is under ``use_pio`` rather than being needlessly re-encoded, so ``nccopy`` is only required when a conversion is actually needed. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- A failed or interrupted CDF-5 conversion of a Forge-generated input (or a nest_ic / upscaler output) no longer leaves a truncated file at the final path that no-clobber reuse logic would treat as finished on the next run. (`#731 <https://github.com/CWorthy-ocean/C-Star/pull/731>`_)

Improvements
~~~~~~~~~~~~


- A user-provided river or CDR file that already sits at its Forge output path and is netCDF-4 now raises under ``use_pio`` instead of being silently accepted and failing later in ROMS. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- Added ``NetCDFFormat`` and ``netcdf_format()`` to ``cstar.base.utils`` as the single owner of magic-number format detection; ``check_nc_pio_compatible`` now uses it instead of an inline header check. (`#729 <https://github.com/CWorthy-ocean/C-Star/pull/729>`_)
- ``convert_to_cdf5`` is now the single owner of the ``nccopy -k cdf5`` call, with a ``remove_source`` option (default on) so user-provided originals can be converted without being deleted. (`#731 <https://github.com/CWorthy-ocean/C-Star/pull/731>`_)
- ``convert_to_cdf5`` refuses a conversion whose source and destination are the same file instead of letting ``nccopy`` truncate its own input. (`#731 <https://github.com/CWorthy-ocean/C-Star/pull/731>`_)

Miscellaneous
~~~~~~~~~~~~~

- Documented inline blueprints in the workplan guide; the nesting tutorial now uses ``blueprint: inline`` for its ``nest_ic`` and ``upscaler`` steps and the tutorial's ``nest_ic_bp.yaml`` dummy blueprint is removed. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
- The workplan JSON schema advertises the ``inline`` token. (`#734 <https://github.com/CWorthy-ocean/C-Star/pull/734>`_)
