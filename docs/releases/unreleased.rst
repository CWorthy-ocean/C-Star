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

- N/A

Bug Fixes
~~~~~~~~~


- Classic-format netCDF inputs (CDF-1/2/5, e.g. forge use_pio output) were often misclassified as text, and validate() then rejected them with a "text file (e.g. a roms-tools YAML)" TypeError. Before that check existed, the same misclassification also made local files get copied with shutil.copy2 instead of symlinked, and made remote files get read whole into memory and skip sha256 verification. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- A 0-byte local input file now raises a clear "is empty" ValueError from validate() instead of being reported as a text/YAML file. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)
- Local binary inputs given by a relative path are now symlinked by their resolved path, so the staged symlink no longer dangles. This already affected netCDF4/HDF5 inputs, and the classification fix above would otherwise have extended it to classic-format files that used to be copied as text. (`#728 <https://github.com/CWorthy-ocean/C-Star/pull/728>`_)

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- N/A
