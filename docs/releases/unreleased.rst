.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~

- N/A

New features
~~~~~~~~~~~~

- N/A

Bug Fixes
~~~~~~~~~


- Pre-run now refuses a namelist file name too long for ROMS (more than 250 characters, or more than 220 for ``output_root_name``/``extract_root_name``), listing each one with its length; before, pre-run passed and ROMS stopped at startup (ucla-roms >= 0.13.0) or cut the name off silently (earlier releases). (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- Forge names a boundary or surface BGC file whose ``use_vars`` tag would exceed 40 characters ``<source>-<n>vars_<digest>`` (e.g. ``boundary-bgc-unified-23vars_3581d4be.nc``) instead of joining every variable; a domain already generated under the old long name keeps and reuses its file. (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- Forge records its surface-BGC files as products of the run (it was looking for them under names it never writes). (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- N/A
