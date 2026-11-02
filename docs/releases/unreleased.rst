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


- Forge blueprints accept ``bottom_fill_blend_m`` in ``initial_conditions`` and ``boundary``: the depth below a source column's bottom over which tracers blend into the nearest column with water at that depth. (`#877 <https://github.com/CWorthy-ocean/C-Star/pull/877>`_)

  - ``0`` takes the neighbouring column's value at once, ``.inf`` copies the bottom value down, and leaving it unset uses roms-tools' default.
  - The wizard has a "Below-bottom blend depth" box in the initial-conditions and boundary interpolation settings.
  - Setting a value needs a roms-tools release with the option; blueprints that leave it unset work with any release.

- ``nest_ic`` blueprints accept ``bottom_fill_blend_m`` for the child's initial conditions, where the child is deeper than the parent (blueprint schema 1.2.0). (`#877 <https://github.com/CWorthy-ocean/C-Star/pull/877>`_)

Bug Fixes
~~~~~~~~~


- Pre-run now refuses a namelist file name too long for ROMS (more than 250 characters, or more than 220 for ``output_root_name``/``extract_root_name``), listing each one with its length; before, pre-run passed and ROMS stopped at startup (ucla-roms >= 0.13.0) or cut the name off silently (earlier releases). (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- Forge names a boundary or surface BGC file whose ``use_vars`` tag would exceed 40 characters ``<source>-<n>vars_<digest>`` (e.g. ``boundary-bgc-unified-23vars_3581d4be.nc``) instead of joining every variable; a domain already generated under the old long name keeps and reuses its file. (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- Forge records its surface-BGC files as products of the run (it was looking for them under names it never writes). (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- An infinite value (``.inf``) in a blueprint or a workplan step's overrides was turned into ``null`` when saved, hashed or template-filled; it now stays infinite. (`#877 <https://github.com/CWorthy-ocean/C-Star/pull/877>`_)

Improvements
~~~~~~~~~~~~

- N/A

Miscellaneous
~~~~~~~~~~~~~

- Published the ``nest_ic`` 1.2.0 blueprint schema and described the new field on the ``nest_ic`` page. (`#877 <https://github.com/CWorthy-ocean/C-Star/pull/877>`_)
