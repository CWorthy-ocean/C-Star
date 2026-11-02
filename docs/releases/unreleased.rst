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
- The wizard's diagnostics output row has a Diagnostics switch that compiles ucla-roms with ``DIAGNOSTICS``, writing the ``_dia`` file of momentum and tracer term balances. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)

  - Off by default; while it is off, the other diagnostics settings are hidden.
  - The temperature and salinity diagnostics boxes sit in the same row and show only while tracer diagnostics are on.
  - With ParallelIO it needs ucla-roms 0.13.1 or later; Forge refuses diagnostics with ParallelIO on an older release pin, which aborts at the first diagnostics output.

- Forge blueprints and ModelSpecs accept ``cppdefs.diagnostics`` to define ``DIAGNOSTICS`` in the generated ``cppdefs.opt``. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)
- Forge blueprints accept ``groom`` on ``initial_conditions`` and ``boundary``; ``groom: false`` turns off ROMS-Tools' grooming of ESPER-derived tracers, which is on by default. (`#878 <https://github.com/CWorthy-ocean/C-Star/pull/878>`_)

  - On (the default, ROMS-Tools' own) is left out of saved blueprints and the content hash, so existing blueprints keep their hash and an explicit ``groom: true`` hashes the same as leaving it out.

- Forge blueprints accept ``groom_settings`` on ``initial_conditions`` and ``boundary``, overriding ROMS-Tools' grooming thresholds; it is set by hand in the blueprint YAML only. (`#878 <https://github.com/CWorthy-ocean/C-Star/pull/878>`_)

  - Unknown keys or invalid values fail when the blueprint loads, and so does ``groom_settings`` with ``groom: false``, since ROMS-Tools applies the settings only while grooming is on. An empty ``groom_settings`` counts as unset.

- The wizard has a "Groom ESPER tracers" checkbox on the initial-conditions and boundary sections, checked by default. It keeps any hand-written ``groom_settings`` when a blueprint is loaded and saved again with the box checked, and leaves them out while the box is unchecked. (`#878 <https://github.com/CWorthy-ocean/C-Star/pull/878>`_)

Bug Fixes
~~~~~~~~~


- Pre-run now refuses a namelist file name too long for ROMS (more than 250 characters, or more than 220 for ``output_root_name``/``extract_root_name``), listing each one with its length; before, pre-run passed and ROMS stopped at startup (ucla-roms >= 0.13.0) or cut the name off silently (earlier releases). (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- Forge names a boundary or surface BGC file whose ``use_vars`` tag would exceed 40 characters ``<source>-<n>vars_<digest>`` (e.g. ``boundary-bgc-unified-23vars_3581d4be.nc``) instead of joining every variable; a domain already generated under the old long name keeps and reuses its file. (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- Forge records its surface-BGC files as products of the run (it was looking for them under names it never writes). (`#879 <https://github.com/CWorthy-ocean/C-Star/pull/879>`_)
- An infinite value (``.inf``) in a blueprint or a workplan step's overrides was turned into ``null`` when saved, hashed or template-filled; it now stays infinite. (`#877 <https://github.com/CWorthy-ocean/C-Star/pull/877>`_)
- On Perlmutter, C-Star again loads a module set that exists (``cray-hdf5/1.14.3.9``, ``cray-netcdf/4.9.2.3``, ``cray-parallel-netcdf/1.12.3.21`` under ``PrgEnv-gnu/8.7.0`` and ``cray-mpich/9.1.0``), so ``NETCDFHOME`` and ``PNETCDFHOME`` resolve and ROMS builds on CPU nodes. (`#882 <https://github.com/CWorthy-ocean/C-Star/pull/882>`_)
- Forge wrote the diagnostics output period as a real number (``86400.0``), which ucla-roms cannot read into its integer setting, so any run with ``DIAGNOSTICS`` compiled in stopped at startup. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)

Improvements
~~~~~~~~~~~~


- Module lists (``.lmod`` files) may contain blank lines and ``#`` comments; the Perlmutter list now names its login defaults explicitly so a change of site defaults does not change the build. (`#882 <https://github.com/CWorthy-ocean/C-Star/pull/882>`_)
- The wizard no longer has a separate "Temperature and salinity output" section: the temperature and salinity checkboxes are under "Variables in history files", the only file they affect. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)
- The bundled ``roms-marbl-0.13-default`` ModelSpec (the wizard's default) now pins ucla-roms 0.13.1, which changes no namelist key. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)

Miscellaneous
~~~~~~~~~~~~~

- Published the ``nest_ic`` 1.2.0 blueprint schema and described the new field on the ``nest_ic`` page. (`#877 <https://github.com/CWorthy-ocean/C-Star/pull/877>`_)
- The macOS cells of the weekly extended integration workflow skip the nesting + upscaling workplan test, which cannot finish on the macOS runner; ubuntu cells still run it. (`#881 <https://github.com/CWorthy-ocean/C-Star/pull/881>`_)
- The system-registration developer page's ``perlmutter.lmod`` example now shows the real Perlmutter list instead of Anvil's modules. (`#882 <https://github.com/CWorthy-ocean/C-Star/pull/882>`_)
- Fixed a flaky unit test: the service-loop delay test no longer depends on how busy the CI runner is. (`#883 <https://github.com/CWorthy-ocean/C-Star/pull/883>`_)
- Bundled ModelSpecs, the bundled and documentation Forge blueprints are re-pinned to the template commit that adds ``DIAGNOSTICS``; their rendered ``cppdefs.opt`` is unchanged. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)
- The integration tests and the weekly extended CI run against ucla-roms 0.13.1. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)
- The wizard docs describe the new placement and the diagnostics switch. (`#880 <https://github.com/CWorthy-ocean/C-Star/pull/880>`_)
- The ESPER entry in the Forge source-datasets docs gives the oxygen floor as 5.0 mmol/m³ (MARBL's ``parm_o2_min``) and explains grooming, ``groom`` and ``groom_settings``. (`#878 <https://github.com/CWorthy-ocean/C-Star/pull/878>`_)
