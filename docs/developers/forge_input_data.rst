.. _forge-input-data:

Forge internals: input data generation
======================================

Input generation is the middle stage of processing a forge blueprint: it turns
the staged :doc:`source datasets <forge_source_data>` into the netCDF inputs a
UCLA-ROMS run reads. It lives in ``cstar/applications/forge/input_generator.py``
and runs inside ``ForgeExecutor.generate_inputs()``, which builds a
``RomsInputGenerator`` and calls its ``generate_all()``. Normal use goes through
``cstar forge run`` or ``cstar blueprint run``; constructing the generator by hand
is only for debugging.

What it produces
----------------

Each kind of input is a handler registered with ``@register_input(name, order,
label)`` in ``INPUT_REGISTRY``, and ``generate_all()`` runs the ones the blueprint
asks for in ``order``:

.. list-table::
   :header-rows: 1
   :widths: 32 10 58

   * - Input
     - Order
     - Notes
   * - ``grid``
     - 10
     - Always generated. Also the child grid when nesting.
   * - ``initial_conditions``
     - 20
     - Physics and, with MARBL, biogeochemistry.
   * - ``forcing.surface``
     - 30
     - Physics, biogeochemistry and restoring streams.
   * - ``forcing.boundary``
     - 40
     - Skipped for a nested child, which takes its boundaries from the parent.
   * - ``forcing.tidal``
     - 50
     -
   * - ``forcing.river``
     - 60
     - From Dai or GloFAS, or a user-supplied river forcing file.
   * - ``cdr_forcing``
     - 80
     - Generated, or a user-supplied file. In ``bgc_mode: cdr_lite`` the CDR
       tracer counts are read from the generated file.
   * - ``forcing.carbonate_sensitivity``
     - 85
     - User-supplied ``_cdrgas`` files, staged rather than generated.

A handler writes its netCDF file(s) to ``input_data_dir`` as
``{domain_name}_{input}.nc`` (dots in the domain name become underscores), writes
a ROMS-Tools YAML sidecar beside the UCLA-ROMS blueprint, adds the file to the
matching section of the UCLA-ROMS blueprint it is building, and records any
settings it derives. The settings dicts are the executor's own, shared by
reference, so what a handler writes is what ``configure_build()`` renders.

Where the source paths come from
--------------------------------

Before calling ROMS-Tools, each ``source``/``bgc_source`` block is completed by
``_resolve_source_block()``: a source with an explicit ``path`` is used as given, a
streamable one (ERA5) is left without a path, and any other gets the staged path
from ``SourceDatasets.path_for_source()``. A multi-file GLORYS source is then
either trimmed to the run window or replaced by a just-in-time kerchunk reference
(``--no-subchunk`` turns that off). The block's ``options`` mapping is passed
straight to the ROMS-Tools constructor.

After generation, when the build does not use ParallelIO, ``_partition_files()``
splits every input across the processor layout with ROMS-Tools'
``partition_netcdf``, beside the whole files.

Adding an input
---------------

1. Add a ``_generate_<x>(self, key="<name>", **kwargs)`` method to
   ``RomsInputGenerator`` and register it with ``@register_input(name=...,
   order=..., label=...)``, choosing an ``order`` that runs after anything it
   depends on.
2. Write the file through the class's filename helper, append a ``Resource`` to
   the matching UCLA-ROMS blueprint section, and set any settings the input
   implies.
3. Make the resolver emit the new input for the blueprints that need it, and
   thread the selection through ``sources_to_forcing_override()`` (``engine.py``)
   so it reaches the generator. A field that resolves correctly but is dropped
   on the way is the usual bug here.
4. Cover it in the Forge unit tests, and in the integration tier if it reads real
   source data.
