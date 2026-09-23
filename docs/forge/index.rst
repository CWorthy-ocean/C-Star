.. _forge-index:

Forge
=====

.. image:: ../images/csforge.png
   :alt: C-Star Forge logo
   :align: center
   :width: 300px

Forge is C-Star's application for creating new ROMS-MARBL domains. Setting up
a regional simulation has traditionally meant weeks of work: designing a grid,
collecting and regridding forcing datasets, hand-editing configuration files,
and hoping the result reproduces on the next machine. Forge automates that
path. You describe what you want (a region, a resolution, a time window,
forcing sources) in a :doc:`forge blueprint <../blueprints/forge>`, and Forge
produces everything the model needs to run, in a form C-Star can build and
execute anywhere.

How it works
------------

Forge has two sides separated by the blueprint.

**Authoring** happens in the :doc:`wizard <../wizard>`. It composes a forge
blueprint from :doc:`specs <specs>` in the :doc:`catalog <../catalog>`, a
model configuration, a domain, a forcing selection and output settings, plus
your edits, and writes out the resolved values in full.

**Processing** happens when you run the blueprint with
``cstar blueprint run`` (or ``cstar forge run`` for the full option set). On
whatever machine you run it, Forge:

1. stages the :doc:`source datasets <source_datasets>` the blueprint needs,
   downloading what it can and checking for the files you must provide;
2. generates the input files with
   `ROMS-Tools <https://roms-tools.readthedocs.io>`__: the grid, initial
   conditions, surface and boundary forcing, and tides, rivers and carbon
   dioxide removal forcing where requested;
3. renders the model's compile-time switches (``cppdefs.opt``) and run-time
   namelist (``namelist.nml``) from the blueprint's settings;
4. writes a :doc:`ROMS-MARBL blueprint <../blueprints/roms_marbl>` that
   points at all of the above.

Running that ROMS-MARBL blueprint is a normal C-Star simulation; Forge is no
longer involved. The :doc:`end-to-end tutorial <../tutorials/end_to_end>`
walks through the whole sequence on a toy domain.

The datasets Forge draws on include GLORYS ocean reanalysis, ERA5
atmospheric reanalysis, a unified biogeochemical climatology, SRTM15 and
ETOPO bathymetry, river discharge from GloFAS and Dai, and TPXO tides. Two
of them require a free registration; see :doc:`../data_access`.

Pages in this section
---------------------

- :doc:`specs`: the catalog entries a blueprint is composed from, and the
  ``model.yaml`` format.
- :doc:`source_datasets`: every source Forge knows about, how each is
  obtained, and where user-staged files go.

Elsewhere: :doc:`../blueprints/forge` for the blueprint itself, its
processing options and outputs; :doc:`../wizard` for the interface;
:doc:`../catalog` for where specs and saved blueprints live; :doc:`../hpc`
for data locations on clusters; and :doc:`../developers/forge_internals` for
the architecture.
