.. _forge-index:

Domain generation (Forge)
==========================

.. image:: ../images/csforge.png
   :alt: C-Star Forge logo
   :align: center
   :width: 300px

C-Star is built on a system of **applications** (a model or computation you want to
run) and **blueprints** (the inputs to an application that make its result
reproducible). **Forge** is the built-in application for *creating* new ROMS-MARBL
domains.

Setting up a regional ocean simulation has traditionally meant weeks of bespoke
work: designing a grid, collecting and regridding forcing datasets, hand-editing
model configuration files, and hoping the result is reproducible on the next
machine. Forge automates that path for ROMS-MARBL domains. You describe *what*
you want -- a region, a resolution, a time window, forcing sources -- and Forge
produces everything the model needs to run, in a form that C-Star can build and
execute anywhere.

The whole workflow revolves around two YAML documents:

- A **forge blueprint** describes the domain you want. It is the single input to
  Forge, and it is complete: given the same forge blueprint, Forge generates the
  same setup.
- A **ROMS-MARBL blueprint** (see :doc:`../blueprints`) describes the setup Forge
  generated -- the model code, input files, and runtime settings of a concrete,
  runnable simulation. It is Forge's output, and C-Star's input.

How it works
-------------

Forge takes you from "I want a regional ROMS-MARBL domain here" to a running
simulation in three conceptual steps:

1. **Build a forge blueprint.** An interactive **wizard** -- a point-and-click
   web form, also usable inside Jupyter -- walks you through the choices: a model
   spec, a domain from the bundled catalog (or your own), forcing sources,
   output settings. The result is saved as a single ``forge_blueprint.yaml``.
   Because the wizard is just a front-end for writing this file, you can also
   start from an example blueprint and edit it by hand.

2. **Process the blueprint.** The Forge executor consumes the forge blueprint
   on the machine where the data should live. It fetches and prepares the
   source datasets, generates every ROMS input file (grid, initial conditions,
   surface and boundary forcing, rivers, tides), renders the model settings,
   and emits the ROMS-MARBL blueprint describing the finished setup.

3. **Run the simulation.** C-Star consumes the ROMS-MARBL blueprint to fetch
   and compile the model code and execute the simulation -- on your laptop or on
   a supported HPC system. Forge is out of the picture at this point: the
   handoff is the blueprint file alone.

The input files are generated with `ROMS Tools <https://roms-tools.readthedocs.io/en/latest/index.html>`__,
drawing on GLORYS (ocean reanalysis), ERA5 (atmospheric reanalysis), UNIFIED_BGC
(biogeochemical climatology), SRTM15 (bathymetry), DAI/GLOFAS (river discharge),
and TPXO (tides).

Each step can happen on a different machine. A common pattern is building the
blueprint in a browser on your laptop, processing it on the cluster where the
forcing data lives, and running the simulation through C-Star's scheduler
support on that same cluster.

Install
-------

Forge ships as part of ``cstar-ocean`` -- there is no separate package to
install. Install ``cstar-ocean`` as described in :doc:`../installation`, then
verify Forge is available:

.. code-block:: console

   cstar --version
   cstar forge --help

Getting started
----------------

This walkthrough takes you from an installed environment to a running toy
simulation on a laptop or workstation.

Register for data access
~~~~~~~~~~~~~~~~~~~~~~~~~

Forge downloads forcing data from open datasets
(`documented in roms-tools <https://roms-tools.readthedocs.io/en/latest/datasets.html>`__).
Most access is automatic and anonymous -- ERA5 atmospheric forcing, for example,
needs no account at all. Two sources require a one-time (free) registration,
each with a small setup step afterwards.

.. _forge-data-access-glorys:

Copernicus Marine (GLORYS)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

GLORYS ocean-state data provides initial and boundary conditions, and Forge
downloads it on demand through the Copernicus Marine toolbox. **Required for
essentially every domain**, including the toy example below.

1. `Sign up for the Copernicus Marine Service <https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service>`__.
2. Authenticate once, using the ``copernicusmarine`` CLI that ships with
   ``cstar-ocean``'s dependencies:

   .. code-block:: console

      copernicusmarine login    # prompts for your username and password

   This stores a credentials file under ``~/.copernicusmarine/`` that every
   future run picks up automatically.

For non-interactive contexts (HPC batch jobs, CI), set the environment
variables ``COPERNICUSMARINE_SERVICE_USERNAME`` and
``COPERNICUSMARINE_SERVICE_PASSWORD`` instead of running the login step.

.. _forge-data-access-tpxo:

TPXO (tidal forcing)
^^^^^^^^^^^^^^^^^^^^^

TPXO tidal constituents are needed **only for domains with tidal forcing** --
not for the toy example below. TPXO's license doesn't permit automated
fetching, so Forge cannot download it for you.

1. `Sign up for TPXO access <https://www.tpxo.net/global>`__.
2. You'll receive download instructions from the TPXO team. Download the
   **TPXO10.v2a** netCDF files and place them under your source-data directory
   in exactly this layout:

   .. code-block:: text

      <source_data>/TPXO/TPXO10.v2a/grid_tpxo10v2a.nc
      <source_data>/TPXO/TPXO10.v2a/h_tpxo10.v2a.nc
      <source_data>/TPXO/TPXO10.v2a/u_tpxo10.v2a.nc

Find your ``source_data`` directory with ``cstar forge show-paths``. If
anything is missing at processing time, Forge fails with a message listing
the exact expected paths.

Other user-staged datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A few optional datasets follow the same "you download, Forge verifies"
pattern as TPXO -- for example WOA climatology, GLOFAS river discharge, and
EMOD topography. Each handler checks a documented location under
``<source_data>/`` and raises with instructions if files are missing; see
:doc:`source_data` for the full inventory.

Build a forge blueprint with the wizard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

   cstar forge wizard        # serves the wizard at http://localhost:8866

In the wizard: pick a model spec, then pick the **wio-toy** domain from the
catalog -- a deliberately tiny (20x20x10) Western Indian Ocean domain that
processes in minutes and exists exactly for first runs like this one. Review
the resolved YAML in the Review pane, then **Save** (or **Download**)
``forge_blueprint.yaml``.

.. tip::

   In a hurry? A ready-made wio-toy blueprint ships in the repository at
   :download:`docs/forge-blueprint-example.wio-toy.yaml
   <../forge-blueprint-example.wio-toy.yaml>`. You can skip the wizard
   entirely and process it directly.

.. note::

   **Where your work is saved.** Blueprints and workplans you save from the
   wizard, and any specs you register to the catalog, land in your own
   writable catalog layer at ``~/cstar/catalog`` (``blueprints/``,
   ``workplans/``, and spec directories) -- never inside the installed
   package. The bundled examples (like ``wio-toy`` above) stay visible
   alongside your own in the wizard's dropdowns, read-only, marked with a
   ``(bundled)`` badge. Set ``CSTAR_CATALOG`` to point the writable layer
   elsewhere (see :doc:`catalog`).

Process the blueprint
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

   cstar blueprint run path/to/forge_blueprint.yaml

This fetches the source data (GLORYS, ERA5 -- expect the first run to spend
most of its time downloading), generates all ROMS input files, renders the
model settings, and emits a **ROMS-MARBL blueprint** under the blueprint's
``working_dir`` (for wio-toy:
``~/cstar/_forge_bp_runs/cson_roms-marbl_v0.1_wio-toy_10procs/``). The final
line of output tells you exactly what to do next:

.. code-block:: text

   Blueprint: ~/cstar/_forge_bp_runs/.../roms_marbl_blueprint.yaml
   Run it with:  cstar blueprint run <path>

.. tip::

   Power-user options (stage selection, ``--clobber``, dask tuning,
   verbosity) are available via the dedicated ``cstar forge run
   path/to/forge_blueprint.yaml`` entry point (see ``cstar forge run
   --help``).

Run the simulation
~~~~~~~~~~~~~~~~~~~

.. code-block:: console

   cstar blueprint run <path-to-roms_marbl_blueprint.yaml>

Both steps use the same ``cstar blueprint run`` command; each blueprint's
``application`` field tells C-Star which application processes it. The forge
blueprint from the previous step (``application: forge``) is handled by
Forge, the application built into ``cstar-ocean``. The generated
**ROMS-MARBL blueprint** (``application: roms_marbl``), with all of the
inputs needed to execute the simulation, is handled by the ``roms_marbl``
application, also built into C-Star.

C-Star fetches and compiles the model code and executes the simulation;
outputs land under the same working directory. See :doc:`../blueprints` and
:doc:`../workplans` for more on run management and analysis.

Using the wizard from a login node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Login nodes have no browser; bind locally and SSH-forward from your laptop:

.. code-block:: console

   # on the login node:
   cstar forge wizard --no-browser
   # on your laptop:
   ssh -N -L 8866:localhost:8866 <user>@<login-node>
   # then open http://localhost:8866 locally

If your HPC provides a Jupyter interface, it may be more convenient to use
the wizard notebook instead of the Voila web app. Run ``cstar forge
copy-notebook`` to place a runnable copy at
``~/cstar/forge-blueprint-wizard.ipynb`` (use ``--dest`` to choose another
spot, and re-run with ``--force`` after upgrading ``cstar-ocean`` to refresh
it).

The wizard served via ``cstar forge wizard`` needs no Jupyter kernel
registration. But if you want to open notebooks in a **cluster's central
Jupyter installation** (e.g. an HPC OnDemand portal), that external Jupyter
must be told about your environment's kernel -- with an activation wrapper,
so shell magics and environment-activation-dependent packages work inside
notebooks:

.. code-block:: console

   cstar env register-kernel   # with the environment active; --help for options

Alternatively, Forge is designed so that you can build your forge blueprints
on one machine and process the data on another; feel free to run the wizard
from your laptop, upload your blueprint to your HPC, and process it there
from the command line.

Next steps
-----------

- Browse the bundled **domain catalog** (:doc:`catalog`) in the wizard, or
  customize a domain's grid parameters.
- See :doc:`reference` for the forge blueprint and model spec schemas, and
  :doc:`internals` for the developer-facing architecture guide.
