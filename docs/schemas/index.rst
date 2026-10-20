Schema Reference
################

Workplans
---------

Schemas for the built-in `Workplan` are listed here.

- :download:`Workplan Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/wp/workplan_schema.1.0.0.json>`

Runs
----

Schemas for the files a run leaves behind are listed here. See
:ref:`workplan_run_manifest`.

- :download:`Run Manifest Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/run_manifest_schema.1.0.0.json>`:
  ``cstar-run.yaml``, the description of a run: its attempts, its steps and what it depends on.
- :download:`Step Record Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/step_record_schema.1.0.0.json>`:
  ``cstar-step.yaml``, what one step did, written by the step itself.

Blueprints
----------

Schemas for the built-in blueprints are listed here. Each blueprint's
``schema_version`` is semver read from an older file's point of view: a newer
major means older files need migration (automatic where the application provides
an adapter), while an older minor or patch loads unchanged. See
:ref:`versioning_blueprint_schema`.

ROMS-MARBL Blueprint
~~~~~~~~~~~~~~~~~~~~

The `roms_marbl` blueprint enables the orchestration of an application that 
executes a ROMS simulation.

- :download:`ROMS-MARBL Blueprint Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/roms_marbl/roms_marbl_schema.1.0.0.json>`
- :download:`ROMS-MARBL Blueprint Schema v2.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/roms_marbl/roms_marbl_schema.2.0.0.json>`
- :download:`ROMS-MARBL Blueprint Schema v2.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/roms_marbl/roms_marbl_schema.2.1.0.json>`
- :download:`ROMS-MARBL Blueprint Schema v3.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/roms_marbl/roms_marbl_schema.3.0.0.json>`
- :download:`ROMS-MARBL Blueprint Schema v3.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/roms_marbl/roms_marbl_schema.3.1.0.json>`
- :download:`ROMS-MARBL Blueprint Schema v3.2.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/roms_marbl/roms_marbl_schema.3.2.0.json>`


Hello World Blueprint
~~~~~~~~~~~~~~~~~~~~~

The `hello_world` blueprint is an example starting point for creating and
running custom applications with user-provided blueprints.

- :download:`Hello World Blueprint Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/hello_world/hello_world_schema.1.0.0.json>`
- :download:`Hello World Blueprint Schema v1.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/hello_world/hello_world_schema.1.1.0.json>`


Plotter Blueprint
~~~~~~~~~~~~~~~~~

The `plotter` blueprint is a more complex example of creating and
running a custom application with user-provided blueprints.


- :download:`Plotter Blueprint Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/plotter/plotter_schema.1.0.0.json>`
- :download:`Plotter Blueprint Schema v2.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/plotter/plotter_schema.2.0.0.json>`
- :download:`Plotter Blueprint Schema v2.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/plotter/plotter_schema.2.1.0.json>`


CDR Metrics Blueprint
~~~~~~~~~~~~~~~~~~~~~

The `cdr_metrics` blueprint computes CDR metrics from the `_cdr` output of
ROMS-MARBL runs.

- :download:`CDR Metrics Blueprint Schema v1.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/cdr_metrics/cdr_metrics_schema.1.1.0.json>`


Forge Blueprint
~~~~~~~~~~~~~~~

The `forge` blueprint (see :doc:`../forge/index`) configures the domain-generation
application that produces ROMS-MARBL blueprints; its JSON schema is not yet published
here.
