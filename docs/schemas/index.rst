Schema Reference
################

Workplans
---------

Schemas for the built-in `Workplan` are listed here.

- :download:`Workplan Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/wp/workplan_schema.1.0.0.json>`
- :download:`Workplan Schema v1.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/wp/workplan_schema.1.1.0.json>`:
  adds the run a ``runs`` alias resolved to when the workplan was scheduled (``id``, ``path``).
- :download:`Workplan Schema v1.2.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/wp/workplan_schema.1.2.0.json>`:
  adds the version of the schema the workplan was written with (``schema_version``); a workplan without one reads as 1.0.0, and a newer one is refused.

Runs
----

Schemas for the files a run leaves behind are listed here. See
:ref:`workplan_run_manifest`.

- :download:`Run Manifest Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/run_manifest_schema.1.0.0.json>`:
  ``cstar-run.yaml``, the description of a run: its attempts, its steps and what it depends on.
- :download:`Step Record Schema v1.0.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/step_record_schema.1.0.0.json>`:
  ``cstar-step.yaml``, what one step did, written by the step itself.
- :download:`Run Manifest Schema v1.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/run_manifest_schema.1.1.0.json>`:
  adds the inputs each attempt carried (``directory``, ``carried``) and each step's carried blueprint (``carried_blueprint``).
- :download:`Step Record Schema v1.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/step_record_schema.1.1.0.json>`:
  adds the blueprint file the step ran (``blueprint_file``).
- :download:`Run Manifest Schema v1.2.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/run_manifest_schema.1.2.0.json>`:
  adds each step's latest submission (``launcher``, ``job_id``, ``submitted_at``).
- :download:`Step Record Schema v1.2.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/step_record_schema.1.2.0.json>`:
  adds what ran the step (``execution``).
- :download:`Run Manifest Schema v1.3.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/run_manifest_schema.1.3.0.json>`:
  adds the uuid of each external run (``id``).
- :download:`Run Manifest Schema v1.4.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/run_manifest_schema.1.4.0.json>`:
  adds the content digest of each product (``digest``).
- :download:`Step Record Schema v1.3.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/step_record_schema.1.3.0.json>`:
  adds the content digest of each product (``digest``).
- :download:`Run Manifest Schema v1.5.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/run/run_manifest_schema.1.5.0.json>`:
  adds no fields: each step's ``definition_hash`` is tagged with the recipe that computed it (``def-v2:<hex>``: current application names, fields at their defaults left out), and a hash with another tag or none, such as every hash an older manifest recorded, is not compared when its run is re-entered.

Blueprints
----------

Schemas for the built-in blueprints are listed here. Each blueprint's
``schema_version`` is semver read from an older file's point of view: a newer
major means older files need migration (automatic where the application provides
an adapter), while an older minor or patch loads unchanged. See
:ref:`versioning_blueprint_schema`.

UCLA-ROMS Blueprint
~~~~~~~~~~~~~~~~~~~

The `ucla_roms` blueprint enables the orchestration of an application that
executes a UCLA-ROMS simulation.

- :download:`UCLA-ROMS Blueprint Schema v3.3.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/ucla_roms/ucla_roms_schema.3.3.0.json>`

ROMS-MARBL Blueprint (earlier name)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These schemas were published under the application's earlier name, `roms_marbl`.

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
UCLA-ROMS runs with MARBL.

- :download:`CDR Metrics Blueprint Schema v1.1.0 <https://raw.githubusercontent.com/CWorthy-ocean/C-Star/refs/heads/main/docs/schemas/bp/cdr_metrics/cdr_metrics_schema.1.1.0.json>`


Forge Blueprint
~~~~~~~~~~~~~~~

The `forge` blueprint (see :doc:`../applications/forge/index`) configures the domain-generation
application that produces UCLA-ROMS blueprints; its JSON schema is not yet published
here.
