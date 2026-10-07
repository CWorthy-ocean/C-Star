Custom C-Star Applications
==========================

Custom applications enable users to execute new types of behavior with C-Star. The real
power of custom applications becomes clear when they are integrated into a workplan.

For a complete, in-tree example of a custom application built this way, see
:doc:`developers/forge_internals` -- the domain-generation application
``forge`` (source at ``cstar/applications/forge/``) follows exactly the
pattern described below.


Applications Overview
---------------------

Creating an application requires three key components

1. Implement a `Blueprint` for your application. Use it to expose any
   configuration options available for your application.
2. Implement a `BlueprintRunner` for your application. A *runner* instance
   will be created by C-Star when asked to execute the associated `Blueprint`.
3. Tie the blueprint and runner together by registering an `ApplicationDefinition`.

Creating a `Blueprint`
----------------------

A `Blueprint` (see: :class:`cstar.orchestration.models.Blueprint`) serves
as the *interface* for users to execute your application. Instead of
writing code, users will create a file containing a configured `Blueprint`.

Your `Blueprint` may include as many configuration options as needed.

For illustration purposes, consider creating an application that will be send
notifications. We create a `HelloWorldBlueprint` as follows:

.. code-block:: python
   :caption: Creating HelloWorldBlueprint

   from cstar.orchestration.models import Blueprint

   class HelloWorldBlueprint(Blueprint):
      """A simple blueprint demonstrating the integration of a Blueprint and it's
      runner application.
      """
      application: str = "hello_world"
      """A unique identifier for this application."""

      target: str
      """The person to notify."""

Notice how this blueprint contains no execution logic - only the configuration necessary
to send the notification.

.. note::

   Implementing `HelloWorldBlueprint` requires the `application` field to be a unique
   string. It is used by *C-Star* to manage the lifecycle of the application.

`HelloWorldBlueprint` is functionally complete but under the hood blueprints rely
on `pydantic` to handle model serialization and deserialization. Adding additional
fields to this blueprint can make use of all the power of `pydantic`, like adding
field constraints (e.g. `min_length=10`) or more complex behaviors with model or field
validators. See the `pydantic documentation <https://pydantic.dev/docs/>`__ for more info.


Creating a `BlueprintRunner`
----------------------------

A `BlueprintRunner` (see: :class:`cstar.entrypoint.runner.BlueprintRunner`) is
used to execute an application. At runtime, the runner receives
a `Blueprint` instance, configures it's behavior using the blueprint, and performs any
behaviors desired by the application author.

.. note:: 
   The real power of `Blueprint` and `BlueprintRunner` are clear when we go on to
   create a `Workplan`. 
   
   While not covered here, a `Workplan` enables many applications to
   be executed together, and even allows us to re-use `Blueprint` files
   by asking *C-Star* to override values at runtime.

The simplest `BlueprintRunner` can be completed in a single method. Here, we
create a runner for the `HelloWorldBlueprint` that uses `print` to show our
notification in the console.

When implementing `run`, we can perform any action: call external services, create
files, etc. C-Star requires the application developer to ensure that the runner
status is updated whenever it completes or fails. See :class:`cstar.execution.handler.ExecutionStatus`
for additional details on the available states.

In this example, we:

* send our notification
* update the runner status with `self.add_state(ExecutionStatus.COMPLETED)`
* return the result to the calling code with `return self.result`

.. code-block:: python
   :caption: Creating HelloWorldRunner

   from cstar.entrypoint.runner import BlueprintRunner
   from cstar.applications.core import RunnerResult


   class HelloWorldRunner(BlueprintRunner[HelloWorldBlueprint]):
      """Worker class to execute a simple "Hello, world" application specified via blueprint."""

      @t.override
      async def run(self) -> RunnerResult[HelloWorldBlueprint]:
         """Process the blueprint.

         Returns
         -------
         RunnerResult
               The result after completing processing of the blueprint.
         """
         print(f"Hello, {self.blueprint.target}")
         self.add_state(ExecutionStatus.COMPLETED)
         return self.result

Creating the `ApplicationDefinition`
------------------------------------

An application definition (see: :class:`cstar.applications.core.ApplicationDefinition`) links
our components together. It also let's us configurate additional,
advanceed behaviors, like:

* Specifying `Transforms` to modify values in the blueprint at runtime
* Specifying migration `Adapters` for upgrading blueprints as the schema changes
  (see :ref:`versioning_blueprint_schema`)

For our sample application, we create `HelloWorldApplication`, specifying our unique
application identifier and the previously created `HelloWorldBlueprint` and `HelloWorldRunner`:

.. code-block:: python
   :caption: Creating an ApplicationDefinition

   from cstar.applications.core import ApplicationDefinition, register_application


   @register_application
   class HelloWorldApplication(
      ApplicationDefinition[HelloWorldBlueprint, HelloWorldRunner],
   ):
      name = "hello_world"
      runner = HelloWorldRunner
      blueprint = HelloWorldBlueprint

An application that supports continuing a failed prior attempt in place, rather than
starting fresh, sets ``resumable = True`` on its `ApplicationDefinition` and honours
`RunnerRequest.resume` in its `BlueprintRunner`. Only then does ``--resume`` on
``cstar blueprint run`` (or a workplan resume) reach that application; otherwise the
CLI rejects the flag before the runner is ever started.

Likewise, an application that can perform every stage before its model launch and
stop sets ``pre_runnable = True``. Its `BlueprintRunner` then honours
`RunnerRequest.pre_run`: it performs every pre-launch stage, reports a completed
state, launches nothing and skips post-run. Only then does ``--pre-run`` on
``cstar blueprint run`` or ``cstar workplan run`` reach that application. Otherwise
the CLI rejects the flag for a blueprint run, and a workplan pre-run skips the
application's steps and reports why.

.. _versioning_blueprint_schema:

Versioning the Blueprint Schema
-------------------------------

Every blueprint file carries a ``schema_version: x.y.z``. The version an
application currently reads is the default of ``schema_version`` on its
blueprint model (exposed as ``ApplicationDefinition.schema_version``). The
version is standard semver, read from the point of view of an *older file*
checked against the build's current version:

.. list-table::
   :header-rows: 1
   :widths: 10 45 45

   * - Bump
     - Meaning
     - An older file
   * - major
     - The set of valid documents changed incompatibly.
     - Does not load unchanged. It is migrated automatically if the application
       registers a ``SchemaAdapter`` for that major; otherwise the tooling
       refuses it with guidance for migrating by hand.
   * - minor
     - Additive, with defaults. An older file loads *and runs identically*.
     - Loads unchanged: no adapter, no migration copy, no workplan rewrite.
   * - patch
     - The published schema document is republished without changing the set of
       valid blueprints or their meaning (descriptions, examples). Rare.
     - Loads unchanged.

A file newer than the build, at any level, is refused with a request to upgrade
``cstar-ocean``. Whether an older major can be migrated automatically is a
property of the adapter registry, not of the version number.

The migration planner (``cstar.system.migration.BlueprintMigration.plan``) treats
a file with the same major as the build as compatible and returns an empty plan.
For an older major it walks the registered adapters keyed by source version,
at each step choosing the adapter with the highest source not above the file's
version. With adapters ``2.0.0 -> 2.1.0`` and ``2.1.0 -> 3.0.0`` registered, a
``2.0.0`` file takes both steps and a ``2.1.0`` file takes the second. A migrated
document is written at the build's current version. Each adapter's target must
be greater than its source, and an application may register only one adapter per
source version.

Registering an Adapter
~~~~~~~~~~~~~~~~~~~~~~

An adapter subclasses :class:`cstar.base.adapter.SchemaAdapter` and implements
``application()``, ``source()``, ``target()`` and ``_migrate_schema()``. The base
class copies the document, calls ``_migrate_schema`` and stamps the target
version. For example, if a hypothetical ``hello_world`` 2.0.0 moves ``output_dir`` onto the
``working_dir`` attribute of the base ``Blueprint``:

.. code-block:: python
   :caption: Adapting a renamed field

   import typing as t

   from cstar.base.adapter import SchemaAdapter


   class HelloWorldSchemaAdapterV1V2(SchemaAdapter):
       """Schema migration from schema version `1.0.0` to `2.0.0`.

       - use `working_dir` from the `Blueprint` base class instead of `output_dir`
       """

       @classmethod
       def application(cls) -> str:
           return "hello_world"

       @classmethod
       def source(cls) -> str:
           return "1.0.0"

       @classmethod
       def target(cls) -> str:
           return "2.0.0"

       @classmethod
       def _migrate_schema(cls, model: dict[str, t.Any]) -> dict[str, t.Any]:
           if output_dir := model.pop("output_dir", None):
               model["working_dir"] = output_dir
           return {**model}

Register the adapter in the ``migrations`` tuple of the application definition,
and set the new default on the blueprint model's ``schema_version``:

.. code-block:: python
   :caption: Registering migrations

   @register_application
   class HelloWorldApplication(
      ApplicationDefinition[HelloWorldBlueprint, HelloWorldRunner],
   ):
      name = "hello_world"
      runner = HelloWorldRunner
      blueprint = HelloWorldBlueprint
      migrations = (HelloWorldSchemaAdapterV1V2,)

See ``RomsMarblSchemaAdapter2025v1`` in ``cstar/applications/roms_marbl/migration.py``
for a working example.

A major bump with no automatic migration is registered the same way, as a
:class:`cstar.base.adapter.SchemaBreak`. Its ``source()`` is the last version of
the retired major and its ``target()`` is the new major. The planner refuses
older files with ``guidance()`` instead of adapting them, so there is no
``_migrate_schema`` to write:

.. code-block:: python
   :caption: Refusing an unmigratable major

   from cstar.base.adapter import SchemaBreak


   class HelloWorldSchemaBreakV2V3(SchemaBreak):
       @classmethod
       def application(cls) -> str:
           return "hello_world"

       @classmethod
       def source(cls) -> str:
           return "2.0.0"

       @classmethod
       def target(cls) -> str:
           return "3.0.0"

       @classmethod
       def guidance(cls) -> str:
           return "Replace the `targets` list with a single `target` string."

Choosing the Bump
~~~~~~~~~~~~~~~~~

A new field is a minor bump only if its default reproduces the old behavior: an
older file that omits the field must run exactly as it did before. A new field
whose default would change what an old file does is a major change, and needs an
adapter that writes the old behavior into the document explicitly (or a
``SchemaBreak``).

Each version gets its own published schema,
``docs/schemas/bp/<app>/<app>_schema.<version>.json``. Never regenerate a
published version in place; ``cstar blueprint schemas`` (a developer-mode command,
enabled with ``CSTAR_FF_DEVELOPER_MODE=1``) writes the file named for the version
currently on the model, so bump ``schema_version`` first. Add the new file to
``docs/schemas/index.rst``.

What Users See
~~~~~~~~~~~~~~

``cstar blueprint check`` reports a file that is too new, or an older major that
needs migration (with the ``cstar blueprint migrate`` command to run), or an older
major that cannot be migrated (with your ``guidance()``), and exits non-zero in
each case; a compatible file is validated normally. ``cstar blueprint migrate``
reports that a compatible file needs nothing and writes nothing. ``cstar blueprint
run`` and ``cstar workplan run`` migrate automatically, and only when adapters
are needed; a workplan reports every step it cannot migrate at once and refuses
before creating a run directory. ``CSTAR_DISABLE_MIGRATION=1`` turns off that
automatic migration, and has no effect on a compatible older file. For example:

.. code-block:: text

   roms_marbl schema 9.9.9 is newer than this build of cstar-ocean reads (3.0.0). Upgrade cstar-ocean to read this file.

   roms_marbl schema 0.5.0 has no automatic migration to 3.0.0 from 0.x. Update the blueprint by hand to the 3.0.0 schema (published under docs/schemas), then validate it with: cstar blueprint check <path>

Tying it Together
-----------------

After completing our components, we still need to create an instance of the
blueprint. First, I configure a `HelloWorldBlueprint` and save it to a file.

.. code-block:: yaml
   :caption: notify-ankona.yaml

   name: notify @ankona
   description: Send a notification to @ankona
   application: hello_world
   target: '@ankona'

Notice the additional fields that are included from the `Blueprint` base class:

* name - a user-friendly name used for logs and tracking in the system
* description - a user-friendly description of the purpose of this blueprint instance

These fields are required when creating the configured blueprint instance. For example, we
might later create another file `notify-scott.yaml` with:

.. code-block:: yaml
   :caption: notify-scott.yaml

   name: notify @scott
   description: Send a notification to @scott
   application: hello_world
   target: '@scott'

Remember - a `Blueprint` defines the available configuration. The
blueprint files specify how an *execution* of the application should behave.

Executing the `Blueprint`
-------------------------

We've created our blueprint and runner classes and created two separate blueprint instances.
We have finally reached the point where we can execute the application using the *C-Star* CLI:

.. code-block:: console
   :caption: Executing an application with the CLI

   > cstar blueprint run notify-ankona.yaml
   Hello, @ankona

   > cstar blueprint run notify-scott.yaml
   Hello, @scott

Making Your Application Discoverable
------------------------------------

C-Star automatically loads internal applications as-needed and makes use of python's ``entrypoints`` functionality to discover external applications.

Declare a ``cstar.applications`` entry point pointing at the module containing
your ``@register_application``-decorated class, e.g. in ``pyproject.toml``:

.. code-block:: toml
   :caption: Registering an application as an installed plugin

   [project.entry-points."cstar.applications"]
   my_app = "my_package.my_app"

The entry-point name must be the application's ``name`` -- the same value your
blueprints carry in their ``application`` field. *C-Star* imports that module
(and only that module -- the entry point's return value is unused) the first
time an application by that name is requested.

Choose a name no built-in already uses. *C-Star* resolves the in-tree
``cstar.applications`` module first and only consults entry points when that
comes up empty, so a plugin can never shadow a built-in application: if
``cstar.applications`` already defines a module with that name, your plugin is
silently never imported. Note that this tutorial's own ``hello_world`` is one
such name, so an installed plugin could not claim it.
