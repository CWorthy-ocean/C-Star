Custom C-Star Applications
==========================

Custom applications enable users to execute new types of behavior with C-Star. The real
power of custom applications becomes clear when they are integrated into a workplan.


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

A `Blueprint` serves as the _interface_ for users to execute your application. Instead of
writing code, users will create a file containing a configured `Blueprint`.

Your `Blueprint` may include as many configuration options as needed.

For illustration purposes, consider creating an application that will be send
notifications. We create a `HelloWorldBlueprint` as follows:

.. code-block:: python
   :caption: Creating `HelloWorldBlueprint`

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

`HelloWorldBlueprint` is functionally complete but under the hood, blueprints rely
on `pydantic` to handle model serialization and deserialization. Adding additional
fields to this blueprint can make use of all the power of `pydantic`, like adding
field constraints (e.g. `min_length=10`) or more complex behaviors with model or field
validators. See the `pydantic documentation <https://pydantic.dev/docs/>` for more info.


Creating a `BlueprintRunner`
----------------------------

A `BlueprintRunner` is used to execute an application. At runtime, the runner receives
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
files, etc. C-Star requirs the application developer to ensure that the runner
status is updated whenever it completes or fails. See `cstar.execution.handler.ExecutionStatus`
for additional details on the available states.

In this example, we:

* send our notification
* update the runner status with `self.add_state(ExecutionStatus.COMPLETED)`
* return the result to the calling code with `return self.result`

.. code-block:: python
   :caption: Creating `HelloWorldRunner`

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

An application definition links our components together. It also let's us configurate additional,
advanceed behaviors, like:

* Specifying `Transforms` to modify values in the blueprint at runtime
* Specifying migration `Adapters` for upgrading blueprints as the schema changes

For our sample application, we create `HelloWorldApplication`, specifying our unique
application identifier and the previously created `HelloWorldBlueprint` and `HelloWorldRunner`:

.. code-block:: python
   :caption: Creating an `ApplicationDefinition`
   from cstar.applications.core import ApplicationDefinition, register_application

   @register_application
   class HelloWorldApplication(
      ApplicationDefinition[HelloWorldBlueprint, HelloWorldRunner],
   ):
      name = "hello_world"
      runner = HelloWorldRunner
      blueprint = HelloWorldBlueprint

Tying it Together
-----------------

After completing our components, we still need to create an instance of the
blueprint. First, I configure a `HelloWorldBlueprint` and save it to a file.

.. code-block:: yaml
   :caption: notify-ankona.yaml

   name: Send Notification
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

   name: Send Notification
   description: Send a notification to @scott
   application: hello_world
   target: '@scott'

Remember - the `Blueprint` tells us _what configuration options_ are available. The
blueprint files specify how a single _execution_ of the application should behave.

Executing the `Blueprint`
-----------------------

We've created our blueprint and runner classes and created two separate blueprint instances.
We have finally reached the point where we can execute the application using the *C-Star* CLI:

.. code-block:: console
   :caption: Executing an application with the CLI

   > cstar blueprint run notify-ankona.yaml
   Hello, @ankona

   > cstar blueprint run notify-scott.yaml
   Hello, @scott
