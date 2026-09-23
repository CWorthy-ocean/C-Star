Directives
==========

A directive is a per-step instruction, given under a step's ``directives:``
mapping, that runs on the compute node just before the application starts and
modifies the blueprint using information that only exists at run time -- for
example, a restart file written by an earlier step.

apply-overrides
----------------

C-Star packages every step's ``blueprint_overrides``, together with the
system-level overrides it adds (such as the step's working directory), into an
``apply-overrides`` directive when the workplan is scheduled, and applies them
on the compute node. You do not write this directive yourself.

continue-from
--------------

Sets a step's initial conditions from the latest restart file of an earlier
run. Configure exactly one of:

- ``step`` -- the name of an earlier step whose output holds the restart file
- ``path`` -- a fixed directory or file path to the restart file

``step`` and ``path`` are mutually exclusive.

If the step also carries an explicit ``runtime_params.start_date`` override
(via ``blueprint_overrides``) that disagrees with the restart file this
directive locates, a warning is logged. A step that leaves ``start_date`` to
the directive is not warned.

nest-from
----------

Supplies boundary forcing for a nested child run from a parent (or sibling)
simulation. Configure exactly one of:

- ``step`` -- the name of the step whose output holds the parent run's
  boundary files
- ``path`` -- a fixed directory or file path to the parent run's boundary
  files

Several sources can be joined by separating them with ``;`` in a single
``step`` or ``path`` value; boundary files from every listed source are
combined, in the order given.

Two keys are deprecated:

- ``bry_path`` was an alias for ``path``; use ``path`` instead.
- ``rst_path`` additionally set the step's initial conditions from a restart
  file found at the given path; use a ``continue-from`` directive instead.
  ``rst_path`` cannot be combined with a ``continue-from`` directive on the
  same step, since both would set ``initial_conditions``.

Ordering
--------

``apply-overrides`` always runs first, so a deferred blueprint's overrides
are applied before any other directive inspects or modifies it. Other
directives on a step run in the order they are listed under ``directives:``.

Example
-------

The following workplan runs a parent simulation, then a child step nested
inside it. The child step continues from the parent's restart file and takes
its boundary forcing from the parent's output.

.. code-block:: yaml

    name: nesting_example
    description: Run a parent simulation, then a nested child run
    state: draft

    steps:
      - name: parent_run
        application: roms_marbl
        blueprint: ./blueprints/parent_blueprint.yaml

      - name: child_run
        application: roms_marbl
        blueprint: ./blueprints/child_blueprint.yaml
        depends_on:
          - parent_run
        directives:
          continue-from:
            step: parent_run
          nest-from:
            step: parent_run

Running a single blueprint with directives
--------------------------------------------

Outside a workplan, ``cstar blueprint run --directives <file>`` applies a
directive file to a single blueprint before running it. The file has the
same ``directives:`` mapping used in a step:

.. code-block:: yaml

    directives:
      continue-from:
        path: /path/to/parent_run/output
      nest-from:
        path: /path/to/parent_run/output
