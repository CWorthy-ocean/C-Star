Directives
==========

A directive is a per-step instruction, given under a step's ``directives:``
mapping, that runs on the compute node just before the application starts and
modifies the blueprint using information that only exists at run time -- for
example, a restart file written by an earlier step.

Existing directives
-------------------

continue-from
`````````````

Sets a step's initial conditions from a restart file of an earlier run.
Configure exactly one restart source:

- ``step`` -- the name of an earlier step whose output holds the restart file;
  a step of another workplan run is named ``<step>@<alias>`` (see
  :ref:`workplan_external_runs`)
- ``path`` -- a fixed directory or file path to the restart file

``step`` and ``path`` are mutually exclusive. The step named by ``step`` must
be listed in the step's ``depends_on``, directly or through an earlier step.

By default the latest restart file in the source is used. To continue from a
different one, add:

- ``timestamp`` -- the date and time of the restart to use: an ISO 8601
  date-time (``2012-02-01 00:00:00`` or ``2012-02-01T00:00:00``), a date alone
  (``2012-02-01``, meaning midnight), or the 14-digit stamp from the restart
  file's name (``20120201000000``)

Only the restart whose file name carries exactly that timestamp is used. The
timestamp is matched against the file name -- the date C-Star uses as the
step's start date -- not against the time records inside the file.

.. code-block:: yaml

    directives:
      continue-from:
        step: parent_run
        timestamp: 2012-02-01 00:00:00

If the step also carries an explicit ``runtime_params.start_date`` override
(via ``blueprint_overrides``) that disagrees with the restart file this
directive locates, a warning is logged. It's recommended not to override
the start date directly when using ``continue-from`` -- let the directive
set the correct date for you.

nest-from
`````````

Supplies boundary forcing for a nested child run from a parent (or sibling)
simulation. Configure exactly one of:

- ``step`` -- the name of the step whose output holds the parent run's
  boundary files; a step of another workplan run is named ``<step>@<alias>``
  (see :ref:`workplan_external_runs`) and must be listed in ``depends_on``
- ``path`` -- a fixed directory or file path to the parent run's boundary
  files

Several sources can be joined by separating them with ``;`` in a single
``step`` or ``path`` value; boundary files from every listed source are
combined, in the order given. If you parent and child are split into matching
time segments, you may need to specify the directly corresponding parent segment
as well as the one right after it, so that ROMS has enough forcing data to finish
the last time step.



carbonate-sensitivity-from
``````````````````````````

Supplies the carbonate sensitivities that a CDR-LiTE run reads as surface
forcing -- the variables ``ddic_dco2`` and ``ddic_dalk`` -- from an earlier
UCLA-ROMS run, by setting the blueprint's ``forcing.carbonate_sensitivity``
dataset. Configure exactly one of:

- ``step`` -- the name of the step whose output holds the carbonate
  sensitivity files; a step of another workplan run is named
  ``<step>@<alias>`` (see :ref:`workplan_external_runs`). The step must be
  listed in the step's ``depends_on``, directly or through an earlier step.
- ``path`` -- a fixed directory or file path to the carbonate sensitivity files

``step`` and ``path`` are mutually exclusive. Several sources can be joined by
separating them with ``;`` in a single ``step`` or ``path`` value; the files
from every listed source are combined, in the order given, and every listed
source must contain carbonate sensitivity files.

ROMS writes the sensitivities to ``<output_root_name>_cdrgas.<timestamp>.nc``
files only when the producing run has MARBL enabled and its namelist sets
``do_cdr_gas_exch_output`` to true (``cdr_gas_exch_output_settings`` in the
blueprint's ``namelist_overrides``; ``cdr_gas_exch_output`` in a forge output
spec), so the step the directive reads from must be a UCLA-ROMS run with MARBL and
that option on. The files found there replace any ``forcing.carbonate_sensitivity``
entries already in the blueprint.

.. code-block:: yaml

    directives:
      carbonate-sensitivity-from:
        step: marbl_run

.. note::

    ucla-roms releases up to 0.9.1 write the ``_cdrgas`` files without the
    forcing time variables a CDR-LiTE build expects. A CDR-LiTE step fed from
    such a run fails during setup with a message naming the missing
    variables.

Ordering
--------

A step's ``blueprint_overrides`` are applied first, then its directives, in
the order they are listed under ``directives:``.

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
        application: ucla_roms
        blueprint: ./blueprints/parent_blueprint.yaml

      - name: child_run
        application: ucla_roms
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
