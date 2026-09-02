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


- ``Blueprint.single_node`` (default ``False``): a blueprint can declare that its work runs in a single process. The workplan transformer records this alongside the step's CPU requirement, and the SLURM launcher pins such a job to one node and clamps ``num_cpus`` to the queue's CPUs per node (a user-supplied ``cpus_per_node`` is treated as the capacity of record). A single-node job that also requests more than one node is rejected at job creation. (`#664 <https://github.com/CWorthy-ocean/C-Star/pull/664>`_)

  - ``SlurmComputeSpec.single_node`` and a matching ``single_node`` argument on ``create_scheduler_job`` / ``SchedulerJob``, so the marker can also be set directly in a step's ``compute_overrides["slurm"]``.

- Per-partition CPU capacity: ``SlurmPartition.max_cpus_per_node`` queries ``sinfo --format=%c`` for the partition, returning the smallest node type's value on heterogeneous partitions so derived node counts are always satisfiable. (`#664 <https://github.com/CWorthy-ocean/C-Star/pull/664>`_)

Bug Fixes
~~~~~~~~~


- On systems that do not require an explicit task distribution, jobs were submitted with a bare ``--ntasks``, leaving SLURM free to scatter tasks across nodes and stranding single-process steps on a fraction of their CPUs. The minimum node count is now derived from the partition's (or system's) CPUs per node and emitted as ``--nodes``, with ``--ntasks-per-node`` added when a per-node capacity was supplied. When no capacity can be determined the job is submitted without a node count and a warning is logged. (`#664 <https://github.com/CWorthy-ocean/C-Star/pull/664>`_)

Improvements
~~~~~~~~~~~~


- ``cpus_per_node`` on non-task-distribution systems now doubles as the assumed per-node capacity when deriving the node count, so a user can target a heterogeneous partition's larger node types. (`#664 <https://github.com/CWorthy-ocean/C-Star/pull/664>`_)

Miscellaneous
~~~~~~~~~~~~~

- N/A
