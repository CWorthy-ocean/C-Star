Registering a New System
========================

C-Star recognises the systems listed in :doc:`../deployments/machines`; on any
other cluster it runs as on a workstation (see :ref:`machines_unlisted`).
This page is for maintainers who add a system to the set C-Star ships. A
user describing their own cluster follows :doc:`../deployments/describe_cluster`
instead, which also explains how to find the modules and environment
variables; write and test the file there first.

A bundled system is one file,
``cstar/catalog/bundled/systems/<system-name>.yaml``, where ``<system-name>``
is the short lowercase name (``anvil``, ``bouchet``, ``perlmutter``) and
matches the file's ``name``. The :download:`schema
<../schemas/system/system_schema.1.0.0.json>` lists every key; a file with a
key it does not know is refused.

Rules for a bundled file
------------------------

- List every module the build needs explicitly, including the ones the site
  loads by default, so a change of site defaults does not change the build.
- Verify the file on the machine: ``cstar system check <file>`` on a login
  node, then a ROMS build and a job through C-Star.
- Date each pin in a comment, as the other files do, for example
  ``# Verified on Anvil 2026-11-02``, so the next maintainer knows what was
  checked and when.
- Quote a fixed walltime (``"12:00:00"``); YAML reads it unquoted as a number
  and C-Star refuses it.
- Add a ``detect`` rule only if Lmod does not report the machine's name.

Tests and documentation
-----------------------

``test_bundled_systems_golden.py`` pins what each bundled system gives
C-Star. Regenerate its fixture with ``UPDATE_GOLDEN=1`` and review the diff.

Finally, add the new system to the :doc:`../deployments/machines` table,
keeping the rows in alphabetical order by system name.
