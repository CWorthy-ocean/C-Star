.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- Change creates a variation from XDG spec for ``CSTAR_DATA_HOME`` (`#641 <https://github.com/CWorthy-ocean/C-Star/pull/641>`_)
- ``cstar workplan run`` on a local machine now schedules all steps and returns; use ``cstar workplan status`` to follow progress. (`#647 <https://github.com/CWorthy-ocean/C-Star/pull/647>`_)
- A step whose dependency fails is now aborted and marked Failed instead of running against incomplete upstream output. (`#647 <https://github.com/CWorthy-ocean/C-Star/pull/647>`_)
- The experimental ``CSTAR_FF_ENABLE_LOCAL_PROXY`` flag is removed; its behavior is now the default. (`#647 <https://github.com/CWorthy-ocean/C-Star/pull/647>`_)

New features
~~~~~~~~~~~~

- N/A

Bug Fixes
~~~~~~~~~


- On srun-based SLURM systems (Anvil, Perlmutter, Expanse), a ROMS task dying mid-run (OOM kill, segfault) left the job hung until the walltime limit; the job step is now terminated immediately when any task exits abnormally (``srun --kill-on-bad-exit=1``). (`#644 <https://github.com/CWorthy-ocean/C-Star/pull/644>`_)
- Local workplan runs hung indefinitely because a step that finished was still reported as running. (`#647 <https://github.com/CWorthy-ocean/C-Star/pull/647>`_)
- ``cstar workplan status`` showed local steps as Running forever after the launching process exited. (`#647 <https://github.com/CWorthy-ocean/C-Star/pull/647>`_)

Improvements
~~~~~~~~~~~~


- Avoid default user-facing output directory that is hidden. (`#641 <https://github.com/CWorthy-ocean/C-Star/pull/641>`_)
- Avoid changing env var in tests where an autouse fixture already handles it (`#641 <https://github.com/CWorthy-ocean/C-Star/pull/641>`_)
- Generated sbatch scripts set ``ulimit -s unlimited`` before running, preventing stack-overflow segfaults from large Fortran automatic arrays at larger tile counts. (`#644 <https://github.com/CWorthy-ocean/C-Star/pull/644>`_)

Miscellaneous
~~~~~~~~~~~~~

- The tagged GitHub release is now created automatically when the "Finalize release notes for ``<tag>``" PR is merged, using the release notes just finalized in the docs as the release body — instead of the manual tag-and-publish step with GitHub's weaker auto-generated summary. (`#645 <https://github.com/CWorthy-ocean/C-Star/pull/645>`_)
- Merging the release-notes finalization PR no longer causes the release-notes updater to re-open a spurious "Unreleased" section. (`#645 <https://github.com/CWorthy-ocean/C-Star/pull/645>`_)
