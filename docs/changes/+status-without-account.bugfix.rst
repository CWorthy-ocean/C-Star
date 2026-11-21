``cstar run status``, ``cstar workplan status`` and ``cstar run show`` no longer stop with a Python traceback when ``CSTAR_SLURM_ACCOUNT`` is unset: reading a run's status needs no submission settings.
- ``cstar run status`` prints a one-line error, and exits 1, when it cannot read the statuses at all, such as for a workplan that names a scheduler this machine lacks.
