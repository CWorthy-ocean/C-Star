``cstar run status``, ``cstar workplan status`` and ``cstar run show`` no longer stop with a Python traceback when ``CSTAR_SLURM_ACCOUNT`` is unset: reading a run's status needs no submission settings.
- When ``cstar run status`` cannot read the statuses, such as for a workplan that names a scheduler this machine lacks, it prints a one-line error instead of a traceback.
