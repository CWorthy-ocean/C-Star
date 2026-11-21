``cstar run promote`` no longer refuses to move a finished submitted run as unfinished when ``CSTAR_SLURM_ACCOUNT`` is unset; it reads the steps' statuses from the scheduler without it.
- ``cstar run check`` refreshes the run without it too, instead of warning that the manifest was not refreshed and reporting the attempt as open.
