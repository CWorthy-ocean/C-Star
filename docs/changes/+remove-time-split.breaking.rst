Removed: automatic time-splitting and its feature flags ``CSTAR_FF_ORCH_TRX_TIMESPLIT``, ``CSTAR_FF_ORCH_TRX_TIMESPLIT_FREQ`` and ``CSTAR_FF_ORCH_TRX_TIMESPLIT_LONGNAME``; split a run into explicit workplan steps chained with ``continue-from`` (the wizard's "Chunk a run in time" recipe writes them).
- An exported flag has no effect, and the CLI says so.
