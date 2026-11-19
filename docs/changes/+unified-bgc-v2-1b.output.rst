Forge stages the corrected unified BGC climatology as ``BGCdataset_v2_1b.nc``, in which the negative deep-ocean values that interpolation left in the labile DOC, DON and DOP fields are set to zero; the first Forge run after upgrading downloads it again (about 10 GB) into the shared source-data cache.
- A cached ``BGCdataset_v2_1a.nc`` is reported as stale on every staging run until it is deleted; Forge never deletes it.
- Domains whose initial or boundary BGC came from UNIFIED should be regenerated before a run that will be compared with earlier ones.
