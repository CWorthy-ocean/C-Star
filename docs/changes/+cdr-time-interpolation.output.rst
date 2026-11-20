The namelist's ``cdr_time_interpolation`` now follows the CDR releases' ``time_interpolation``, so releases with ``time_interpolation: true`` are interpolated in time, as roms-tools accounts them, instead of held as steps one record late.
- Forge's record of the CDR forcing (``_cdr_forcing.yaml``) now includes the grid, like its records of the other inputs.
