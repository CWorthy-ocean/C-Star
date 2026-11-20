Forge refuses a CDR forcing that does not fit the run or the grid, which it used to generate without a message.
- A CDR window (``cdr_forcing`` ``start_time``/``end_time``) that does not cover the run, or a release whose ``times`` all fall outside the run, is refused when the blueprint is built or processed.
- A release outside the domain, on land, on the edge of the grid or below the seafloor fails in input generation; ROMS used to move it to the nearest cell.
- ``cdr_frc.time_interpolation`` set against the releases' ``time_interpolation`` is refused, including in a blueprint saved before this version whose CDR releases set ``time_interpolation: true``; set the two to match.
