Forge refuses a CDR forcing that does not fit the run or the grid, which it used to generate without a message.
- A CDR window (``cdr_forcing`` ``start_time``/``end_time``) that starts after the run starts or ends before it ends is refused when the blueprint is built or processed; ROMS used to stop on it at start-up or mid-run.
- A release whose ``times`` all fall before the run starts, or at or after it ends, is refused.
- A release outside the domain, on land, on the edge of the grid or below the seafloor now fails in input generation; ROMS used to move it to the nearest cell.
- ``cdr_frc.time_interpolation`` set against the releases' ``time_interpolation`` is refused, as is a blueprint saved by an earlier release whose releases set ``time_interpolation: true``; set the two to match.
