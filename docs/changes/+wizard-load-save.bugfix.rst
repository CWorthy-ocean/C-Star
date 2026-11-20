A blueprint loaded into the Forge wizard and saved again keeps what the file asked for; before, the wizard rebuilt it from its fields and silently dropped what they could not hold.
- Run start, end and reference dates keep their time of day, so the nested-run tutorial's child still starts at 06:00. A date you change starts at midnight.
- A parent or child grid keeps ``hmin``, ``close_narrow_channels`` and the other ``grid_kwargs`` the wizard has no field for, so a child is aligned to the parent that actually ran. ``prefill_kwargs`` and ``extrap_kwargs`` are kept too.
- A simple-mode CDR forcing with more than one release, a flux that changes in time, a tracer other than ALK or a time of day is kept as loaded, with its fields locked; **Edit as one release** replaces it with its first release at a constant ALK flux.
- Picking a DomainSpec sets its ``hmin``, channel closing and mask shapefile, and keeps its other ``grid_kwargs``.
- The load status names a ModelSpec, ForcingSpec or OutputSpec that is not in the catalog; the blueprint is saved under the spec the dropdown shows instead.
