Forge refuses a MARBL setup whose biogeochemical sources would start the plankton at zero, and two setups that used to fail at compile or ROMS start-up.
- An initial-conditions or boundary BGC source set with no ``CHL`` or no ``Fe`` source (ESPER, GLODAP or WOA_BGC alone) is refused; keep a UNIFIED item supplying ``CHL`` and ``Fe`` as the bundled specs do, or add them to a ``constants`` source.
- ``cppdefs.marbl: true`` set as an override without ``bgc_mode: marbl`` is refused; set ``bgc_mode: marbl`` instead.
- Open boundaries with no ``forcing.boundary`` block are refused unless the domain is a nested child; add a boundary block or close those sides.
