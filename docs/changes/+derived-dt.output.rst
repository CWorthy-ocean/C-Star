Forge derives the time step from the grid unless you set one, and the derived ``dt`` is now the largest divisor of 3600 s at or below the CFL estimate, so a new blueprint can get a different ``dt``.
- The wizard no longer saves 7200 s by default: until you type a time step it shows and saves the derived value, which follows grid edits, and "Use CFL value" (formerly "Compute from CFL") goes back to it.
- Derived ``dt`` for the bundled domains: GoA-10th_deg 450 s, NEP-4th_deg 1200 s, PAC_2fth_deg 1800 s, ccs-12km 900 s, hvalfjordur-0 180 s, wio-toy and gulf-guinea-toy 3600 s (MARBL stopped at startup with their old values).
- A DomainSpec saved from the wizard carries ``dt`` only when you set it.
