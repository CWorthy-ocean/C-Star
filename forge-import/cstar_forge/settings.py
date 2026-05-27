"""
ROMS Configuration Template Renderer

This module provides functionality to render ROMS configuration files
from Jinja2 templates using a settings dictionary, and to write the
Fortran namelist file (namelist.nml) used by the new ROMS input system.
"""

import shutil
import warnings
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape, meta
from typing import Dict, Any, Union, Set, Optional

import f90nml


def _fortran_cdr_file_decl(path: Any, max_line_len: int = 72) -> str:
    """
    Emit ``character(len=...) :: cdr_file = '...'`` for ROMS ``cdr_frc.opt``.

    Long paths are split for Fortran 77 fixed form: lines that continue end with the
    ``//`` concatenation operator; the next line is a continuation with a non-blank
    character in column 6 (here ``&``). No end-of-line ``&`` is used—only column 6 on
    the following line. No physical line exceeds *max_line_len* (default 72). The first
    line opens the literal (``= '...``) before any ``//``; embedded ``'`` in paths are
    doubled per Fortran rules.
    """
    raw = str(path)
    esc = raw.replace("'", "''")
    # Keep a one-character safety buffer to preserve historical behavior and
    # avoid edge-case truncation in downstream Fortran usage.
    n = len(raw) + 1
    prefix = f"      character(len={n}) :: cdr_file = '"
    single = prefix + esc + "'"
    if len(single) <= max_line_len:
        return single

    # First line must open the string literal before any trailing & (not ``= &`` with no quote).
    # F77: end with ``' //`` only; continuation is column 6 on the next line, not ``&`` here.
    tail_first_cont = "' //"
    max0 = max_line_len - len(prefix) - len(tail_first_cont)
    max0 = max(max0, 1)

    # Continuation lines: "     &         'CHUNK' //" (≤72 cols) or "... 'CHUNK'" (last).
    cont = "     &         '"
    tail_mid = "' //"
    tail_end = "'"
    max_chunk = max_line_len - len(cont) - len(tail_mid)
    max_chunk = max(max_chunk, 8)

    lines: list[str] = []
    ch0 = esc[:max0]
    i = len(ch0)
    lines.append(f"{prefix}{ch0}{tail_first_cont}")

    while i < len(esc):
        take = min(max_chunk, len(esc) - i)
        ch = esc[i : i + take]
        i += take
        if i >= len(esc):
            lines.append(f"{cont}{ch}{tail_end}")
        else:
            lines.append(f"{cont}{ch}{tail_mid}")
    return "\n".join(lines)


def render_roms_settings(
    template_files: list[str],
    template_dir: Union[str, Path],
    settings_dict: dict[str, Any],
    code_output_dir: Union[str, Path],
    n_tracers: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Render ROMS configuration files from Jinja2 templates.
    
    Accepts a list of template files, a settings dictionary, and an output directory.
    Loops over the template files, applies templating with the settings context,
    and writes the resulting files to the output directory.
    
    Args:
        template_files: List of template file names (with .j2 extension)
        template_dir: Directory containing Jinja2 template files
        settings_dict: Final merged settings dictionary for template rendering
        code_output_dir: Directory where rendered files will be saved
    
    Returns:
        Dictionary with "location" (absolute path to code_output_dir) and "filter" 
        (dict with "files" list of rendered/copied file names)
    """
    # Convert paths to Path objects
    template_dir = Path(template_dir)
    code_output_dir = Path(code_output_dir)
    
    # Validate template directory exists
    if not template_dir.exists():
        raise FileNotFoundError(
            f"Template directory does not exist: {template_dir}"
        )
    if not template_dir.is_dir():
        raise ValueError(
            f"Template path is not a directory: {template_dir}"
        )
    
    # Ensure output directory is empty or doesn't exist
    if not code_output_dir.exists():
        raise FileNotFoundError(
            f"Output directory does not exist: {code_output_dir}"
        )
    if not code_output_dir.is_dir():
        raise ValueError(
            f"Output path is not a directory: {code_output_dir}"
        )
        
    # Validate that every .j2 template has a corresponding settings_dict entry
    # and that every settings_dict entry has a corresponding .j2 template
    template_keys = set()
    for template_file in template_files:
        if template_file.endswith('.j2'):
            # Extract the key from template filename
            # First try the entire base name (e.g., "roms.in.j2" -> "roms.in")
            # If that doesn't exist in settings_dict, try the part before the last dot (e.g., "bgc.opt.j2" -> "bgc")
            base_name = template_file[:-3]  # Remove .j2
            # Check if the entire base name exists as a key in settings_dict
            if settings_dict and base_name in settings_dict:
                key = base_name
            else:
                # Fall back to the part before the last dot
                key = base_name.rsplit('.', 1)[0] if '.' in base_name else base_name
            template_keys.add(key)
    
    # Get settings_dict keys (top-level keys)
    settings_keys = set(settings_dict.keys()) if settings_dict else set()
    
    # Check for templates without settings_dict entries
    missing_settings = template_keys - settings_keys
    if missing_settings:
        raise ValueError(
            f"Template files without corresponding settings_dict entries: {sorted(missing_settings)}. "
            f"Template files: {sorted([f for f in template_files if f.endswith('.j2')])}, "
            f"Settings keys: {sorted(settings_keys)}"
        )
    
    # Validate nested structure: check that template variables match settings_dict structure
    # Create a temporary environment for parsing templates (must register the same filters as
    # ROMSTemplateRenderer or parse() raises TemplateAssertionError for custom filters).
    temp_env = Environment(loader=FileSystemLoader(str(template_dir)))
    _attach_roms_jinja_filters(temp_env)
    
    for template_file in template_files:
        if not template_file.endswith('.j2'):
            continue  # Skip non-template files
        
        # Extract the key from template filename
        # First try the entire base name (e.g., "roms.in.j2" -> "roms.in")
        # If that doesn't exist in settings_dict, try the part before the last dot (e.g., "bgc.opt.j2" -> "bgc")
        base_name = template_file[:-3]  # Remove .j2
        # Check if the entire base name exists as a key in settings_dict
        if settings_dict and base_name in settings_dict:
            key = base_name
        else:
            # Fall back to the part before the last dot
            key = base_name.rsplit('.', 1)[0] if '.' in base_name else base_name
        
        if key not in settings_dict:
            continue  # Already caught by earlier validation
        
        # Read template file and parse it
        template_path = template_dir / template_file
        if not template_path.exists():
            continue  # Already caught by earlier validation
        
        try:
            # Parse template and find undeclared variables using Jinja2's meta API
            template_source = temp_env.loader.get_source(temp_env, template_file)[0]
            parsed_ast = temp_env.parse(template_source)
            template_vars = meta.find_undeclared_variables(parsed_ast)
        except Exception as e:
            raise ValueError(
                f"Failed to parse template '{template_file}': {e}"
            )
        
        # Get nested keys from settings_dict
        settings_value = settings_dict[key]
        if not isinstance(settings_value, dict):
            raise ValueError(
                f"Settings_dict['{key}'] must be a dictionary, but got {type(settings_value).__name__}. "
                f"Template file: {template_file}"
            )
        
        settings_nested_keys = set(settings_value.keys())
        
        # Exclude 'nt' from template_vars since it's added dynamically during rendering
        # 'nt' is a special variable for number of tracers, not part of settings_dict
        template_vars_to_check = template_vars - {'nt'}
        
        # If key matches base_name (full match case), template_vars should match settings_nested_keys directly
        # Otherwise (partial match case), template_vars should contain the key itself
        if key == base_name:
            # Full match case: template variables like {{ title.casename }} -> 'title' should match settings_dict['roms.in'].keys()
            # Check for template variables without settings_dict entries
            missing_nested_settings = template_vars_to_check - settings_nested_keys
            if missing_nested_settings:
                raise ValueError(
                    f"Template '{template_file}' references variables without corresponding settings_dict entries: "
                    f"{sorted(missing_nested_settings)}. "
                    f"Template variables: {sorted(template_vars_to_check)}, "
                    f"Settings_dict['{key}'] keys: {sorted(settings_nested_keys)}"
                )
            
            # Check for settings_dict entries without template variables
            missing_nested_template_vars = settings_nested_keys - template_vars_to_check
            if missing_nested_template_vars:
                raise ValueError(
                    f"Settings_dict['{key}'] contains keys without corresponding template variables in '{template_file}': "
                    f"{sorted(missing_nested_template_vars)}. "
                    f"Template variables: {sorted(template_vars_to_check)}, "
                    f"Settings_dict['{key}'] keys: {sorted(settings_nested_keys)}"
                )
        else:
            # Partial match case: template variables like {{ bgc.wrt_his }} -> 'bgc' 
            # We expect 'bgc' to be in template_vars, and settings_dict['bgc'] should exist
            # The nested structure validation is less strict here since we can't easily extract
            # nested attribute names (e.g., 'wrt_his') without AST walking
            if key not in template_vars:
                raise ValueError(
                    f"Template '{template_file}' does not reference '{key}' but settings_dict expects it. "
                    f"Template variables: {sorted(template_vars)}"
                )
    
    # Initialize renderer
    renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
    
    # Track rendered/copied files
    rendered_files = []
    
    # Loop over template files and render each one
    for template_file in template_files:
        # Check if template file exists
        template_path = template_dir / template_file
        if not template_path.exists():
            raise FileNotFoundError(
                f"Template file not found: {template_path}"
            )
        
        if template_file.endswith('.j2'):
            # Render template file (remove .j2 extension for output filename)
            output_name = template_file.replace('.j2', '')
            output_path = code_output_dir / output_name
            
            # Extract the key from template filename for context
            base_name = template_file[:-3]  # Remove .j2
            # Check if the entire base name exists as a key in settings_dict
            if settings_dict and base_name in settings_dict:
                key = base_name
            else:
                # Fall back to the part before the last dot
                key = base_name.rsplit('.', 1)[0] if '.' in base_name else base_name
            
            # Get the context for this template
            # - Full match case (key == base_name): use nested dict (e.g., roms.in.j2 -> settings_dict['roms.in'])
            # - Partial match case (key != base_name): use full settings_dict (e.g., bgc.opt.j2 uses {{ bgc.wrt_his }})
            if key == base_name and key in settings_dict:
                # Full match: template uses variables like {{ title.casename }}, context is the nested dict
                context = settings_dict[key].copy()
            else:
                # Partial match: template uses variables like {{ bgc.wrt_his }}, context needs bgc at top level
                context = settings_dict.copy()
            
            # Add n_tracers to context if provided
            if n_tracers is not None:
                context['nt'] = n_tracers
            content = renderer.render_template(template_file, context)
            
            with open(output_path, 'w') as f:
                f.write(content)
            
            rendered_files.append(output_name)
        else:
            # Copy non-template file directly
            output_path = code_output_dir / template_file
            shutil.copy2(template_path, output_path)
            
            rendered_files.append(template_file)
    
    # Return dictionary with location and filter
    return {
        "location": str(code_output_dir.resolve()),
        "branch": "na",
        "filter": {"files": sorted(rendered_files)}
    }
    

class ROMSTemplateRenderer:
    """Renderer for ROMS configuration files from Jinja2 templates."""
    
    def __init__(self, template_dir: Union[str, Path]):
        """
        Initialize the template renderer.
        
        Args:
            template_dir: Directory containing Jinja2 template files (.j2)
        """
        self.template_dir = Path(template_dir)
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True
        )
        _attach_roms_jinja_filters(self.env)
    
    @staticmethod
    def _fortran_bool(value: bool) -> str:
        """
        Convert Python boolean to Fortran boolean string.
        
        Args:
            value: Python boolean value
            
        Returns:
            Fortran boolean string ('.true.' or '.false.')
        """
        return '.true.' if value else '.false.'
    
    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Render a single template with the given context.
        
        Args:
            template_name: Name of the template file (with .j2 extension)
            context: Configuration dictionary for template rendering
            
        Returns:
            Rendered template content as string
        """
        template = self.env.get_template(template_name)
        return template.render(**context)


def _attach_roms_jinja_filters(env: Environment) -> None:
    """Register ROMS Fortran Jinja filters on *env* (renderer and parse-time temp env)."""
    env.filters["lower"] = ROMSTemplateRenderer._fortran_bool
    env.filters["fort_cdr_file_decl"] = _fortran_cdr_file_decl


def write_roms_namelist(
    settings_compile_time: Dict[str, Any],
    settings_run_time: Dict[str, Any],
    output_dir: Union[str, Path],
    n_tracers: int,
) -> None:
    """
    Write a ROMS Fortran namelist file (``namelist.nml``) from the merged
    compile-time and run-time settings dictionaries.

    This function replaces the previous approach of rendering many individual
    ``*.opt`` Jinja2 templates and a ``roms.in`` template.  The ``cppdefs.opt``
    file is still produced separately via :func:`render_roms_settings`; all
    other former opt-file parameters are now collected here.

    The output file is written to ``<output_dir>/namelist.nml``.

    Parameters
    ----------
    settings_compile_time : dict
        The fully merged compile-time settings dict (``_settings_compile_time``
        on the builder).  After the namelist refactor this contains only the
        ``"cppdefs"`` key; it is accepted for API compatibility but not used
        by this function (cppdefs go to ``cppdefs.opt`` via
        :func:`render_roms_settings`).
    settings_run_time : dict
        The fully merged run-time settings dict (``_settings_run_time`` on the
        builder).  Must have at minimum a ``"roms.in"`` top-level key holding
        the sub-sections for grid/forcing/time-stepping.  All former
        compile-time namelist sections (``"param"``, ``"tides"``,
        ``"river_frc"``, ``"blk_frc"``, ``"bgc"``, ``"ocean_vars"``, etc.)
        are now top-level keys of this dict alongside ``"lin_rho_eos"``,
        ``"sss_correction"``, ``"sst_correction"``, and ``"marbl_bgc"``.
    output_dir : str or Path
        Directory into which ``namelist.nml`` will be written.  Must already
        exist.
    n_tracers : int
        Total number of model tracers (temperature + salinity + passive +
        biogeochemistry).  Used to expand scalar mixing/diffusion defaults into
        per-tracer arrays (``akt_bak``, ``tnu2``).

    Notes
    -----
    * Fortran namelists are case-insensitive, so the lowercase group names
      written by ``f90nml`` are equivalent to the uppercase names in the
      reference ``namelist.nml``.
    * Fields present in the settings dicts that have no counterpart in the
      namelist (e.g. ``surf_flux.sst_vname``, ``river_frc.rvol_vname``) are
      silently ignored — they were only needed by the old Jinja2 templates.
    * An empty ``frcfile`` list (all forcing paths ``None``) results in a
      ``&forcing_files`` group with no ``frcfile`` key, which is valid.
    """
    output_dir = Path(output_dir)
    nml_path = output_dir / "namelist.nml"

    # ------------------------------------------------------------------
    # Convenience aliases
    # ------------------------------------------------------------------
    rt = settings_run_time.get("roms.in", {})

    # Namelist sub-dicts (all run-time; fall back to empty dict so .get() is safe)
    param       = settings_run_time.get("param", {})
    blk_frc     = settings_run_time.get("blk_frc", {})
    flux_frc    = settings_run_time.get("flux_frc", {})
    river_frc   = settings_run_time.get("river_frc", {})
    tides       = settings_run_time.get("tides", {})
    ocean_vars  = settings_run_time.get("ocean_vars", {})
    ts_out      = settings_run_time.get("ts_output", {})
    frc_out     = settings_run_time.get("frc_output", {})
    extract     = settings_run_time.get("extract_data", {})
    sponge      = settings_run_time.get("sponge_tune", {})
    calc_pflx   = settings_run_time.get("calc_pflx", {})
    diagnostics = settings_run_time.get("diagnostics", {})
    zslice      = settings_run_time.get("zslice", {})
    bgc         = settings_run_time.get("bgc", {})
    cdr_frc     = settings_run_time.get("cdr_frc", {})
    cdr_out     = settings_run_time.get("cdr_output", {})
    upscale     = settings_run_time.get("upscale_output", {})
    surf_flux   = settings_run_time.get("surf_flux", {})
    stdout_diag = settings_run_time.get("stdout_diag", {})
    rand_out    = settings_run_time.get("random_output", {})
    pipe_frc    = settings_run_time.get("pipe_frc", {})
    particles   = settings_run_time.get("particles", {})
    lin_rho_eos = settings_run_time.get("lin_rho_eos", {})
    sss_corr    = settings_run_time.get("sss_correction", {})
    sst_corr    = settings_run_time.get("sst_correction", {})
    marbl_bgc   = settings_run_time.get("marbl_bgc", {})

    # roms.in sub-dicts
    title_sec   = rt.get("title", {})
    out_root    = rt.get("output_root_name", {})
    time_step   = rt.get("time_stepping", {})
    grid_sec    = rt.get("grid", {})
    s_coord     = rt.get("s_coord", {})
    forcing     = rt.get("forcing", {})
    lat_visc    = rt.get("lateral_visc", {})
    vert_mix    = rt.get("vertical_mixing", {})
    trc_diff2   = rt.get("tracer_diff2", {})
    bot_drag    = rt.get("bottom_drag", {})
    v_sponge    = rt.get("v_sponge", {})
    initial_sec = rt.get("initial", {})

    # ------------------------------------------------------------------
    # Build per-tracer arrays for mixing / diffusion
    # ------------------------------------------------------------------
    tnu2_scalar  = float(trc_diff2.get("tnu2_default", 0.0))
    akv_bak_val  = float(vert_mix.get("akv", 0.0))
    akt_scalar   = float(vert_mix.get("akt_default", 0.0))
    tnu2_list    = [tnu2_scalar] * n_tracers
    akt_bak_list = [akt_scalar]  * n_tracers

    # ------------------------------------------------------------------
    # Collect non-None forcing file paths in canonical order
    # ------------------------------------------------------------------
    _forcing_keys = [
        "surface_forcing_path",
        "surface_forcing_bgc_path",
        "boundary_forcing_path",
        "boundary_forcing_bgc_path",
        "tidal_forcing_path",
        "river_path",
    ]
    frcfile = [str(forcing[k]) for k in _forcing_keys if forcing.get(k) is not None]

    # ------------------------------------------------------------------
    # Assemble namelist sections in the same order as the reference file
    # ------------------------------------------------------------------
    nml_dict: Dict[str, Any] = {}

    # ---- Simulation name ----
    nml_dict["simulation_name_settings"] = {
        "output_root_name": str(out_root.get("output_root_name", "roms")),
        "title":            str(title_sec.get("casename", "roms_run")),
    }

    # ---- Time stepping ----
    nml_dict["time_stepping"] = {
        "ntimes":  int(time_step.get("ntimes", 1)),
        "dt":      float(time_step.get("dt", 60.0)),
        "ndtfast": int(time_step.get("ndtfast", 30)),
        "ninfo":   int(time_step.get("ninfo", 1)),
    }

    # ---- Grid ----
    nml_dict["grid_settings"] = {
        "grdname": str(grid_sec.get("grid_file", "")),
    }

    # ---- S-coordinate ----
    nml_dict["s_coord"] = {
        "theta_s": float(s_coord.get("theta_s", 6.0)),
        "theta_b": float(s_coord.get("theta_b", 6.0)),
        "hc":      float(s_coord.get("tcline", 25.0)),   # tcline → hc
    }

    # ---- Processor / grid dimensions (formerly param.opt) ----
    nml_dict["param_settings"] = {
        "np_xi":      int(param.get("NP_XI", 1)),
        "np_eta":     int(param.get("NP_ETA", 1)),
        "nsub_x":     int(param.get("NSUB_X", 1)),
        "nsub_e":     int(param.get("NSUB_E", 1)),
        "llm":        int(param.get("LLm", 100)),
        "mmm":        int(param.get("MMm", 100)),
        "n":          int(param.get("N", 50)),
        "nt_passive": int(param.get("nt_passive", 0)),
        "ntrc_bio":   int(param.get("ntrc_bio", 0)),
    }

    # ---- Initial conditions ----
    nml_dict["initial_conditions"] = {
        "ininame": str(initial_sec.get("initial_file", "")),   # initial_file → ininame
        "nrrec":   int(initial_sec.get("nrrec", 1)),
    }

    # ---- Forcing files (list; omit key entirely if empty) ----
    forcing_section: Dict[str, Any] = {}
    if frcfile:
        forcing_section["frcfile"] = frcfile
    nml_dict["forcing_files"] = forcing_section

    # ---- Bulk forcing (formerly blk_frc.opt) ----
    nml_dict["bulk_frc_settings"] = {
        "interp_bulk_frc":      bool(blk_frc.get("interp_frc", True)),          # interp_frc → interp_bulk_frc
        "check_bulk_frc_units": bool(blk_frc.get("check_bulk_frc_units", False)),
    }

    # ---- Flux forcing (new section) ----
    nml_dict["flux_frc_settings"] = {
        "interp_flux_frc": bool(flux_frc.get("interp_flux_frc", True)),
    }

    # ---- River forcing (formerly river_frc.opt) ----
    nml_dict["river_frc_settings"] = {
        "river_source":     bool(river_frc.get("river_source", False)),
        "river_analytical": bool(river_frc.get("analytical", False)),   # analytical → river_analytical
        "nriv":             int(river_frc.get("nriv", 1)),
    }

    # ---- Tides (formerly tides.opt) ----
    nml_dict["tides_settings"] = {
        "bry_tides": bool(tides.get("bry_tides", False)),
        "pot_tides": bool(tides.get("pot_tides", False)),
        "ana_tides": bool(tides.get("ana_tides", False)),
        "ntides":    int(tides.get("ntides", 10)),
    }

    # ---- Basic ocean output (formerly ocean_vars.opt) ----
    nml_dict["basic_output_settings"] = {
        "wrt_file_his":      bool(ocean_vars.get("wrt_file_his", False)),
        "output_period_his": float(ocean_vars.get("output_period_his", 86400.0)),
        "nrpf_his":          int(ocean_vars.get("nrpf_his", 7)),
        "wrt_z":             bool(ocean_vars.get("wrt_Z", True)),
        "wrt_ub":            bool(ocean_vars.get("wrt_Ub", True)),
        "wrt_vb":            bool(ocean_vars.get("wrt_Vb", True)),
        "wrt_u":             bool(ocean_vars.get("wrt_U", True)),
        "wrt_v":             bool(ocean_vars.get("wrt_V", True)),
        "wrt_r":             bool(ocean_vars.get("wrt_R", False)),
        "wrt_o":             bool(ocean_vars.get("wrt_O", False)),
        "wrt_w":             bool(ocean_vars.get("wrt_W", True)),
        "wrt_akv":           bool(ocean_vars.get("wrt_Akv", False)),
        "wrt_akt":           bool(ocean_vars.get("wrt_Akt", False)),
        "wrt_aks":           bool(ocean_vars.get("wrt_Aks", False)),
        "wrt_hbls":          bool(ocean_vars.get("wrt_Hbls", False)),
        "wrt_hbbl":          bool(ocean_vars.get("wrt_Hbbl", False)),
        "wrt_file_avg":      bool(ocean_vars.get("wrt_file_avg", False)),
        "output_period_avg": float(ocean_vars.get("output_period_avg", 604800.0)),
        "nrpf_avg":          int(ocean_vars.get("nrpf_avg", 1)),
        "wrt_avg_z":         bool(ocean_vars.get("wrt_avg_Z", True)),
        "wrt_avg_ub":        bool(ocean_vars.get("wrt_avg_Ub", True)),
        "wrt_avg_vb":        bool(ocean_vars.get("wrt_avg_Vb", True)),
        "wrt_avg_u":         bool(ocean_vars.get("wrt_avg_U", True)),
        "wrt_avg_v":         bool(ocean_vars.get("wrt_avg_V", True)),
        "wrt_avg_r":         bool(ocean_vars.get("wrt_avg_R", True)),
        "wrt_avg_o":         bool(ocean_vars.get("wrt_avg_O", True)),
        "wrt_avg_w":         bool(ocean_vars.get("wrt_avg_W", True)),
        "wrt_avg_akv":       bool(ocean_vars.get("wrt_avg_Akv", True)),
        "wrt_avg_akt":       bool(ocean_vars.get("wrt_avg_Akt", True)),
        "wrt_avg_aks":       bool(ocean_vars.get("wrt_avg_Aks", True)),
        "wrt_avg_hbls":      bool(ocean_vars.get("wrt_avg_Hbls", True)),
        "wrt_avg_hbbl":      bool(ocean_vars.get("wrt_avg_Hbbl", True)),
        "wrt_file_rst":      bool(ocean_vars.get("wrt_file_rst", True)),
        "monthly_restarts":  bool(ocean_vars.get("monthly_restarts", False)),
        "output_period_rst": float(ocean_vars.get("output_period_rst", 86400.0)),
        "nrpf_rst":          int(ocean_vars.get("nrpf_rst", 2)),
    }

    # ---- Tracer (T/S) output (new section; formerly part of tracers.opt) ----
    nml_dict["ts_output_settings"] = {
        "wrt_temp":     bool(ts_out.get("wrt_temp", False)),
        "wrt_salt":     bool(ts_out.get("wrt_salt", False)),
        "wrt_temp_dia": bool(ts_out.get("wrt_temp_dia", False)),
        "wrt_salt_dia": bool(ts_out.get("wrt_salt_dia", False)),
    }

    # ---- Forcing output (new section) ----
    nml_dict["frc_output_settings"] = {
        "wrt_frc":     bool(frc_out.get("wrt_frc", False)),
        "wrt_frc_avg": bool(frc_out.get("wrt_frc_avg", False)),
        "output_period": float(frc_out.get("output_period", 3600.0)),
        "nrpf":          int(frc_out.get("nrpf", 4)),
    }

    # ---- Nested-grid extraction (formerly extract_data.opt) ----
    nml_dict["extract_data_settings"] = {
        "do_extract":    bool(extract.get("do_extract", False)),
        "extract_period": float(extract.get("extract_period", 1800.0)),
        "nrpf_extract":  int(extract.get("nrpf", 48)),      # nrpf → nrpf_extract
        "extract_file":  str(extract.get("extract_file", "sample_edata.nc")),
        "n_chd":         int(extract.get("N_chd", 90)),
        "theta_s_chd":   float(extract.get("theta_s_chd", 5.0)),
        "theta_b_chd":   float(extract.get("theta_b_chd", 2.0)),
        "hc_chd":        float(extract.get("hc_chd", 250.0)),
    }

    # ---- Sponge tuning (formerly sponge_tune.opt) ----
    nml_dict["sponge_tune_settings"] = {
        "ub_tune":      bool(sponge.get("ub_tune", False)),
        "sp_timscale":  float(sponge.get("sp_timscale", 86400.0)),
        "wrt_sponge":   bool(sponge.get("wrt_sponge", True)),
        "spn_avg":      bool(sponge.get("spn_avg", True)),
        "nrpf":         int(sponge.get("nrpf", 7)),
        "output_period": float(sponge.get("output_period", 86400.0)),
    }

    # ---- Baroclinic pressure flux (new section; absorbs diag_pflx from diagnostics.opt) ----
    # Prefer calc_pflx section; fall back to the old diagnostics.diag_pflx / timescale fields.
    nml_dict["calc_pflx_settings"] = {
        "calc_pflx": bool(calc_pflx.get("calc_pflx",
                          diagnostics.get("diag_pflx", True))),
        "timescale":  float(calc_pflx.get("timescale",
                            diagnostics.get("timescale", 86400.0))),
    }

    # ---- Z-level output (new section) ----
    nml_dict["zslice_settings"] = {
        "do_zslice":    bool(zslice.get("do_zslice", False)),
        "zslice_avg":   bool(zslice.get("zslice_avg", False)),
        "wrt_t_zsl":    bool(zslice.get("wrt_T_zsl", False)),
        "wrt_u_zsl":    bool(zslice.get("wrt_U_zsl", False)),
        "wrt_v_zsl":    bool(zslice.get("wrt_V_zsl", False)),
        "output_period": float(zslice.get("output_period", 1200.0)),
        "nrpf":          int(zslice.get("nrpf", 72)),
        "ndep":          int(zslice.get("ndep", 2)),
        "vecdep":        list(zslice.get("vecdep", [-2.0, -15.0])),
        "nt_z":          int(zslice.get("nt_z", 2)),
        "trc2zsc":       list(zslice.get("trc2zsc", [1, 2])),
    }

    # ---- BGC output (formerly bgc.opt) ----
    nml_dict["bgc_settings"] = {
        "interp_bgc_frc":       bool(bgc.get("interp_frc", False)),           # interp_frc → interp_bgc_frc
        "wrt_bgc_his":          bool(bgc.get("wrt_his", False)),               # wrt_his → wrt_bgc_his
        "output_period_his":    float(bgc.get("output_period_his", 86400.0)),
        "nrpf_his":             int(bgc.get("nrpf_his", 7)),
        "wrt_bgc_avg":          bool(bgc.get("wrt_avg", False)),               # wrt_avg → wrt_bgc_avg
        "output_period_avg":    float(bgc.get("output_period_avg", 86400.0)),
        "nrpf_avg":             int(bgc.get("nrpf_avg", 7)),
        "wrt_bgc_dia_his":      bool(bgc.get("wrt_his_dia", False)),           # wrt_his_dia → wrt_bgc_dia_his
        "output_period_his_dia": float(bgc.get("output_period_his_dia", 86400.0)),
        "nrpf_his_dia":         int(bgc.get("nrpf_his_dia", 7)),
        "wrt_bgc_dia_avg":      bool(bgc.get("wrt_avg_dia", False)),           # wrt_avg_dia → wrt_bgc_dia_avg
        "output_period_avg_dia": float(bgc.get("output_period_avg_dia", 60.0)),
        "nrpf_avg_dia":         int(bgc.get("nrpf_avg_dia", 1)),
    }

    # ---- MARBL biogeochemistry settings (new section; formerly in roms.in) ----
    nml_dict["marbl_biogeochemistry_settings"] = {
        "marbl_config_file":          str(marbl_bgc.get("marbl_config_file", "marbl_in")),
        "marbl_tracers_to_write":     str(marbl_bgc.get("marbl_tracers_to_write", "")),
        "marbl_diagnostics_to_write": str(marbl_bgc.get("marbl_diagnostics_to_write", "")),
        "marbl_timestep_ratio":       int(marbl_bgc.get("marbl_timestep_ratio", 1)),
    }

    # ---- CDR forcing (formerly cdr_frc.opt) ----
    nml_dict["cdr_frc_settings"] = {
        "cdr_source":            bool(cdr_frc.get("cdr_source", False)),
        "cdr_file":              str(cdr_frc.get("cdr_file", "cdr_release.nc")),
        "ncdr_parm":             int(cdr_frc.get("ncdr_parm", 1)),
        "nz_chd":                int(cdr_frc.get("nz_chd", 50)),
        "forcing_depth_profiles": bool(cdr_frc.get("forcing_depth_profiles", False)),
        "forcing_3d":            bool(cdr_frc.get("forcing_3d", False)),
        "forcing_parameterized": bool(cdr_frc.get("forcing_parameterized", False)),
        "time_interpolation":    bool(cdr_frc.get("time_interpolation", False)),
        "relocate_to_wet_pts":   bool(cdr_frc.get("relocate_to_wet_pts", False)),
        "cdr_volume":            bool(cdr_frc.get("cdr_volume", False)),
    }

    # ---- CDR output (formerly cdr_output.opt) ----
    nml_dict["cdr_output_settings"] = {
        "do_cdr_output":         bool(cdr_out.get("do_cdr", False)),           # do_cdr → do_cdr_output
        "wrt_cdr_avg":           bool(cdr_out.get("do_avg", True)),            # do_avg → wrt_cdr_avg
        "cdr_monthly_averages":  bool(cdr_out.get("monthly_averages", False)), # monthly_averages → cdr_monthly_averages
        "output_period":         float(cdr_out.get("output_period", 3600.0)),
        "nrpf":                  int(cdr_out.get("nrpf", 4)),
    }

    # ---- Upscaling (formerly upscale_output.opt) ----
    nml_dict["upscale_settings"] = {
        "do_upscale":        bool(upscale.get("do_upscale", False)),
        "nrpf_uscl":         int(upscale.get("nrpf_uscl", 48)),
        "output_period_uscl": float(upscale.get("output_period_uscl", 3600.0)),
    }

    # ---- Linear EOS (new top-level run-time section; formerly scalars.F / roms.in) ----
    nml_dict["lin_rho_eos_settings"] = {
        "tcoef": float(lin_rho_eos.get("Tcoef", 0.2)),
        "t0":    float(lin_rho_eos.get("T0", 1.0)),
        "scoef": float(lin_rho_eos.get("Scoef", 0.822)),
        "s0":    float(lin_rho_eos.get("S0", 1.0)),
    }

    # ---- Reference density ----
    nml_dict["rho0_settings"] = {
        "rho0": float(lat_visc.get("rho0", 1027.5)),
    }

    # ---- Slipperiness ----
    nml_dict["gamma2_settings"] = {
        "gamma2": float(rt.get("gamma2", 1.0)),
    }

    # ---- Tracer diffusion (per-tracer array; formerly roms.in) ----
    nml_dict["tracer_diff2"] = {
        "tnu2": tnu2_list,
    }

    # ---- Bottom drag ----
    nml_dict["bottom_drag_settings"] = {
        "rdrg":  float(bot_drag.get("rdrg", 0.0)),
        "rdrg2": float(bot_drag.get("rdrg2", 1.0e-3)),
        "zob":   float(bot_drag.get("zob", 1.0e-2)),
    }

    # ---- Vertical mixing (per-tracer array; formerly roms.in) ----
    nml_dict["vertical_mixing_settings"] = {
        "akv_bak": akv_bak_val,
        "akt_bak": akt_bak_list,
    }

    # ---- Lateral viscosity ----
    nml_dict["lateral_visc_settings"] = {
        "visc2": float(lat_visc.get("visc2", 0.0)),
    }

    # ---- Open-boundary binding velocity ----
    nml_dict["ubind_settings"] = {
        "ubind": float(rt.get("ubind", 0.1)),
    }

    # ---- Sponge maximum viscosity ----
    nml_dict["v_sponge_settings"] = {
        "v_sponge": float(v_sponge.get("v_sponge", 1.0)),
    }

    # ---- SSS / SST surface restoring (new sections; formerly roms.in scalars) ----
    nml_dict["sss_correction"] = {
        "dsssdt": float(sss_corr.get("dSSSdt", 0.0)),
    }
    nml_dict["sst_correction"] = {
        "dsstdt": float(sst_corr.get("dSSTdt", 0.0)),
    }

    # ---- Diagnostics (formerly diagnostics.opt; diag_pflx moved to calc_pflx) ----
    nml_dict["diagnostics_settings"] = {
        "diag_avg":       bool(diagnostics.get("diag_avg", False)),
        "diag_uv":        bool(diagnostics.get("diag_uv", False)),
        "diag_trc":       bool(diagnostics.get("diag_trc", False)),
        "output_period":  float(diagnostics.get("output_period", 86400.0)),
        "nrpf":           int(diagnostics.get("nrpf", 7)),
        "code_check_mode": bool(diagnostics.get("code_check_mode",
                                ocean_vars.get("code_check", False))),
    }

    # ---- Stdout diagnostics (new section) ----
    nml_dict["stdout_diag_settings"] = {
        "code_check_mode": bool(stdout_diag.get("code_check_mode", False)),
    }

    # ---- Random / custom output (new section) ----
    nml_dict["random_output_settings"] = {
        "do_random":    bool(rand_out.get("do_random", False)),
        "output_period": float(rand_out.get("output_period", 3600.0)),
        "nrpf":          int(rand_out.get("nrpf", 10)),
    }

    # ---- Surface flux output (formerly surf_flux.opt) ----
    nml_dict["surf_flx_settings"] = {
        "wrt_smflx":    bool(surf_flux.get("wrt_smflx", False)),
        "wrt_stflx":    bool(surf_flux.get("wrt_stflx", False)),
        "wrt_swflx":    bool(surf_flux.get("wrt_swflx", False)),
        "sflx_avg":     bool(surf_flux.get("sflx_avg", False)),
        "output_period": float(surf_flux.get("output_period", 120.0)),
        "nrpf":          int(surf_flux.get("nrpf", 10)),
    }

    # ---- Pipe forcing (new section) ----
    nml_dict["pipe_frc_settings"] = {
        "pipe_source":  bool(pipe_frc.get("pipe_source", False)),
        "p_analytical": bool(pipe_frc.get("p_analytical", False)),
        "npip":         int(pipe_frc.get("npip", 1)),
    }

    # ---- Lagrangian particles (new section) ----
    nml_dict["particles_settings"] = {
        "floats":           bool(particles.get("floats", False)),
        "np":               int(particles.get("np", 50)),
        "extra_space_fac":  float(particles.get("extra_space_fac", 1.5)),
        "exchange_facx":    float(particles.get("exchange_facx", 0.1)),
        "exchange_facy":    float(particles.get("exchange_facy", 0.1)),
        "exchange_facc":    float(particles.get("exchange_facc", 0.01)),
        "output_period":    float(particles.get("output_period", 400.0)),
        "nrpf":             int(particles.get("nrpf", 100)),
        "ppm3":             float(particles.get("ppm3", 1.0e-6)),
        "pmin":             int(particles.get("pmin", 200)),
    }

    # ------------------------------------------------------------------
    # Write the namelist file
    # ------------------------------------------------------------------
    nml = f90nml.Namelist(nml_dict)
    nml.write(str(nml_path), force=True)

