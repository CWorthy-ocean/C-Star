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
        builder).  Every namelist section is a top-level key: grid/forcing/
        time-stepping sections (``"grid"``, ``"forcing"``, ``"time_stepping"``,
        ``"s_coord"``, ``"initial"``, ``"title"``, …) sit flat alongside the
        former compile-time sections (``"param"``, ``"tides"``, ``"river_frc"``,
        ``"blk_frc"``, ``"bgc"``, ``"ocean_vars"``, …) and the newer
        ``"lin_rho_eos"``, ``"sss_correction"``, ``"sst_correction"``, and
        ``"marbl_bgc"``.
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
    #
    # Every section and key read below is guaranteed to be present: the
    # builder initializes both settings dicts from the model's
    # ``run-time-defaults.yml`` (deep-copied, then merged with overrides and
    # dynamic values) before calling this function. We therefore index
    # directly rather than carrying inline hard-coded fallbacks — a missing
    # key indicates an incomplete defaults YAML and should fail loudly.
    # ------------------------------------------------------------------
    # ``rt`` aliases the run-time dict itself: all sections (former roms.in
    # entries and the rest) now live flat at the top level.
    rt = settings_run_time

    # Namelist sub-dicts (all top-level run-time sections)
    param       = settings_run_time["param"]
    blk_frc     = settings_run_time["blk_frc"]
    flux_frc    = settings_run_time["flux_frc"]
    river_frc   = settings_run_time["river_frc"]
    tides       = settings_run_time["tides"]
    ocean_vars  = settings_run_time["ocean_vars"]
    ts_out      = settings_run_time["ts_output"]
    frc_out     = settings_run_time["frc_output"]
    extract     = settings_run_time["extract_data"]
    sponge      = settings_run_time["sponge_tune"]
    calc_pflx   = settings_run_time["calc_pflx"]
    diagnostics = settings_run_time["diagnostics"]
    zslice      = settings_run_time["zslice"]
    bgc         = settings_run_time["bgc"]
    cdr_frc     = settings_run_time["cdr_frc"]
    cdr_out     = settings_run_time["cdr_output"]
    upscale     = settings_run_time["upscale_output"]
    surf_flux   = settings_run_time["surf_flux"]
    stdout_diag = settings_run_time["stdout_diag"]
    rand_out    = settings_run_time["random_output"]
    pipe_frc    = settings_run_time["pipe_frc"]
    particles   = settings_run_time["particles"]
    lin_rho_eos = settings_run_time["lin_rho_eos"]
    sss_corr    = settings_run_time["sss_correction"]
    sst_corr    = settings_run_time["sst_correction"]
    marbl_bgc   = settings_run_time["marbl_bgc"]

    # Former roms.in sub-dicts (now top-level)
    title_sec   = rt["title"]
    out_root    = rt["output_root_name"]
    time_step   = rt["time_stepping"]
    grid_sec    = rt["grid"]
    s_coord     = rt["s_coord"]
    forcing     = rt["forcing"]
    lat_visc    = rt["lateral_visc"]
    vert_mix    = rt["vertical_mixing"]
    trc_diff2   = rt["tracer_diff2"]
    bot_drag    = rt["bottom_drag"]
    v_sponge    = rt["v_sponge"]
    initial_sec = rt["initial"]

    # ------------------------------------------------------------------
    # Build per-tracer arrays for mixing / diffusion (scalar default → list)
    # ------------------------------------------------------------------
    tnu2_list    = [float(trc_diff2["tnu2_default"])] * n_tracers
    akv_bak_val  = float(vert_mix["akv"])
    akt_bak_list = [float(vert_mix["akt_default"])] * n_tracers

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
    frcfile = [str(forcing[k]) for k in _forcing_keys if forcing[k] is not None]

    # ------------------------------------------------------------------
    # Assemble namelist sections in the same order as the reference file
    # ------------------------------------------------------------------
    nml_dict: Dict[str, Any] = {}

    # ---- Simulation name ----
    nml_dict["simulation_name_settings"] = {
        "output_root_name": str(out_root["output_root_name"]),
        "title":            str(title_sec["casename"]),
    }

    # ---- Time stepping ----
    nml_dict["time_stepping"] = {
        "ntimes":  int(time_step["ntimes"]),
        "dt":      float(time_step["dt"]),
        "ndtfast": int(time_step["ndtfast"]),
        "ninfo":   int(time_step["ninfo"]),
    }

    # ---- Grid ----
    nml_dict["grid_settings"] = {
        "grdname": str(grid_sec["grid_file"]),
    }

    # ---- S-coordinate ----
    nml_dict["s_coord"] = {
        "theta_s": float(s_coord["theta_s"]),
        "theta_b": float(s_coord["theta_b"]),
        "hc":      float(s_coord["tcline"]),   # tcline → hc
    }

    # ---- Processor / grid dimensions (formerly param.opt) ----
    nml_dict["param_settings"] = {
        "np_xi":      int(param["NP_XI"]),
        "np_eta":     int(param["NP_ETA"]),
        "nsub_x":     int(param["NSUB_X"]),
        "nsub_e":     int(param["NSUB_E"]),
        "llm":        int(param["LLm"]),
        "mmm":        int(param["MMm"]),
        "n":          int(param["N"]),
        "nt_passive": int(param["nt_passive"]),
        "ntrc_bio":   int(param["ntrc_bio"]),
    }

    # ---- Initial conditions ----
    nml_dict["initial_conditions"] = {
        "ininame": str(initial_sec["initial_file"]),   # initial_file → ininame
        "nrrec":   int(initial_sec["nrrec"]),
    }

    # ---- Forcing files (list; omit key entirely if empty) ----
    forcing_section: Dict[str, Any] = {}
    if frcfile:
        forcing_section["frcfile"] = frcfile
    nml_dict["forcing_files"] = forcing_section

    # ---- Bulk forcing (formerly blk_frc.opt) ----
    nml_dict["bulk_frc_settings"] = {
        "interp_bulk_frc":      bool(blk_frc["interp_frc"]),          # interp_frc → interp_bulk_frc
        "check_bulk_frc_units": bool(blk_frc["check_bulk_frc_units"]),
    }

    # ---- Flux forcing (new section) ----
    nml_dict["flux_frc_settings"] = {
        "interp_flux_frc": bool(flux_frc["interp_flux_frc"]),
    }

    # ---- River forcing (formerly river_frc.opt) ----
    nml_dict["river_frc_settings"] = {
        "river_source":     bool(river_frc["river_source"]),
        "river_analytical": bool(river_frc["analytical"]),   # analytical → river_analytical
        "nriv":             int(river_frc["nriv"]),
    }

    # ---- Tides (formerly tides.opt) ----
    nml_dict["tides_settings"] = {
        "bry_tides": bool(tides["bry_tides"]),
        "pot_tides": bool(tides["pot_tides"]),
        "ana_tides": bool(tides["ana_tides"]),
        "ntides":    int(tides["ntides"]),
    }

    # ---- Basic ocean output (formerly ocean_vars.opt) ----
    nml_dict["basic_output_settings"] = {
        "wrt_file_his":      bool(ocean_vars["wrt_file_his"]),
        "output_period_his": float(ocean_vars["output_period_his"]),
        "nrpf_his":          int(ocean_vars["nrpf_his"]),
        "wrt_z":             bool(ocean_vars["wrt_Z"]),
        "wrt_ub":            bool(ocean_vars["wrt_Ub"]),
        "wrt_vb":            bool(ocean_vars["wrt_Vb"]),
        "wrt_u":             bool(ocean_vars["wrt_U"]),
        "wrt_v":             bool(ocean_vars["wrt_V"]),
        "wrt_r":             bool(ocean_vars["wrt_R"]),
        "wrt_o":             bool(ocean_vars["wrt_O"]),
        "wrt_w":             bool(ocean_vars["wrt_W"]),
        "wrt_akv":           bool(ocean_vars["wrt_Akv"]),
        "wrt_akt":           bool(ocean_vars["wrt_Akt"]),
        "wrt_aks":           bool(ocean_vars["wrt_Aks"]),
        "wrt_hbls":          bool(ocean_vars["wrt_Hbls"]),
        "wrt_hbbl":          bool(ocean_vars["wrt_Hbbl"]),
        "wrt_file_avg":      bool(ocean_vars["wrt_file_avg"]),
        "output_period_avg": float(ocean_vars["output_period_avg"]),
        "nrpf_avg":          int(ocean_vars["nrpf_avg"]),
        "wrt_avg_z":         bool(ocean_vars["wrt_avg_Z"]),
        "wrt_avg_ub":        bool(ocean_vars["wrt_avg_Ub"]),
        "wrt_avg_vb":        bool(ocean_vars["wrt_avg_Vb"]),
        "wrt_avg_u":         bool(ocean_vars["wrt_avg_U"]),
        "wrt_avg_v":         bool(ocean_vars["wrt_avg_V"]),
        "wrt_avg_r":         bool(ocean_vars["wrt_avg_R"]),
        "wrt_avg_o":         bool(ocean_vars["wrt_avg_O"]),
        "wrt_avg_w":         bool(ocean_vars["wrt_avg_W"]),
        "wrt_avg_akv":       bool(ocean_vars["wrt_avg_Akv"]),
        "wrt_avg_akt":       bool(ocean_vars["wrt_avg_Akt"]),
        "wrt_avg_aks":       bool(ocean_vars["wrt_avg_Aks"]),
        "wrt_avg_hbls":      bool(ocean_vars["wrt_avg_Hbls"]),
        "wrt_avg_hbbl":      bool(ocean_vars["wrt_avg_Hbbl"]),
        "wrt_file_rst":      bool(ocean_vars["wrt_file_rst"]),
        "monthly_restarts":  bool(ocean_vars["monthly_restarts"]),
        "output_period_rst": float(ocean_vars["output_period_rst"]),
        "nrpf_rst":          int(ocean_vars["nrpf_rst"]),
    }

    # ---- Tracer (T/S) output (new section; formerly part of tracers.opt) ----
    nml_dict["ts_output_settings"] = {
        "wrt_temp":     bool(ts_out["wrt_temp"]),
        "wrt_salt":     bool(ts_out["wrt_salt"]),
        "wrt_temp_dia": bool(ts_out["wrt_temp_dia"]),
        "wrt_salt_dia": bool(ts_out["wrt_salt_dia"]),
    }

    # ---- Forcing output (new section) ----
    nml_dict["frc_output_settings"] = {
        "wrt_frc":     bool(frc_out["wrt_frc"]),
        "wrt_frc_avg": bool(frc_out["wrt_frc_avg"]),
        "output_period": float(frc_out["output_period"]),
        "nrpf":          int(frc_out["nrpf"]),
    }

    # ---- Nested-grid extraction (formerly extract_data.opt) ----
    nml_dict["extract_data_settings"] = {
        "do_extract":    bool(extract["do_extract"]),
        "extract_period": float(extract["extract_period"]),
        "nrpf_extract":  int(extract["nrpf"]),      # nrpf → nrpf_extract
        "extract_file":  str(extract["extract_file"]),
        "n_chd":         int(extract["N_chd"]),
        "theta_s_chd":   float(extract["theta_s_chd"]),
        "theta_b_chd":   float(extract["theta_b_chd"]),
        "hc_chd":        float(extract["hc_chd"]),
    }

    # ---- Sponge tuning (formerly sponge_tune.opt) ----
    nml_dict["sponge_tune_settings"] = {
        "ub_tune":      bool(sponge["ub_tune"]),
        "sp_timscale":  float(sponge["sp_timscale"]),
        "wrt_sponge":   bool(sponge["wrt_sponge"]),
        "spn_avg":      bool(sponge["spn_avg"]),
        "nrpf":         int(sponge["nrpf"]),
        "output_period": float(sponge["output_period"]),
    }

    # ---- Baroclinic pressure flux (new section; absorbs diag_pflx from diagnostics.opt) ----
    nml_dict["calc_pflx_settings"] = {
        "calc_pflx": bool(calc_pflx["calc_pflx"]),
        "timescale":  float(calc_pflx["timescale"]),
    }

    # ---- Z-level output (new section) ----
    nml_dict["zslice_settings"] = {
        "do_zslice":    bool(zslice["do_zslice"]),
        "zslice_avg":   bool(zslice["zslice_avg"]),
        "wrt_t_zsl":    bool(zslice["wrt_T_zsl"]),
        "wrt_u_zsl":    bool(zslice["wrt_U_zsl"]),
        "wrt_v_zsl":    bool(zslice["wrt_V_zsl"]),
        "output_period": float(zslice["output_period"]),
        "nrpf":          int(zslice["nrpf"]),
        "ndep":          int(zslice["ndep"]),
        "vecdep":        list(zslice["vecdep"]),
        "nt_z":          int(zslice["nt_z"]),
        "trc2zsc":       list(zslice["trc2zsc"]),
    }

    # ---- BGC output (formerly bgc.opt) ----
    nml_dict["bgc_settings"] = {
        "interp_bgc_frc":       bool(bgc["interp_frc"]),           # interp_frc → interp_bgc_frc
        "wrt_bgc_his":          bool(bgc["wrt_his"]),               # wrt_his → wrt_bgc_his
        "output_period_his":    float(bgc["output_period_his"]),
        "nrpf_his":             int(bgc["nrpf_his"]),
        "wrt_bgc_avg":          bool(bgc["wrt_avg"]),               # wrt_avg → wrt_bgc_avg
        "output_period_avg":    float(bgc["output_period_avg"]),
        "nrpf_avg":             int(bgc["nrpf_avg"]),
        "wrt_bgc_dia_his":      bool(bgc["wrt_his_dia"]),           # wrt_his_dia → wrt_bgc_dia_his
        "output_period_his_dia": float(bgc["output_period_his_dia"]),
        "nrpf_his_dia":         int(bgc["nrpf_his_dia"]),
        "wrt_bgc_dia_avg":      bool(bgc["wrt_avg_dia"]),           # wrt_avg_dia → wrt_bgc_dia_avg
        "output_period_avg_dia": float(bgc["output_period_avg_dia"]),
        "nrpf_avg_dia":         int(bgc["nrpf_avg_dia"]),
    }

    # ---- MARBL biogeochemistry settings (new section; formerly in roms.in) ----
    nml_dict["marbl_biogeochemistry_settings"] = {
        "marbl_config_file":          str(marbl_bgc["marbl_config_file"]),
        "marbl_tracers_to_write":     str(marbl_bgc["marbl_tracers_to_write"]),
        "marbl_diagnostics_to_write": str(marbl_bgc["marbl_diagnostics_to_write"]),
        "marbl_timestep_ratio":       int(marbl_bgc["marbl_timestep_ratio"]),
    }

    # ---- CDR forcing (formerly cdr_frc.opt) ----
    nml_dict["cdr_frc_settings"] = {
        "cdr_source":            bool(cdr_frc["cdr_source"]),
        "cdr_file":              str(cdr_frc["cdr_file"]),
        "ncdr_parm":             int(cdr_frc["ncdr_parm"]),
        "nz_chd":                int(cdr_frc["nz_chd"]),
        "forcing_depth_profiles": bool(cdr_frc["forcing_depth_profiles"]),
        "forcing_3d":            bool(cdr_frc["forcing_3d"]),
        "forcing_parameterized": bool(cdr_frc["forcing_parameterized"]),
        "time_interpolation":    bool(cdr_frc["time_interpolation"]),
        "relocate_to_wet_pts":   bool(cdr_frc["relocate_to_wet_pts"]),
        "cdr_volume":            bool(cdr_frc["cdr_volume"]),
    }

    # ---- CDR output (formerly cdr_output.opt) ----
    nml_dict["cdr_output_settings"] = {
        "do_cdr_output":         bool(cdr_out["do_cdr"]),           # do_cdr → do_cdr_output
        "wrt_cdr_avg":           bool(cdr_out["do_avg"]),            # do_avg → wrt_cdr_avg
        "cdr_monthly_averages":  bool(cdr_out["monthly_averages"]), # monthly_averages → cdr_monthly_averages
        "output_period":         float(cdr_out["output_period"]),
        "nrpf":                  int(cdr_out["nrpf"]),
    }

    # ---- Upscaling (formerly upscale_output.opt) ----
    nml_dict["upscale_settings"] = {
        "do_upscale":        bool(upscale["do_upscale"]),
        "nrpf_uscl":         int(upscale["nrpf_uscl"]),
        "output_period_uscl": float(upscale["output_period_uscl"]),
    }

    # ---- Linear EOS (new top-level run-time section; formerly scalars.F / roms.in) ----
    nml_dict["lin_rho_eos_settings"] = {
        "tcoef": float(lin_rho_eos["Tcoef"]),
        "t0":    float(lin_rho_eos["T0"]),
        "scoef": float(lin_rho_eos["Scoef"]),
        "s0":    float(lin_rho_eos["S0"]),
    }

    # ---- Reference density ----
    nml_dict["rho0_settings"] = {
        "rho0": float(lat_visc["rho0"]),
    }

    # ---- Slipperiness ----
    nml_dict["gamma2_settings"] = {
        "gamma2": float(rt["gamma2"]),
    }

    # ---- Tracer diffusion (per-tracer array; formerly roms.in) ----
    nml_dict["tracer_diff2"] = {
        "tnu2": tnu2_list,
    }

    # ---- Bottom drag ----
    nml_dict["bottom_drag_settings"] = {
        "rdrg":  float(bot_drag["rdrg"]),
        "rdrg2": float(bot_drag["rdrg2"]),
        "zob":   float(bot_drag["zob"]),
    }

    # ---- Vertical mixing (per-tracer array; formerly roms.in) ----
    nml_dict["vertical_mixing_settings"] = {
        "akv_bak": akv_bak_val,
        "akt_bak": akt_bak_list,
    }

    # ---- Lateral viscosity ----
    nml_dict["lateral_visc_settings"] = {
        "visc2": float(lat_visc["visc2"]),
    }

    # ---- Open-boundary binding velocity ----
    nml_dict["ubind_settings"] = {
        "ubind": float(rt["ubind"]),
    }

    # ---- Sponge maximum viscosity ----
    nml_dict["v_sponge_settings"] = {
        "v_sponge": float(v_sponge["v_sponge"]),
    }

    # ---- SSS / SST surface restoring (new sections; formerly roms.in scalars) ----
    nml_dict["sss_correction"] = {
        "dsssdt": float(sss_corr["dSSSdt"]),
    }
    nml_dict["sst_correction"] = {
        "dsstdt": float(sst_corr["dSSTdt"]),
    }

    # ---- Diagnostics (formerly diagnostics.opt; diag_pflx moved to calc_pflx) ----
    # code_check_mode has no diagnostics-section key; it is driven by ocean_vars.code_check.
    nml_dict["diagnostics_settings"] = {
        "diag_avg":       bool(diagnostics["diag_avg"]),
        "diag_uv":        bool(diagnostics["diag_uv"]),
        "diag_trc":       bool(diagnostics["diag_trc"]),
        "output_period":  float(diagnostics["output_period"]),
        "nrpf":           int(diagnostics["nrpf"]),
        "code_check_mode": bool(ocean_vars["code_check"]),
    }

    # ---- Stdout diagnostics (new section) ----
    nml_dict["stdout_diag_settings"] = {
        "code_check_mode": bool(stdout_diag["code_check_mode"]),
    }

    # ---- Random / custom output (new section) ----
    nml_dict["random_output_settings"] = {
        "do_random":    bool(rand_out["do_random"]),
        "output_period": float(rand_out["output_period"]),
        "nrpf":          int(rand_out["nrpf"]),
    }

    # ---- Surface flux output (formerly surf_flux.opt) ----
    nml_dict["surf_flx_settings"] = {
        "wrt_smflx":    bool(surf_flux["wrt_smflx"]),
        "wrt_stflx":    bool(surf_flux["wrt_stflx"]),
        "wrt_swflx":    bool(surf_flux["wrt_swflx"]),
        "sflx_avg":     bool(surf_flux["sflx_avg"]),
        "output_period": float(surf_flux["output_period"]),
        "nrpf":          int(surf_flux["nrpf"]),
    }

    # ---- Pipe forcing (new section) ----
    nml_dict["pipe_frc_settings"] = {
        "pipe_source":  bool(pipe_frc["pipe_source"]),
        "p_analytical": bool(pipe_frc["p_analytical"]),
        "npip":         int(pipe_frc["npip"]),
    }

    # ---- Lagrangian particles (new section) ----
    nml_dict["particles_settings"] = {
        "floats":           bool(particles["floats"]),
        "np":               int(particles["np"]),
        "extra_space_fac":  float(particles["extra_space_fac"]),
        "exchange_facx":    float(particles["exchange_facx"]),
        "exchange_facy":    float(particles["exchange_facy"]),
        "exchange_facc":    float(particles["exchange_facc"]),
        "output_period":    float(particles["output_period"]),
        "nrpf":             int(particles["nrpf"]),
        "ppm3":             float(particles["ppm3"]),
        "pmin":             int(particles["pmin"]),
    }

    # ------------------------------------------------------------------
    # Write the namelist file
    # ------------------------------------------------------------------
    nml = f90nml.Namelist(nml_dict)
    nml.write(str(nml_path), force=True)

