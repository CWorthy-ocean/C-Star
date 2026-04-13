"""
ROMS Configuration Template Renderer

This module provides functionality to render ROMS configuration files
from Jinja2 templates using a settings dictionary.
"""

import shutil
import warnings
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape, meta
from typing import Dict, Any, Union, Set, Optional


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
    
    # Check for settings_dict entries without templates
    missing_templates = settings_keys - template_keys
    if missing_templates:
        raise ValueError(
            f"Settings_dict entries without corresponding template files: {sorted(missing_templates)}. "
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

