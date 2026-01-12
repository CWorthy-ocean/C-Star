"""
Tests for the settings.py module.

Tests cover:
- render_roms_settings function
- ROMSTemplateRenderer class
- Template rendering
- Validation and error handling
"""
from pathlib import Path
import pytest
import yaml
from unittest.mock import MagicMock

from cson_forge.settings import render_roms_settings, ROMSTemplateRenderer


class TestRenderRomsSettings:
    """Tests for render_roms_settings function."""
    
    def test_render_basic_template(self, tmp_path):
        """Test rendering a basic template."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_template.j2").write_text("Hello {{ name }}, value is {{ value }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "test_template": {
                "name": "World",
                "value": 42
            }
        }
        
        result = render_roms_settings(
            template_files=["test_template.j2"],
            template_dir=template_dir,
            settings_dict=settings_dict,
            code_output_dir=output_dir,
        )
        
        assert result["location"] == str(output_dir.resolve())
        assert "test_template" in result["filter"]["files"]
        assert (output_dir / "test_template").exists()
        assert (output_dir / "test_template").read_text() == "Hello World, value is 42"
    
    def test_render_multiple_templates(self, tmp_path):
        """Test rendering multiple templates."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "template1.j2").write_text("Content 1: {{ key1 }}")
        (template_dir / "template2.j2").write_text("Content 2: {{ key2 }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "template1": {"key1": "value1"},
            "template2": {"key2": "value2"}
        }
        
        result = render_roms_settings(
            template_files=["template1.j2", "template2.j2"],
            template_dir=template_dir,
            settings_dict=settings_dict,
            code_output_dir=output_dir,
        )
        
        assert len(result["filter"]["files"]) == 2
        assert "template1" in result["filter"]["files"]
        assert "template2" in result["filter"]["files"]
        assert (output_dir / "template1").read_text() == "Content 1: value1"
        assert (output_dir / "template2").read_text() == "Content 2: value2"
    
    def test_render_with_n_tracers(self, tmp_path):
        """Test rendering with n_tracers parameter."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_template.j2").write_text("Number of tracers: {{ nt }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "test_template": {}
        }
        
        result = render_roms_settings(
            template_files=["test_template.j2"],
            template_dir=template_dir,
            settings_dict=settings_dict,
            code_output_dir=output_dir,
            n_tracers=34,
        )
        
        assert (output_dir / "test_template").read_text() == "Number of tracers: 34"
    
    def test_copy_non_template_file(self, tmp_path):
        """Test copying non-template files."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "Makefile").write_text("compile:\n\techo 'compiling'")
        (template_dir / "template1.j2").write_text("{{ key1 }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "template1": {"key1": "value1"}
        }
        
        result = render_roms_settings(
            template_files=["Makefile", "template1.j2"],
            template_dir=template_dir,
            settings_dict=settings_dict,
            code_output_dir=output_dir,
        )
        
        assert "Makefile" in result["filter"]["files"]
        assert "template1" in result["filter"]["files"]
        assert (output_dir / "Makefile").exists()
        assert (output_dir / "Makefile").read_text() == "compile:\n\techo 'compiling'"
    
    def test_render_with_full_match_key(self, tmp_path):
        """Test rendering with full match key (e.g., roms.in.j2 -> roms.in)."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "roms.in.j2").write_text("Title: {{ title.casename }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "roms.in": {
                "title": {
                    "casename": "test_case"
                }
            }
        }
        
        result = render_roms_settings(
            template_files=["roms.in.j2"],
            template_dir=template_dir,
            settings_dict=settings_dict,
            code_output_dir=output_dir,
        )
        
        assert (output_dir / "roms.in").read_text() == "Title: test_case"
    
    def test_render_with_partial_match_key(self, tmp_path):
        """Test rendering with partial match key (e.g., bgc.opt.j2 -> bgc)."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "bgc.opt.j2").write_text("Output: {{ bgc.wrt_his }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "bgc": {
                "wrt_his": True
            }
        }
        
        result = render_roms_settings(
            template_files=["bgc.opt.j2"],
            template_dir=template_dir,
            settings_dict=settings_dict,
            code_output_dir=output_dir,
        )
        
        assert (output_dir / "bgc.opt").read_text() == "Output: True"
    
    def test_missing_template_directory(self, tmp_path):
        """Test that missing template directory raises FileNotFoundError."""
        template_dir = tmp_path / "nonexistent_templates"
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        with pytest.raises(FileNotFoundError, match="Template directory does not exist"):
            render_roms_settings(
                template_files=["test.j2"],
                template_dir=template_dir,
                settings_dict={"test": {}},
                code_output_dir=output_dir,
            )
    
    def test_template_directory_not_directory(self, tmp_path):
        """Test that template path that is not a directory raises ValueError."""
        template_file = tmp_path / "not_a_dir"
        template_file.write_text("not a directory")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        with pytest.raises(ValueError, match="is not a directory"):
            render_roms_settings(
                template_files=["test.j2"],
                template_dir=template_file,
                settings_dict={"test": {}},
                code_output_dir=output_dir,
            )
    
    def test_missing_output_directory(self, tmp_path):
        """Test that missing output directory raises FileNotFoundError."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test.j2").write_text("test")
        
        output_dir = tmp_path / "nonexistent_output"
        
        with pytest.raises(FileNotFoundError, match="Output directory does not exist"):
            render_roms_settings(
                template_files=["test.j2"],
                template_dir=template_dir,
                settings_dict={"test": {}},
                code_output_dir=output_dir,
            )
    
    def test_missing_template_file(self, tmp_path):
        """Test that missing template file raises FileNotFoundError."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        with pytest.raises(FileNotFoundError, match="Template file not found"):
            render_roms_settings(
                template_files=["nonexistent.j2"],
                template_dir=template_dir,
                settings_dict={"nonexistent": {}},
                code_output_dir=output_dir,
            )
    
    def test_template_without_settings_entry(self, tmp_path):
        """Test that template without settings_dict entry raises ValueError."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test.j2").write_text("test")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        with pytest.raises(ValueError, match="without corresponding settings_dict entries"):
            render_roms_settings(
                template_files=["test.j2"],
                template_dir=template_dir,
                settings_dict={},
                code_output_dir=output_dir,
            )
    
    def test_settings_entry_without_template(self, tmp_path):
        """Test that settings_dict entry without template raises ValueError."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        with pytest.raises(ValueError, match="without corresponding template files"):
            render_roms_settings(
                template_files=[],
                template_dir=template_dir,
                settings_dict={"test": {}},
                code_output_dir=output_dir,
            )
    
    def test_template_variables_mismatch(self, tmp_path):
        """Test that template variables not in settings_dict raise ValueError."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test.j2").write_text("Value: {{ missing_var }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "test": {
                "existing_var": "value"
            }
        }
        
        with pytest.raises(ValueError, match="references variables without corresponding settings_dict entries"):
            render_roms_settings(
                template_files=["test.j2"],
                template_dir=template_dir,
                settings_dict=settings_dict,
                code_output_dir=output_dir,
            )
    
    def test_settings_dict_not_dict(self, tmp_path):
        """Test that settings_dict entry that is not a dict raises ValueError."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test.j2").write_text("Value: {{ var }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "test": "not a dict"
        }
        
        with pytest.raises(ValueError, match="must be a dictionary"):
            render_roms_settings(
                template_files=["test.j2"],
                template_dir=template_dir,
                settings_dict=settings_dict,
                code_output_dir=output_dir,
            )
    
    def test_template_parsing_error(self, tmp_path):
        """Test that invalid template syntax raises ValueError."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test.j2").write_text("{{ unclosed")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "test": {"var": "value"}
        }
        
        with pytest.raises(ValueError, match="Failed to parse template"):
            render_roms_settings(
                template_files=["test.j2"],
                template_dir=template_dir,
                settings_dict=settings_dict,
                code_output_dir=output_dir,
            )
    
    def test_nt_variable_excluded(self, tmp_path):
        """Test that 'nt' variable is excluded from template validation."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test.j2").write_text("Tracers: {{ nt }}, Value: {{ value }}")
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        settings_dict = {
            "test": {
                "value": 42
            }
        }
        
        # Should not raise error even though 'nt' is not in settings_dict
        result = render_roms_settings(
            template_files=["test.j2"],
            template_dir=template_dir,
            settings_dict=settings_dict,
            code_output_dir=output_dir,
            n_tracers=34,
        )
        
        assert (output_dir / "test").read_text() == "Tracers: 34, Value: 42"


class TestROMSTemplateRenderer:
    """Tests for ROMSTemplateRenderer class."""
    
    def test_renderer_initialization(self, tmp_path):
        """Test ROMSTemplateRenderer initialization."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        
        renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
        
        assert renderer.template_dir == template_dir
        assert renderer.env is not None
    
    def test_render_template(self, tmp_path):
        """Test rendering a single template."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_template.j2").write_text("Hello {{ name }}, value is {{ value }}")
        
        renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
        config = {"name": "World", "value": 42}
        
        result = renderer.render_template("test_template.j2", config)
        
        assert result == "Hello World, value is 42"
    
    def test_render_template_without_j2_extension(self, tmp_path):
        """Test rendering template without .j2 extension in name."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_template.j2").write_text("Hello {{ name }}")
        
        renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
        config = {"name": "World"}
        
        # Should work with .j2 extension
        result = renderer.render_template("test_template.j2", config)
        assert result == "Hello World"
    
    def test_fortran_bool_filter(self, tmp_path):
        """Test Fortran boolean filter."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_template.j2").write_text("Flag: {{ flag | lower }}")
        
        renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
        
        config = {"flag": True}
        result = renderer.render_template("test_template.j2", config)
        assert result == "Flag: .true."
        
        config = {"flag": False}
        result = renderer.render_template("test_template.j2", config)
        assert result == "Flag: .false."
    
    def test_render_template_with_nested_context(self, tmp_path):
        """Test rendering template with nested context."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_template.j2").write_text("Title: {{ title.name }}, Value: {{ nested.value }}")
        
        renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
        config = {
            "title": {"name": "Test Case"},
            "nested": {"value": 42}
        }
        
        result = renderer.render_template("test_template.j2", config)
        
        assert result == "Title: Test Case, Value: 42"
    
    def test_render_template_missing_variable(self, tmp_path):
        """Test that missing template variable renders as empty string (default Jinja2 behavior)."""
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        (template_dir / "test_template.j2").write_text("Value: {{ missing_var }}")
        
        renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
        config = {}
        
        # Jinja2 by default renders undefined variables as empty strings, not errors
        result = renderer.render_template("test_template.j2", config)
        assert result == "Value: "
    
    def test_render_template_missing_template_file(self, tmp_path):
        """Test that missing template file raises TemplateNotFound."""
        from jinja2.exceptions import TemplateNotFound
        
        template_dir = tmp_path / "templates"
        template_dir.mkdir()
        
        renderer = ROMSTemplateRenderer(template_dir=str(template_dir))
        config = {}
        
        with pytest.raises(TemplateNotFound):
            renderer.render_template("nonexistent.j2", config)
