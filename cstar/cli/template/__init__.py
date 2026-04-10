import typer

from cstar.base.feature import is_feature_enabled
from cstar.cli.template.create import app as app_create

app = typer.Typer(
    name="template",
    help="Generate templates as a starting point for your blueprints and workplans.",
)

if is_feature_enabled("CLI_TEMPLATE_CREATE"):
    app.add_typer(app_create)
