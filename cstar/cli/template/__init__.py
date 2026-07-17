import typer

from cstar.base.feature import is_feature_enabled

app = typer.Typer(
    name="template",
    help="Generate templates as a starting point for your blueprints and workplans.",
)

if is_feature_enabled("CLI_TEMPLATE_CREATE"):
    from cstar.cli.template.create import app as app_create

    app.add_typer(app_create)
