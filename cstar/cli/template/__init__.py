import typer

from cstar.base.feature import is_feature_enabled
from cstar.cli.template.create import app as app_create

app = typer.Typer()

if is_feature_enabled("CLI_TEMPLATE_CREATE"):
    app.add_typer(app_create)
