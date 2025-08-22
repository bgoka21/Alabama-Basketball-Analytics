import click
from app.services.draft_stock_importer import import_workbook


@click.command("import_draft_stock")
@click.argument("xlsx_path", type=click.Path(exists=True))
@click.option("--strict/--no-strict", default=True, show_default=True)
@click.option("--commit-batch", default=500, show_default=True)
def import_draft_stock(xlsx_path, strict, commit_batch):
    """Import the Excel workbook into prospects (upsert)."""
    summary = import_workbook(xlsx_path, strict=strict, commit_batch=commit_batch)
    click.echo(f"Done. Sheets: {summary['sheets']} | rows: {summary['rows']} | upserts: {summary['upserts']}")
