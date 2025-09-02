import click
from flask.cli import with_appcontext
from app import db
from app.models.prospect import Prospect
from app.utils.coach_names import normalize_coach_name


@click.command("backfill_coach_names")
@with_appcontext
def backfill_coach_names() -> None:
    """Rewrite Prospect.coach values to their canonical display names."""
    if not click.confirm("Rewrite all Prospect.coach values to canonical names?", default=False):
        click.echo("Aborted")
        return
    total = 0
    updated = 0
    batch = 0
    for p in Prospect.query.yield_per(500):
        total += 1
        _, disp = normalize_coach_name(p.coach)
        if p.coach != disp:
            p.coach = disp
            updated += 1
        batch += 1
        if batch >= 500:
            db.session.commit()
            batch = 0
    if batch:
        db.session.commit()
    click.echo(f"Processed {total} rows; updated {updated}.")
