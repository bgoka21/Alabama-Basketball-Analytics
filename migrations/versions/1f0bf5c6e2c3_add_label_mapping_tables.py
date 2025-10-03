"""Add normalized label mapping tables for player stats and possessions"""

from alembic import op
import sqlalchemy as sa
import json
import re


# revision identifiers, used by Alembic.
revision = '1f0bf5c6e2c3'
down_revision = 'ff5ca5bde710'
branch_labels = None
depends_on = None


def _normalize_values(values):
    normalized = set()
    for raw in values:
        if not isinstance(raw, str):
            continue
        label = raw.strip().upper()
        if label:
            normalized.add(label)
    return normalized


def _coerce_iter(value):
    if value is None:
        return ()
    if isinstance(value, str):
        return re.split(r",", value)
    if isinstance(value, dict):
        return value.values()
    try:
        return tuple(value)
    except TypeError:
        return ()


def _collect_labels_from_blob(blob):
    labels = set()
    if not blob:
        return labels
    data = blob
    if isinstance(blob, str):
        try:
            data = json.loads(blob)
        except Exception:
            return labels
    if isinstance(data, dict):
        entries = (data,)
    elif isinstance(data, list):
        entries = data
    else:
        entries = ()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        labels |= _normalize_values(_coerce_iter(entry.get('drill_labels')))
        labels |= _normalize_values(_coerce_iter(entry.get('possession_type')))
        labels |= _normalize_values(_coerce_iter(entry.get('team')))
    return labels


def _collect_possession_labels(raw):
    return _normalize_values(_coerce_iter(raw))


def upgrade():
    bind = op.get_bind()

    if not bind.dialect.has_table(bind, 'player_stat_labels'):
        op.create_table(
            'player_stat_labels',
            sa.Column('player_stats_id', sa.Integer(), sa.ForeignKey('player_stats.id', ondelete='CASCADE'), primary_key=True),
            sa.Column('label', sa.String(length=64), primary_key=True),
        )
        op.create_index('ix_player_stat_labels_label', 'player_stat_labels', ['label'])

    if not bind.dialect.has_table(bind, 'possession_labels'):
        op.create_table(
            'possession_labels',
            sa.Column('possession_id', sa.Integer(), sa.ForeignKey('possession.id', ondelete='CASCADE'), primary_key=True),
            sa.Column('label', sa.String(length=64), primary_key=True),
        )
        op.create_index('ix_possession_labels_label', 'possession_labels', ['label'])

    player_stats = bind.execute(sa.text(
        "SELECT id, shot_type_details, stat_details FROM player_stats"
    )).mappings()
    insert_player_label = sa.text(
        "INSERT INTO player_stat_labels (player_stats_id, label) VALUES (:player_stats_id, :label)"
    )
    for row in player_stats:
        labels = set()
        labels |= _collect_labels_from_blob(row['shot_type_details'])
        labels |= _collect_labels_from_blob(row['stat_details'])
        for label in sorted(labels):
            bind.execute(insert_player_label, {
                'player_stats_id': row['id'],
                'label': label,
            })

    possessions = bind.execute(sa.text(
        "SELECT id, drill_labels FROM possession"
    )).mappings()
    insert_possession_label = sa.text(
        "INSERT INTO possession_labels (possession_id, label) VALUES (:possession_id, :label)"
    )
    for row in possessions:
        labels = _collect_possession_labels(row['drill_labels'])
        for label in sorted(labels):
            bind.execute(insert_possession_label, {
                'possession_id': row['id'],
                'label': label,
            })


def downgrade():
    op.drop_index('ix_possession_labels_label', table_name='possession_labels')
    op.drop_table('possession_labels')
    op.drop_index('ix_player_stat_labels_label', table_name='player_stat_labels')
    op.drop_table('player_stat_labels')
