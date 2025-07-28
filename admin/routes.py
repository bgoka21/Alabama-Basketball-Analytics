import os, json
from collections import defaultdict
from datetime import datetime, date
import datetime as datetime_module
import io
import re
import traceback
import zipfile
import pandas as pd  # Added pandas import for CSV parsing and NaN handling
from types import SimpleNamespace

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, send_file, current_app, session, make_response, abort
)
from flask_login import login_required, current_user, confirm_login, login_user, logout_user
from utils.auth       import admin_required
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from models.database import (
    db,
    Game,
    TeamStats,
    PlayerStats,
    BlueCollarStats,
    OpponentBlueCollarStats,
    Possession,
    PlayerPossession,
    ShotDetail,
    Session,
    Season,
    Roster,
    Practice,
    SkillEntry,
    PnRStats,
    PlayerDevelopmentPlan
)
from models.database import PageView
from models.uploaded_file import UploadedFile
from models.recruit import Recruit, RecruitShotTypeStat
from models.user import User
from sqlalchemy import func, and_, or_, case
from sqlalchemy.orm import aliased
from utils.db_helpers import array_agg_or_group_concat
from utils.skill_config import shot_map, label_map
from test_parse import get_possession_breakdown_detailed
from test_parse import parse_csv           # your existing game parser
from parse_practice_csv import parse_practice_csv, blue_collar_values  # <— make sure this is here
from parse_recruits_csv import parse_recruits_csv
from stats_config import LEADERBOARD_STATS
from utils.session_helpers import get_player_stats_for_date_range
from utils.leaderboard_helpers import get_player_overall_stats, get_on_court_metrics

# --- Helper Functions at the top ---

def safe_str(value):
    """Safely convert a value to a string, returning an empty string if the value is None."""
    return "" if value is None else str(value)

def extract_tokens(text):
    """
    Extract tokens from the given text string.
    If the input is not a string or is NaN, it safely converts the value to a string or returns an empty list.
    """
    if pd.isna(text):
        return []
    if not isinstance(text, str):
        text = str(text)
    tokens = text.replace(',', ' ').split()
    return tokens

def compute_leaderboard(stat_key, season_id, start_dt=None, end_dt=None, label_set=None):
    """Return (config, rows) for the leaderboard.

    Optional ``start_dt`` and ``end_dt`` parameters limit the stats to a
    specific date range (inclusive). Dates are matched against the associated
    ``Practice.date`` or ``Game.game_date`` fields.
    """
    cfg = next((c for c in LEADERBOARD_STATS if c['key'] == stat_key), None)
    if not cfg:
        abort(404)

    ps_fields = [
        'points','assists','pot_assists','second_assists','turnovers',
        'fta','ftm','atr_attempts','atr_makes',
        'fg2_attempts','fg2_makes','fg3_attempts','fg3_makes',
        'foul_by','contest_front','contest_side','contest_behind',
        'contest_late','contest_early','contest_no',
        'bump_positive','bump_missed',
        'blowby_total','blowby_triple_threat','blowby_closeout','blowby_isolation',
        'practice_wins','practice_losses','sprint_wins','sprint_losses'
    ]
    ps_q = (
        db.session.query(
            PlayerStats.player_name.label('player'),
            *[func.coalesce(func.sum(getattr(PlayerStats, k)), 0).label(k) for k in ps_fields]
        )
        .filter(PlayerStats.season_id == season_id)
    )
    if label_set:
        clauses = []
        for lbl in label_set:
            pattern = f"%{lbl}%"
            clauses.append(PlayerStats.shot_type_details.ilike(pattern))
            clauses.append(PlayerStats.stat_details.ilike(pattern))
        ps_q = ps_q.filter(or_(*clauses))
    if start_dt or end_dt:
        ps_q = (
            ps_q
            .outerjoin(Game, PlayerStats.game_id == Game.id)
            .outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        )
        if start_dt:
            ps_q = ps_q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_dt),
                    and_(PlayerStats.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            ps_q = ps_q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_dt),
                    and_(PlayerStats.practice_id != None, Practice.date <= end_dt),
                )
            )
    ps_q = ps_q.group_by(PlayerStats.player_name)
    ps_rows = {r.player: r._asdict() for r in ps_q.all()}

    bc_fields = [
        'total_blue_collar','reb_tip','def_reb','misc',
        'deflection','steal','block','off_reb','floor_dive','charge_taken'
    ]
    bc_q = (
        db.session.query(
            Roster.player_name.label('player'),
            *[func.coalesce(func.sum(getattr(BlueCollarStats, k)), 0).label(k) for k in bc_fields]
        )
        .join(Roster, BlueCollarStats.player_id == Roster.id)
        .filter(BlueCollarStats.season_id == season_id)
    )
    if label_set:
        bc_q = bc_q.join(
            PlayerStats,
            and_(
                PlayerStats.season_id == season_id,
                PlayerStats.player_name == Roster.player_name,
                PlayerStats.practice_id == BlueCollarStats.practice_id,
                PlayerStats.game_id == BlueCollarStats.game_id,
            ),
        )
        bc_clauses = []
        for lbl in label_set:
            pattern = f"%{lbl}%"
            bc_clauses.append(PlayerStats.shot_type_details.ilike(pattern))
            bc_clauses.append(PlayerStats.stat_details.ilike(pattern))
        bc_q = bc_q.filter(or_(*bc_clauses))
    if start_dt or end_dt:
        bc_q = (
            bc_q
            .outerjoin(Game, BlueCollarStats.game_id == Game.id)
            .outerjoin(Practice, BlueCollarStats.practice_id == Practice.id)
        )
        if start_dt:
            bc_q = bc_q.filter(
                or_(
                    and_(BlueCollarStats.game_id != None, Game.game_date >= start_dt),
                    and_(BlueCollarStats.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            bc_q = bc_q.filter(
                or_(
                    and_(BlueCollarStats.game_id != None, Game.game_date <= end_dt),
                    and_(BlueCollarStats.practice_id != None, Practice.date <= end_dt),
                )
            )
    bc_q = bc_q.group_by(Roster.player_name)
    bc_rows = {r.player: r._asdict() for r in bc_q.all()}

    # ─── On-court offensive metrics ──────────────────────────────────────
    poss_q = (
        db.session.query(
            Roster.player_name.label('player'),
            func.count(PlayerPossession.id).label('on_poss'),
            func.coalesce(func.sum(Possession.points_scored), 0).label('on_pts')
        )
        .join(PlayerPossession, Roster.id == PlayerPossession.player_id)
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(
            Roster.season_id == season_id,
            Possession.season_id == season_id,
            Possession.possession_side == 'Offense'
        )
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        poss_q = poss_q.filter(or_(*clauses))
    if start_dt or end_dt:
        poss_q = (
            poss_q
            .outerjoin(Game, Possession.game_id == Game.id)
            .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_dt:
            poss_q = poss_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_dt),
                    and_(Possession.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            poss_q = poss_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_dt),
                    and_(Possession.practice_id != None, Practice.date <= end_dt),
                )
            )
    poss_q = poss_q.group_by(Roster.player_name)
    poss_rows = {r.player: {'offensive_possessions': r.on_poss, 'on_pts': r.on_pts} for r in poss_q.all()}

    team_q = (
        db.session.query(
            func.count(Possession.id),
            func.coalesce(func.sum(Possession.points_scored), 0)
        )
        .filter(
            Possession.season_id == season_id,
            Possession.possession_side == 'Offense'
        )
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        team_q = team_q.filter(or_(*clauses))
    if start_dt or end_dt:
        team_q = (
            team_q
            .outerjoin(Game, Possession.game_id == Game.id)
            .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_dt:
            team_q = team_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_dt),
                    and_(Possession.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            team_q = team_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_dt),
                    and_(Possession.practice_id != None, Practice.date <= end_dt),
                )
            )
    TEAM_poss, TEAM_pts = team_q.one()

    # gather practice/game ids for the same filters (used for personal stats)
    id_q = (
        db.session.query(Possession.practice_id, Possession.game_id)
        .filter(
            Possession.season_id == season_id,
            Possession.possession_side == 'Offense'
        )
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        id_q = id_q.filter(or_(*clauses))
    if start_dt or end_dt:
        id_q = (
            id_q
            .outerjoin(Game, Possession.game_id == Game.id)
            .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_dt:
            id_q = id_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_dt),
                    and_(Possession.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            id_q = id_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_dt),
                    and_(Possession.practice_id != None, Practice.date <= end_dt),
                )
            )
    id_rows = id_q.distinct().all()
    practice_ids = [pid for pid, gid in id_rows if pid]
    game_ids = [gid for pid, gid in id_rows if gid]

    # personal turnover counts for the same practices/games
    personal_to_q = (
        db.session.query(
            PlayerStats.player_name.label('player'),
            func.coalesce(func.sum(PlayerStats.turnovers), 0).label('personal_turnovers')
        )
        .filter(PlayerStats.season_id == season_id)
    )
    if practice_ids:
        personal_to_q = personal_to_q.filter(PlayerStats.practice_id.in_(practice_ids))
    if game_ids:
        personal_to_q = personal_to_q.filter(PlayerStats.game_id.in_(game_ids))
    personal_to_q = personal_to_q.group_by(PlayerStats.player_name).all()
    person_turnovers = {name: val for name, val in personal_to_q}

    # personal offensive rebounds counts
    personal_offreb_q = (
        db.session.query(
            BlueCollarStats.player_id.label('player_id'),
            func.coalesce(func.sum(BlueCollarStats.off_reb), 0).label('personal_off_rebs')
        )
        .filter(BlueCollarStats.season_id == season_id)
    )
    if practice_ids:
        personal_offreb_q = personal_offreb_q.filter(BlueCollarStats.practice_id.in_(practice_ids))
    if game_ids:
        personal_offreb_q = personal_offreb_q.filter(BlueCollarStats.game_id.in_(game_ids))
    personal_offreb_q = personal_offreb_q.group_by(BlueCollarStats.player_id).all()
    person_off_rebs = {
        db.session.get(Roster, pid).player_name: count
        for pid, count in personal_offreb_q
    }

    personal_fouls_q = (
        db.session.query(
            PlayerStats.player_name.label('player'),
            func.coalesce(func.sum(PlayerStats.foul_by), 0).label('personal_fouls')
        )
        .filter(
            PlayerStats.season_id == season_id
        )
        .group_by(PlayerStats.player_name)
        .all()
    )
    personal_fouls = {r.player: r.personal_fouls for r in personal_fouls_q}

    events_q = (
        db.session.query(
            Roster.player_name.label('player'),
            func.sum(case((ShotDetail.event_type.in_(['ATR+','2FG+']), 1), else_=0)).label('fgm2'),
            func.sum(case((ShotDetail.event_type=='3FG+', 1), else_=0)).label('fgm3'),
            func.sum(case((ShotDetail.event_type.in_(['ATR+','ATR-','2FG+','2FG-','3FG+','3FG-']), 1), else_=0)).label('fga'),
            func.sum(case((ShotDetail.event_type=='ATR+', 1), else_=0)).label('atr_makes'),
            func.sum(case((ShotDetail.event_type.in_(['ATR+','ATR-']), 1), else_=0)).label('atr_attempts'),
            func.sum(case((ShotDetail.event_type=='2FG+', 1), else_=0)).label('fg2_makes'),
            func.sum(case((ShotDetail.event_type.in_(['2FG+','2FG-']), 1), else_=0)).label('fg2_attempts'),
            func.sum(case((ShotDetail.event_type=='3FG+', 1), else_=0)).label('fg3_makes'),
            func.sum(case((ShotDetail.event_type.in_(['3FG+','3FG-']), 1), else_=0)).label('fg3_attempts'),
            func.sum(case((ShotDetail.event_type=='Turnover', 1), else_=0)).label('turnovers_on'),
            func.sum(
                case((ShotDetail.event_type=='Off Reb', 1), else_=0)
            ).label('off_reb_on'),
            func.sum(
                case((ShotDetail.event_type=='TEAM Off Reb', 1), else_=0)
            ).label('team_off_reb_on'),
            func.sum(case((ShotDetail.event_type=='Foul', 1), else_=0)).label('fouls_on'),
            func.sum(
                case(
                    (ShotDetail.event_type.in_(['ATR-','2FG-','3FG-']), 1),
                    else_=0
                )
            ).label('team_misses_on')
        )
        .join(PlayerPossession, Roster.id == PlayerPossession.player_id)
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .join(ShotDetail, ShotDetail.possession_id == Possession.id)
        .filter(
            Roster.season_id == season_id,
            Possession.season_id == season_id,
            Possession.possession_side == 'Offense'
        )
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        events_q = events_q.filter(or_(*clauses))
    if start_dt or end_dt:
        events_q = (
            events_q
            .outerjoin(Game, Possession.game_id == Game.id)
            .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_dt:
            events_q = events_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_dt),
                    and_(Possession.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            events_q = events_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_dt),
                    and_(Possession.practice_id != None, Practice.date <= end_dt),
                )
            )
    events_q = events_q.group_by(Roster.player_name)
    event_rows = {r.player: r._asdict() for r in events_q.all()}

    extra_rows = {}
    for player in set(poss_rows) | set(event_rows):
        poss = poss_rows.get(player, {})
        events = event_rows.get(player, {})
        on_poss = poss.get('offensive_possessions', 0)
        on_pts = poss.get('on_pts', 0)
        ppp_on = on_pts / on_poss if on_poss else 0
        off_poss = TEAM_poss - on_poss
        off_pts = TEAM_pts - on_pts
        ppp_off = off_pts / off_poss if off_poss else 0
        fgm2 = events.get('fgm2', 0)
        fgm3 = events.get('fgm3', 0)
        fga = events.get('fga', 0)
        efg = (fgm2 + 1.5 * fgm3) / fga if fga else 0
        fg2_pct = events.get('fg2_makes', 0) / events.get('fg2_attempts', 0) if events.get('fg2_attempts', 0) else 0
        fg3_pct = events.get('fg3_makes', 0) / events.get('fg3_attempts', 0) if events.get('fg3_attempts', 0) else 0
        turnover_rate = events.get('turnovers_on', 0) / on_poss if on_poss else 0
        individual_turnover_rate = person_turnovers.get(player, 0) / on_poss if on_poss else 0
        team_miss = events.get('team_misses_on', 0)
        individual_off_reb_rate = (
            person_off_rebs.get(player, 0) / team_miss
            if team_miss
            else 0
        )
        # only count the TEAM Off Reb events for team OREB% numerator
        recorded = events.get('team_off_reb_on', 0)
        team_rebs = recorded if recorded > 0 else (
            sum(
                p.off_reb
                for p in BlueCollarStats.query.filter_by(season_id=season_id)
            )
            * (on_poss / TEAM_poss)
        )
        off_reb_rate = team_rebs / team_miss if team_miss else 0
        fouls_rate = events.get('fouls_on', 0) / on_poss if on_poss else 0
        foul_rate_ind = personal_fouls.get(player, 0) / on_poss if on_poss else 0
        extra_rows[player] = {
            'offensive_possessions': on_poss,
            'ppp_on': round(ppp_on, 2),
            'ppp_off': round(ppp_off, 2),
            'efg_on': round(efg * 100, 1),
            'two_fg_pct': round(fg2_pct * 100, 1),
            'three_fg_pct': round(fg3_pct * 100, 1),
            'turnover_rate': round(turnover_rate * 100, 1),
            'off_reb_rate': round(off_reb_rate * 100, 1),
            'individual_turnover_rate': round(individual_turnover_rate * 100, 1),
            'individual_off_reb_rate': round(individual_off_reb_rate * 100, 1),
            'fouls_drawn_rate': round(fouls_rate * 100, 1),
            'individual_foul_rate': round(foul_rate_ind * 100, 1),
        }

    core_rows = {}
    for player in set(ps_rows) | set(bc_rows) | set(extra_rows):
        base = {**ps_rows.get(player, {}), **bc_rows.get(player, {}), **extra_rows.get(player, {})}
        # derive additional shooting percentages
        atr_a = base.get('atr_attempts', 0)
        atr_m = base.get('atr_makes', 0)
        fg3_a = base.get('fg3_attempts', 0)
        fg3_m = base.get('fg3_makes', 0)
        base['atr_pct'] = (atr_m / atr_a * 100) if atr_a else 0
        base['fg3_pct'] = (fg3_m / fg3_a * 100) if fg3_a else 0

        # assist/turnover ratios
        to = base.get('turnovers', 0)
        ast = base.get('assists', 0)
        sec_ast = base.get('second_assists', 0)
        pot_ast = base.get('pot_assists', 0)
        if to:
            base['assist_turnover_ratio'] = round(ast / to, 2)
            base['adj_assist_turnover_ratio'] = round((ast + sec_ast + pot_ast) / to, 2)
        else:
            base['assist_turnover_ratio'] = 0.0
            base['adj_assist_turnover_ratio'] = 0.0

        core_rows[player] = base

    shot_rows = (
        Roster.query
        .join(PlayerStats,
              and_(PlayerStats.player_name == Roster.player_name,
                   PlayerStats.season_id == Roster.season_id))
        .filter(PlayerStats.season_id == season_id)
    )
    if label_set:
        s_clauses = []
        for lbl in label_set:
            pattern = f"%{lbl}%"
            s_clauses.append(PlayerStats.shot_type_details.ilike(pattern))
            s_clauses.append(PlayerStats.stat_details.ilike(pattern))
        shot_rows = shot_rows.filter(or_(*s_clauses))
    if start_dt or end_dt:
        shot_rows = (
            shot_rows
            .outerjoin(Game, PlayerStats.game_id == Game.id)
            .outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        )
        if start_dt:
            shot_rows = shot_rows.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_dt),
                    and_(PlayerStats.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            shot_rows = shot_rows.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_dt),
                    and_(PlayerStats.practice_id != None, Practice.date <= end_dt),
                )
            )
    shot_rows = (
        shot_rows
        .with_entities(
            Roster.player_name,
            array_agg_or_group_concat(PlayerStats.shot_type_details)
        )
        .group_by(Roster.player_name)
        .all()
    )

    new_shot_rows = []
    for player, blobs in shot_rows:
        if isinstance(blobs, str):
            parts = blobs.split('|||')
        elif isinstance(blobs, (list, tuple)):
            parts = blobs
        else:
            parts = []

        json_list = []
        for fragment in parts:
            if not fragment:
                continue
            try:
                parsed = json.loads(fragment)
            except ValueError:
                continue
            if isinstance(parsed, list):
                json_list.extend(parsed)
            else:
                json_list.append(parsed)

        new_shot_rows.append((player, json_list))

    shot_details = {}
    for player, shot_list in new_shot_rows:
        detail_counts = defaultdict(lambda: {'attempts': 0, 'makes': 0})
        for shot in shot_list:
            raw_sc = shot.get('shot_class', '').lower()
            sc = {'2fg': 'fg2', '3fg': 'fg3'}.get(raw_sc, raw_sc)
            label = 'Assisted' if shot.get('Assisted') else 'Non-Assisted'
            raw_ctx = shot.get('possession_type', '').strip().lower()
            if 'trans' in raw_ctx:
                ctx = 'transition'
            elif 'half' in raw_ctx:
                ctx = 'halfcourt'
            else:
                ctx = 'total'
            if sc not in ['atr', 'fg2', 'fg3']:
                continue
            bucket = detail_counts[(sc, label, ctx)]
            bucket['attempts'] += 1
            bucket['makes'] += (shot.get('result') == 'made')
        flat = {}
        totals_by_sc = defaultdict(lambda: {'attempts': 0, 'makes': 0})
        for (sc, label, ctx), data in detail_counts.items():
            a = data['attempts']
            m = data['makes']
            pts = 2 if sc in ('atr','fg2') else 3
            flat[f"{sc}_{label}_{ctx}_attempts"] = a
            flat[f"{sc}_{label}_{ctx}_makes"] = m
            flat[f"{sc}_{label}_{ctx}_fg_pct"] = (m / a * 100 if a else 0)
            flat[f"{sc}_{label}_{ctx}_pps"] = (pts * m / a if a else 0)
            total = sum(d['attempts'] for k, d in detail_counts.items() if k[0] == sc) or 1
            flat[f"{sc}_{label}_{ctx}_freq_pct"] = (a / total * 100)
            totals_by_sc[sc]['attempts'] += a
            totals_by_sc[sc]['makes'] += m

        total_attempts = sum(t['attempts'] for t in totals_by_sc.values()) or 0
        for sc, t in totals_by_sc.items():
            a = t['attempts']
            m = t['makes']
            pts = 2 if sc in ('atr','fg2') else 3
            flat[f"{sc}_attempts"] = a
            flat[f"{sc}_makes"] = m
            flat[f"{sc}_fg_pct"] = (m / a * 100 if a else 0)
            flat[f"{sc}_pps"] = (pts * m / a if a else 0)
            flat[f"{sc}_freq_pct"] = (a / total_attempts * 100) if total_attempts else 0

        shot_details[player] = flat

    all_players = set(core_rows) | set(shot_details)
    leaderboard = []
    if stat_key == 'assist_summary':
        for player in all_players:
            base = core_rows.get(player, {})
            leaderboard.append(
                (
                    player,
                    base.get('assists', 0),
                    base.get('pot_assists', 0),
                    base.get('second_assists', 0),
                    base.get('turnovers', 0),
                    base.get('assist_turnover_ratio', 0.0),
                    base.get('adj_assist_turnover_ratio', 0.0),
                )
            )
        leaderboard.sort(key=lambda x: x[1], reverse=True)
    elif stat_key == 'offense_summary':
        for player in all_players:
            base = core_rows.get(player, {})
            off_reb_rate = base.get('off_reb_rate', 0.0)
            leaderboard.append(
                (
                    player,
                    base.get('offensive_possessions', 0),
                    base.get('ppp_on', 0.0),
                    base.get('ppp_off', 0.0),
                    base.get('individual_turnover_rate', 0.0),
                    base.get('turnover_rate', 0.0),
                    base.get('individual_off_reb_rate', 0.0),
                    off_reb_rate,
                    base.get('individual_foul_rate', 0.0),
                    base.get('fouls_drawn_rate', 0.0),
                )
            )
        leaderboard.sort(key=lambda x: x[2], reverse=True)
    elif stat_key.endswith('_fg_pct'):
        for player in all_players:
            details = shot_details.get(player, {})
            pct = details.get(stat_key, 0)
            att_key = stat_key.replace('_fg_pct', '_attempts')
            make_key = stat_key.replace('_fg_pct', '_makes')
            freq_key = stat_key.replace('_fg_pct', '_freq_pct')
            attempts = details.get(att_key, 0)
            makes = details.get(make_key, 0)
            freq = details.get(freq_key, 0)
            leaderboard.append((player, makes, attempts, pct, freq))
        leaderboard.sort(key=lambda x: x[3], reverse=True)
    else:
        for player in all_players:
            val = core_rows.get(player, {}).get(stat_key) or shot_details.get(player, {}).get(stat_key, 0)
            leaderboard.append((player, val))
        leaderboard.sort(key=lambda x: x[1], reverse=True)

    return cfg, leaderboard

# Use the top-level templates folder so references like 'admin/base.html'
# resolve correctly when the blueprint is used in isolation (e.g. tests).
admin_bp = Blueprint('admin', __name__, template_folder='../templates')

@admin_bp.record
def register_filters(setup_state):
    """Ensure Jinja filters are available when blueprint is used standalone."""
    app = setup_state.app
    def grade_pps(pps, attempts):
        if not attempts:
            return ""

        def interpolate(start, end, factor):
            return tuple(round(s + (e - s) * max(0.0, min(factor, 1.0))) for s, e in zip(start, end))

        if pps >= 1.1:
            start, end = (200, 255, 200), (0, 128, 0)
            factor = min((pps - 1.1) / 0.5, 1.0)
        elif pps >= 1.0:
            start, end = (255, 255, 224), (255, 215, 0)
            factor = (pps - 1.0) / 0.1
        else:
            start, end = (255, 200, 200), (255, 0, 0)
            factor = min((1.0 - pps) / 0.5, 1.0)

        r, g, b = interpolate(start, end, factor)
        return f"background-color: rgb({r},{g},{b});"

    def grade_atr2fg_pct(pct, attempts):
        if not attempts:
            return ""
        pps = (pct / 100.0) * 2
        return grade_pps(pps, attempts)

    def grade_3fg_pct(pct, attempts):
        if not attempts:
            return ""
        pps = (pct / 100.0) * 3
        return grade_pps(pps, attempts)

    app.jinja_env.filters['grade_atr2fg_pct'] = grade_atr2fg_pct
    app.jinja_env.filters['grade_3fg_pct'] = grade_3fg_pct
    app.jinja_env.filters['grade_pps'] = grade_pps
    app.jinja_env.globals['view_exists'] = lambda name: name in app.view_functions

try:
    from auth.routes import auth_bp
    AUTH_EXISTS = True
except ImportError:
    AUTH_EXISTS = False

ALLOWED_EXTENSIONS = {'csv'}
IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def allowed_image(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in IMAGE_EXTENSIONS

@admin_bp.before_request
def admin_bp_before_request():
    # Always allow the login page & static assets
    if request.endpoint == 'admin.login' or request.endpoint.startswith('static'):
        return

    # First, everyone must be logged in to see any /admin pages
    if not current_user.is_authenticated:
        flash("Please log in to continue.", "error")
        return redirect(url_for('admin.login'))

    # Now lock down only the truly admin-only endpoints:
    admin_only = {
        'admin.dashboard',
        'admin.files_view_unique',
        'admin.upload_file',
        'admin.parse_file',
        'admin.delete_file',
        'admin.delete_data',
        'admin.users_list',
        'admin.add_user',
        'admin.edit_user',
        'admin.delete_user',
        'admin.edit_roster',
        'admin.delete_roster',
        'admin.roster',
        'admin.create_season',
        'admin.usage_report',
        'admin.user_usage_report',
        'admin.draft_upload',
        'draft_upload',
        'admin.upload_headshot',
        # … add any other admin-only endpoints here …
    }

    if request.endpoint in admin_only and not current_user.is_admin:
        flash("You do not have permission to view that page.", "error")
        return redirect(url_for('admin.login'))

    # Everything else under admin_bp (e.g. game_reports, game_stats, players_list, player_shot_type, etc.)
    # is now only gated by login_required (via this before_request), not by admin status.



@admin_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()

        if user and check_password_hash(user.password_hash, password):
            login_user(user, remember=True)
            flash("Login successful!", "success")
            # Send admins to the dashboard, everyone else to the cover page
            if user.is_admin:
                return redirect(url_for('admin.dashboard'))
            elif user.is_player:
                if user.player_name:
                    return redirect(url_for('admin.player_detail', player_name=user.player_name))
                return redirect(url_for('public.homepage'))
            else:
                return redirect(url_for('public.root'))

        flash("Invalid credentials. Please try again.", "error")
        return redirect(url_for('admin.login'))

    return render_template('admin/login.html')

@admin_bp.route('/dashboard', methods=['GET'])
@admin_required
def dashboard():
    # 1a) get season_id from query, or default to most recent
    sid = request.args.get('season_id', type=int)
    if not sid:
        latest = Season.query.order_by(Season.start_date.desc()).first()
        sid = latest.id if latest else None

    # 1b) load only files for that season
    uploaded_files = (
        UploadedFile.query
        .filter_by(season_id=sid)
        .order_by(UploadedFile.upload_date.desc())
        .all()
    )

    recruits = Recruit.query.order_by(Recruit.name).all()

    # 1c) fetch seasons for dropdown
    all_seasons = Season.query.order_by(Season.start_date.desc()).all()

    return render_template(
        'admin/dashboard.html',
        uploaded_files  = uploaded_files,
        all_seasons     = all_seasons,
        selected_season = sid,
        recruits        = recruits,
        active_page     = 'dashboard'
    )


@admin_bp.route('/users', methods=['GET'])
@admin_required
def users_list():
    """Show all users for admin to manage."""
    users = User.query.order_by(User.username).all()
    return render_template('admin/users.html', users=users, active_page='users')

@admin_bp.route('/users/add', methods=['GET', 'POST'])
@admin_required
def add_user():
    """Admin: create a new user account."""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        is_admin = bool(request.form.get('is_admin'))
        is_player = bool(request.form.get('is_player'))
        player_name = request.form.get('player_name', '').strip() or None

        # Basic validation
        if not username or not password:
            flash('Username and password are required.', 'error')
        elif User.query.filter_by(username=username).first():
            flash('That username is already taken.', 'error')
        else:
            # Create & save
            hashed = generate_password_hash(password)
            new = User(
                username=username,
                password_hash=hashed,
                is_admin=is_admin,
                is_player=is_player,
                player_name=player_name,
            )
            db.session.add(new)
            db.session.commit()
            flash(f'User "{username}" created.', 'success')
            return redirect(url_for('admin.users_list'))

    # GET or failed POST: render form
    return render_template('admin/add_user.html', active_page='users')

@admin_bp.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
@admin_required
def edit_user(user_id):
    """Admin: edit an existing user."""
    user = User.query.get_or_404(user_id)

    if request.method == 'POST':
        new_username = request.form.get('username', '').strip()
        new_password = request.form.get('password', '')
        is_admin = bool(request.form.get('is_admin'))
        is_player = bool(request.form.get('is_player'))
        player_name = request.form.get('player_name', '').strip() or None

        if not new_username:
            flash('Username cannot be blank.', 'error')
        elif new_username != user.username and User.query.filter_by(username=new_username).first():
            flash('That username is already taken.', 'error')
        else:
            user.username = new_username
            user.is_admin = is_admin
            user.is_player = is_player
            user.player_name = player_name
            if new_password:
                user.password_hash = generate_password_hash(new_password)
            db.session.commit()
            flash(f'User "{user.username}" updated.', 'success')
            return redirect(url_for('admin.users_list'))

    return render_template('admin/edit_user.html', user=user, active_page='users')

@admin_bp.route('/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    """Admin: delete a user."""
    user = User.query.get_or_404(user_id)

    if user.id == current_user.id:
        flash("You can't delete yourself!", 'error')
    else:
        db.session.delete(user)
        db.session.commit()
        flash(f'User "{user.username}" deleted.', 'success')

    return redirect(url_for('admin.users_list'))

@admin_bp.route('/logout')
@login_required
def logout():
    """Log the current user out."""
    logout_user()
    flash("Logged out successfully.", "success")
    return redirect(url_for('public.homepage'))


@admin_bp.route('/upload', methods=['POST'])
@admin_required
def upload_file():
    if 'file' not in request.files:
        flash('No file part in request', 'error')
        return redirect(url_for('admin.dashboard'))

    files = request.files.getlist('file')
    if not files or files[0].filename == '':
        flash('No selected files', 'error')
        return redirect(url_for('admin.dashboard'))

    category     = request.form.get('category')
    recruit_id   = request.form.get('recruit_id', type=int)
    season_id    = request.form.get('season_id', type=int)
    file_date_str = request.form.get('file_date')   # <-- new

    if category == 'Recruit' and not recruit_id:
        flash('Please select a recruit for this file.', 'error')
        return redirect(url_for('admin.dashboard', season_id=season_id))

    # parse the incoming YYYY-MM-DD string into a date object
    try:
         file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()
    except (TypeError, ValueError):
        flash('Please select a valid date for this file.', 'error')
        return redirect(url_for('admin.dashboard', season_id=season_id))

    for file in files:
        if file and allowed_file(file.filename):
            filename    = secure_filename(file.filename)
            upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
            file.save(upload_path)

            new_upload = UploadedFile(
                filename     = filename,
                parse_status = 'Not Parsed',
                category     = category,
                season_id    = season_id,
                file_date    = file_date,
                recruit_id   = recruit_id if category == 'Recruit' else None
            )
            db.session.add(new_upload)

    db.session.commit()
    flash("Files uploaded successfully!", "success")
    return redirect(url_for('admin.dashboard', season_id=season_id))

@admin_bp.route('/parse/<int:file_id>', methods=['POST'])
@admin_required
def parse_file(file_id):
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    filename      = uploaded_file.filename
    upload_path   = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)

    # 1) Ensure file exists
    if not os.path.exists(upload_path):
        flash(f"File '{filename}' not found on server.", "error")
        return redirect(url_for('admin.files_view_unique'))

    try:
        current_app.logger.debug(f"Starting parse for file '{filename}' at '{upload_path}'")

        # always pick up season from the upload record (or default to latest)
        season_id = (
            uploaded_file.season_id
            or Season.query.order_by(Season.start_date.desc()).first().id
        )

        # PRACTICE branch
        if uploaded_file.category in ['Summer Workouts', 'Pickup', 'Fall Workouts', 'Official Practices']:
            # use the file_date column (or fallback to today)
            file_date = uploaded_file.file_date or date.today()

            # Check if a practice for this date/category already exists
            practice = Practice.query.filter_by(
                season_id=season_id,
                date=file_date,
                category=uploaded_file.category
            ).first()

            if not practice:
                practice = Practice(
                    season_id=season_id,
                    date=file_date,
                    category=uploaded_file.category
                )
                db.session.add(practice)
                db.session.flush()  # ensures practice.id is available
            else:
                # Existing practice: clear any previously parsed stats so we can re-parse
                PlayerStats.query.filter_by(practice_id=practice.id).delete()
                BlueCollarStats.query.filter_by(practice_id=practice.id).delete()
                db.session.flush()

            # 2b) parse into your practice tables
            results = parse_practice_csv(
                upload_path,
                season_id=season_id,
                category=uploaded_file.category,
                file_date=file_date,
            )

            raw_lineups = results.get('lineup_efficiencies', {})
            json_lineups = {
                size: {
                    side: {",".join(combo): ppp for combo, ppp in sides.items()}
                    for side, sides in raw_lineups[size].items()
                }
                for size in raw_lineups
            }

            uploaded_file.lineup_efficiencies = json.dumps(json_lineups)
            uploaded_file.player_on_off = json.dumps(results.get('player_on_off', {}))

            # 3) mark the upload as parsed
            uploaded_file.parse_status = 'Parsed Successfully'
            uploaded_file.last_parsed  = datetime.utcnow()
            db.session.commit()

            flash("Practice parsed successfully! You can now edit it.", "success")
            return redirect(
                url_for('admin.edit_practice',
                        practice_id=practice.id,
                        season_id=season_id)
            )

        # RECRUIT branch
        elif uploaded_file.category == 'Recruit':
            parse_recruits_csv(upload_path, uploaded_file.recruit_id)
            uploaded_file.parse_status = 'Parsed Successfully'
            uploaded_file.last_parsed = datetime.utcnow()
            db.session.commit()

            flash('Recruit file parsed successfully!', 'success')
            return redirect(url_for('recruits.detail_recruit', id=uploaded_file.recruit_id))

        # GAME branch
        else:
            # 2c) run your existing game parser
            results = parse_csv(upload_path, None, season_id)

            # 2d) JSON-ify the lineup efficiencies
            raw_lineups = results.get('lineup_efficiencies', {})
            json_lineups = {
                size: {
                    side: { ",".join(combo): ppp
                            for combo, ppp in sides.items() }
                    for side, sides in raw_lineups[size].items()
                }
                for size in raw_lineups
            }

            # 3) update UploadedFile with breakdowns + status
            uploaded_file.parse_status        = 'Parsed Successfully'
            uploaded_file.last_parsed         = datetime.utcnow()
            uploaded_file.offensive_breakdown = json.dumps(
                results.get('offensive_breakdown', {}) )
            uploaded_file.defensive_breakdown = json.dumps(
                results.get('defensive_breakdown', {}) )
            uploaded_file.lineup_efficiencies = json.dumps(json_lineups)
            db.session.commit()

            # 4) redirect into your game editor
            game = Game.query.filter_by(csv_filename=filename).first()
            if not game:
                flash(
                    f"Parsed OK but couldn’t find Game record for '{filename}'",
                    "warning"
                )
                return redirect(url_for('admin.dashboard'))

            flash(
                f"File '{filename}' parsed successfully! You can now edit the game.",
                "success"
            )
            return redirect(url_for('admin.edit_game', game_id=game.id))

    except Exception as e:
        # on error, record it and flip status
        current_app.logger.exception("Error parsing CSV")
        uploaded_file.parse_status = 'Error'
        uploaded_file.parse_error  = str(e)
        db.session.commit()

        flash(f"Parsing failed for '{filename}': {e}", "error")
        return redirect(url_for('admin.files_view_unique'))


def _reparse_uploaded_practice(uploaded_file, upload_path):
    """Helper to re-parse a practice file for bulk operations."""
    season_id = (
        uploaded_file.season_id
        or Season.query.order_by(Season.start_date.desc()).first().id
    )
    file_date = uploaded_file.file_date or date.today()

    practice = Practice.query.filter_by(
        season_id=season_id,
        date=file_date,
        category=uploaded_file.category,
    ).first()
    if not practice:
        practice = Practice(
            season_id=season_id,
            date=file_date,
            category=uploaded_file.category,
        )
        db.session.add(practice)
        db.session.flush()
    else:
        PlayerStats.query.filter_by(practice_id=practice.id).delete()
        BlueCollarStats.query.filter_by(practice_id=practice.id).delete()
        poss_ids = [p.id for p in Possession.query.filter_by(practice_id=practice.id).all()]
        if poss_ids:
            PlayerPossession.query.filter(
                PlayerPossession.possession_id.in_(poss_ids)
            ).delete(synchronize_session=False)
        Possession.query.filter_by(practice_id=practice.id).delete()
        db.session.flush()

    results = parse_practice_csv(
        upload_path,
        season_id=season_id,
        category=uploaded_file.category,
        file_date=file_date,
    )

    raw_lineups = results.get("lineup_efficiencies", {})
    json_lineups = {
        size: {
            side: {",".join(combo): ppp for combo, ppp in sides.items()}
            for side, sides in raw_lineups[size].items()
        }
        for size in raw_lineups
    }

    uploaded_file.lineup_efficiencies = json.dumps(json_lineups)
    uploaded_file.player_on_off = json.dumps(results.get("player_on_off", {}))
    uploaded_file.parse_status = "Parsed Successfully"
    uploaded_file.last_parsed = datetime.utcnow()
    db.session.commit()
    return practice.id, season_id


def _reparse_uploaded_recruit(uploaded_file, upload_path):
    parse_recruits_csv(upload_path, uploaded_file.recruit_id)
    uploaded_file.parse_status = 'Parsed Successfully'
    uploaded_file.last_parsed = datetime.utcnow()
    db.session.commit()
    return uploaded_file.recruit_id


@admin_bp.route('/reparse/<int:file_id>', methods=['POST'])
@admin_required
def reparse_file(file_id):
    """Re-parse a previously uploaded CSV without removing the file."""
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    filename = uploaded_file.filename
    upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)

    if not os.path.exists(upload_path):
        flash(f"File '{filename}' not found on server.", "error")
        return redirect(url_for('admin.files_view_unique'))

    try:
        if uploaded_file.category in ['Summer Workouts', 'Pickup', 'Fall Workouts', 'Official Practices']:
            practice_id, season_id = _reparse_uploaded_practice(uploaded_file, upload_path)
            flash("Practice re-parsed successfully!", "success")
            return redirect(
                url_for('admin.edit_practice', practice_id=practice_id, season_id=season_id)
            )

        if uploaded_file.category == 'Recruit':
            rid = _reparse_uploaded_recruit(uploaded_file, upload_path)
            flash('Recruit file re-parsed successfully!', 'success')
            return redirect(url_for('recruits.detail_recruit', id=rid))

        flash('Reparse not supported for this file type.', 'error')
        return redirect(url_for('admin.files_view_unique'))

    except Exception as e:
        current_app.logger.exception('Error re-parsing CSV')
        uploaded_file.parse_status = 'Error'
        uploaded_file.parse_error = str(e)
        db.session.commit()
        flash(f"Re-parsing failed for '{filename}': {e}", 'error')
        return redirect(url_for('admin.files_view_unique'))



@admin_bp.route('/logs/<int:file_id>', methods=['GET'])
@admin_required
def view_logs(file_id):
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    return render_template('admin/logs.html', uploaded_file=uploaded_file)

@admin_bp.route('/delete/<int:file_id>', methods=['POST'])
@admin_required
def delete_file(file_id):
    uploaded_file = UploadedFile .query.get_or_404(file_id)
    filename = uploaded_file.filename
    upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)

    db.session.delete(uploaded_file)
    db.session.commit()

    if os.path.exists(upload_path):
        os.remove(upload_path)

    flash(f"File '{filename}' has been deleted.", "success")
    return redirect(url_for('admin.files_view_unique'))


@admin_bp.route('/delete-data/<int:file_id>', methods=['POST'])
@admin_required
def delete_data(file_id):
    """Delete parsed data associated with an uploaded file."""
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    filename = uploaded_file.filename

    # Determine if this was a practice, recruit, or a game
    is_practice = uploaded_file.category in [
        'Summer Workouts', 'Pickup', 'Fall Workouts', 'Official Practices'
    ]
    is_recruit = uploaded_file.category == 'Recruit'

    if is_practice:
        practice = Practice.query.filter_by(
            season_id=uploaded_file.season_id,
            date=uploaded_file.file_date,
            category=uploaded_file.category
        ).first()
        if practice:
            TeamStats.query.filter_by(practice_id=practice.id).delete()
            PlayerStats.query.filter_by(practice_id=practice.id).delete()
            BlueCollarStats.query.filter_by(practice_id=practice.id).delete()
            OpponentBlueCollarStats.query.filter_by(practice_id=practice.id).delete()
            poss_ids = [p.id for p in Possession.query.filter_by(practice_id=practice.id).all()]
            if poss_ids:
                PlayerPossession.query.filter(PlayerPossession.possession_id.in_(poss_ids)).delete(synchronize_session=False)
            Possession.query.filter_by(practice_id=practice.id).delete()
            db.session.delete(practice)
    elif is_recruit:
        RecruitShotTypeStat.query.filter_by(recruit_id=uploaded_file.recruit_id).delete()
    else:
        game = Game.query.filter_by(csv_filename=filename).first()
        if game:
            TeamStats.query.filter_by(game_id=game.id).delete()
            PlayerStats.query.filter_by(game_id=game.id).delete()
            BlueCollarStats.query.filter_by(game_id=game.id).delete()
            OpponentBlueCollarStats.query.filter_by(game_id=game.id).delete()
            poss_ids = [p.id for p in Possession.query.filter_by(game_id=game.id).all()]
            if poss_ids:
                PlayerPossession.query.filter(PlayerPossession.possession_id.in_(poss_ids)).delete(synchronize_session=False)
            Possession.query.filter_by(game_id=game.id).delete()
            db.session.delete(game)

    # Remove the upload record
    upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
    db.session.delete(uploaded_file)
    db.session.commit()

    if os.path.exists(upload_path):
        os.remove(upload_path)

    flash(f"Data for '{filename}' has been deleted.", "success")
    return redirect(url_for('admin.files_view_unique'))

@admin_bp.route('/bulk-action', methods=['POST'], endpoint='bulk_action')
@admin_required
def bulk_action_view():
    selected_ids = request.form.getlist('selected_files')
    action = request.form.get('action')

    if not selected_ids:
        flash("No files selected.", "error")
        return redirect(url_for('admin.files_view_unique'))

    files = UploadedFile.query.filter(UploadedFile.id.in_(selected_ids)).all()

    if action == 'delete':
        for file in files:
            filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], file.filename)
            db.session.delete(file)
            if os.path.exists(filepath):
                os.remove(filepath)
        db.session.commit()
        flash(f"Deleted {len(files)} files.", "success")
    elif action == 'download':
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w') as zf:
            for file in files:
                file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], file.filename)
                if os.path.exists(file_path):
                    zf.write(file_path, arcname=file.filename)
        memory_file.seek(0)
        return send_file(memory_file, download_name="downloaded_files.zip", as_attachment=True)
    elif action == 'reparse':
        count = 0
        for file in files:
            path = os.path.join(current_app.config['UPLOAD_FOLDER'], file.filename)
            if os.path.exists(path):
                if file.category in ['Summer Workouts', 'Pickup', 'Fall Workouts', 'Official Practices']:
                    _reparse_uploaded_practice(file, path)
                    count += 1
                elif file.category == 'Recruit':
                    _reparse_uploaded_recruit(file, path)
                    count += 1
        flash(f"Re-parsed {count} files.", "success")

    return redirect(url_for('admin.files_view_unique'))

@admin_bp.route('/download/<int:file_id>', methods=['GET'])
@admin_required
def download_file(file_id):
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], uploaded_file.filename)

    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        flash(f"File '{uploaded_file.filename}' not found.", "error")
        return redirect(url_for('admin.files_view_unique'))




@admin_bp.route('/game-reports')
@login_required
def game_reports():
    # filter to the currently selected season
    sid   = request.args.get('season_id', type=int)
    if not sid:
        # fallback to most recent season
        sid = Season.query.order_by(Season.start_date.desc()).first().id
    games = Game.query \
                 .filter_by(season_id=sid) \
                 .order_by(Game.game_date.desc()) \
                 .all()
    return render_template('admin/game_reports.html',
                           games=games,
                            active_page='game_reports')



@admin_bp.route('/files', methods=['GET'], endpoint='files_view_unique')
@admin_required
def files_view():
    category_filter = request.args.get('category')
    if category_filter:
        files = UploadedFile.query.filter_by(category=category_filter).order_by(UploadedFile.upload_date.desc()).all()
    else:
        files = UploadedFile.query.order_by(UploadedFile.upload_date.desc()).all()
    return render_template('files.html', files=files, selected_category=category_filter)


@admin_bp.route('/season/<int:season_id>/stats')
@login_required
def season_stats(season_id):
    # ─── Load Season & TeamStats ────────────────────────────────────────────
    season = Season.query.get_or_404(season_id)
    # aggregate team stats across all games in this season
    team_agg = db.session.query(
        func.sum(TeamStats.total_points),
        func.sum(TeamStats.total_atr_makes),    func.sum(TeamStats.total_atr_attempts),
        func.sum(TeamStats.total_fg2_makes),    func.sum(TeamStats.total_fg2_attempts),
        func.sum(TeamStats.total_fg3_makes),    func.sum(TeamStats.total_fg3_attempts),
        func.sum(TeamStats.total_ftm),          func.sum(TeamStats.total_fta),
        func.sum(TeamStats.total_assists),      func.sum(TeamStats.total_turnovers),
        func.sum(TeamStats.total_second_assists), func.sum(TeamStats.total_pot_assists),
        func.sum(TeamStats.total_blue_collar),  func.sum(TeamStats.total_possessions)
    ).filter(
        TeamStats.season_id == season_id,
        TeamStats.is_opponent == False
    ).one()

    opp_agg = db.session.query(
        func.sum(TeamStats.total_points),
        func.sum(TeamStats.total_atr_makes),    func.sum(TeamStats.total_atr_attempts),
        func.sum(TeamStats.total_fg2_makes),    func.sum(TeamStats.total_fg2_attempts),
        func.sum(TeamStats.total_fg3_makes),    func.sum(TeamStats.total_fg3_attempts),
        func.sum(TeamStats.total_ftm),          func.sum(TeamStats.total_fta),
        func.sum(TeamStats.total_assists),      func.sum(TeamStats.total_turnovers),
        func.sum(TeamStats.total_second_assists), func.sum(TeamStats.total_pot_assists),
        func.sum(TeamStats.total_blue_collar),  func.sum(TeamStats.total_possessions)
    ).filter(
        TeamStats.season_id == season_id,
        TeamStats.is_opponent == True
    ).one()

    # Unpack for readability
    (tp,  atrm, atra, fg2m, fg2a, fg3m, fg3a, ftm, fta,
     ast, tov, sec_ast, pot_ast, bc, poss) = team_agg
    (otp, o_atrm, o_atra, o_fg2m, o_fg2a, o_fg3m, o_fg3a, o_ftm, o_fta,
     o_ast, o_tov, o_sec_ast, o_pot_ast, o_bc, o_poss) = opp_agg

    # ─── Compute percentages ────────────────────────────────────────────────
    def pct(made, att, precision=1):
        return round(made/att*100, precision) if att and att>0 else 0.0

    team_stats = SimpleNamespace(
      total_points       = tp or 0,
      total_atr_makes    = atrm or 0,     total_atr_attempts = atra or 0,
      total_fg2_makes    = fg2m or 0,     total_fg2_attempts = fg2a or 0,
      total_fg3_makes    = fg3m or 0,     total_fg3_attempts = fg3a or 0,
      total_ftm          = ftm or 0,      total_fta           = fta or 0,
      total_assists      = ast or 0,      total_turnovers     = tov or 0,
      total_second_assists = sec_ast or 0, total_pot_assists   = pot_ast or 0,
      total_blue_collar  = bc or 0,       total_possessions   = poss or 0,
      assist_pct    = pct(ast or 0, atrm+fg2m+fg3m),
      turnover_pct  = pct(tov or 0, poss or 1),
      tcr_pct       = 0.0,  # requires detailed breakdown per possession type
      oreb_pct      = 0.0,  # requires rebound-chance calc
      ft_rate       = pct(fta or 0, atra+fg2a+fg3a),
      good_shot_pct = pct((ftm or 0)+(atrm or 0)+(fg3m or 0), (fta or 0)+(atra or 0)+(fg3a or 0)+(fg2m or 0))
    )

    opponent_stats = SimpleNamespace(
      total_points       = otp or 0,
      total_atr_makes    = o_atrm or 0,    total_atr_attempts = o_atra or 0,
      total_fg2_makes    = o_fg2m or 0,    total_fg2_attempts = o_fg2a or 0,
      total_fg3_makes    = o_fg3m or 0,    total_fg3_attempts = o_fg3a or 0,
      total_ftm          = o_ftm or 0,     total_fta           = o_fta or 0,
      total_assists      = o_ast or 0,     total_turnovers     = o_tov or 0,
      total_second_assists = o_sec_ast or 0, total_pot_assists  = o_pot_ast or 0,
      total_blue_collar  = o_bc or 0,      total_possessions   = o_poss or 0,
      assist_pct    = pct(o_ast or 0, o_atrm+o_fg2m+o_fg3m),
      turnover_pct  = pct(o_tov or 0, o_poss or 1),
      tcr_pct       = 0.0,
      oreb_pct      = 0.0,
      ft_rate       = pct(o_fta or 0, o_atra+o_fg2a+o_fg3a),
      good_shot_pct = pct((o_ftm or 0)+(o_atrm or 0)+(o_fg3m or 0),
                          (o_fta or 0)+(o_atra or 0)+(o_fg3a or 0)+(o_fg2m or 0))
    )

    # ─── Blue Collar Totals ────────────────────────────────────────────────
    blue_breakdown = db.session.query(
        func.sum(BlueCollarStats.def_reb).label('def_reb'),
        func.sum(BlueCollarStats.off_reb).label('off_reb'),
        func.sum(BlueCollarStats.misc).label('misc'),
        func.sum(BlueCollarStats.deflection).label('deflection'),
        func.sum(BlueCollarStats.steal).label('steal'),
        func.sum(BlueCollarStats.block).label('block'),
        func.sum(BlueCollarStats.floor_dive).label('floor_dive'),
        func.sum(BlueCollarStats.charge_taken).label('charge_taken'),
        func.sum(BlueCollarStats.reb_tip).label('reb_tip')
    ).filter(
        BlueCollarStats.season_id == season_id
    ).one()

    opp_blue_breakdown = db.session.query(
        func.sum(OpponentBlueCollarStats.def_reb).label('def_reb'),
        func.sum(OpponentBlueCollarStats.off_reb).label('off_reb'),
        func.sum(OpponentBlueCollarStats.misc).label('misc'),
        func.sum(OpponentBlueCollarStats.deflection).label('deflection'),
        func.sum(OpponentBlueCollarStats.steal).label('steal'),
        func.sum(OpponentBlueCollarStats.block).label('block'),
        func.sum(OpponentBlueCollarStats.floor_dive).label('floor_dive'),
        func.sum(OpponentBlueCollarStats.charge_taken).label('charge_taken'),
        func.sum(OpponentBlueCollarStats.reb_tip).label('reb_tip')
    ).filter(
        OpponentBlueCollarStats.season_id == season_id
    ).one()

    # ─── Load & Concatenate All CSVs in Season for Possession Breakdown ─────
    dfs = []
    for game in season.games:
        path = os.path.join(current_app.config['UPLOAD_FOLDER'], game.csv_filename)
        if os.path.exists(path):
            df = pd.read_csv(path)
            # preserve the original “GAME SPLITS” column
            df['GAME_SPLITS'] = df.get('GAME SPLITS')
            # split on the first comma only, then strip whitespace
            df['Period'] = (
                df['GAME_SPLITS']
                .fillna('')
                .str.split(',', n=1)
                .str[0]
                .str.strip()
            )
            dfs.append(df)
    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
        off_break, def_break, per_off, per_def = get_possession_breakdown_detailed(full_df)
    else:
        off_break = def_break = {}
        per_off = {h: SimpleNamespace(points=0, count=0) for h in ['1st Half','2nd Half','Overtime']}
        per_def = per_off

        # collect all game-level lineup JSON
    season_lineups = {}
    for game in season.games:
        uf = UploadedFile.query.filter_by(filename=game.csv_filename).first()
        if not uf or not uf.lineup_efficiencies:
            continue
        per_game = json.loads(uf.lineup_efficiencies)
        for size, sides in per_game.items():
            size = int(size)
            sl = season_lineups.setdefault(size, {'offense': {}, 'defense': {}})
            for side in ('offense','defense'):
                for combo, ppp in sides.get(side, {}).items():
                    sl[side].setdefault(combo, []).append(ppp)

    # average them and pick best/worst 5
    best_offense_season = {}
    worst_offense_season = {}
    best_defense_season = {}
    worst_defense_season = {}

    for size, sides in season_lineups.items():
        # offense
        avg_off = {c: sum(v)/len(v) for c,v in sides['offense'].items()}
        best_offense_season[size]  = sorted(avg_off.items(), key=lambda x: x[1], reverse=True)[:5]
        worst_offense_season[size] = sorted(avg_off.items(), key=lambda x: x[1])[:5]
        # defense
        avg_def = {c: sum(v)/len(v) for c,v in sides['defense'].items()}
        best_defense_season[size]  = sorted(avg_def.items(), key=lambda x: x[1])[:5]
        worst_defense_season[size] = sorted(avg_def.items(), key=lambda x: x[1], reverse=True)[:5]


    # ─── RENDER TEMPLATE ────────────────────────────────────────────────────
    return render_template(
        'admin/season_stats.html',
        active_page='stats',
        season=season,
        team_stats=team_stats,
        opponent_stats=opponent_stats,
        blue_collar_stats=blue_breakdown,
        opponent_blue_coll_stats=opp_blue_breakdown,
        offensive_breakdown=off_break,
        defensive_breakdown=def_break,
        periodic_offense=per_off,
        periodic_defense=per_def,
        best_offense=best_offense_season,
        worst_offense=worst_offense_season,
        best_defense=best_defense_season,
        worst_defense=worst_defense_season,
    )



@admin_bp.route('/stats/<int:game_id>')
@login_required
def game_stats(game_id):
    # ─── Load Game & Stored TeamStats ─────────────────────────────────────────
    game = Game.query.get_or_404(game_id)
    team_stats = TeamStats.query.filter_by(game_id=game_id, is_opponent=False).first()
    opponent_stats = TeamStats.query.filter_by(game_id=game_id, is_opponent=True).first()

    # Dummy fallback
    def default_stats():
        class D:
            total_atr_attempts = total_fg2_attempts = total_fg3_attempts = 0
            total_atr_makes    = total_fg2_makes    = total_fg3_makes    = 0
            total_ftm = total_fta = total_possessions = 0
            total_assists = total_turnovers = 0
            total_second_assists = total_pot_assists = 0
            total_blue_collar = 0
        return D()
    if not team_stats:     team_stats     = default_stats()
    if not opponent_stats: opponent_stats = default_stats()

    # ─── Load DB rows you’ll need in the template ──────────────────────────────
    player_stats            = PlayerStats.query.filter_by(game_id=game_id).all()

    # Summed team‐level blue‐collar stats
    team_blue_breakdown = db.session.query(
        func.sum(BlueCollarStats.def_reb).label('def_reb'),
        func.sum(BlueCollarStats.off_reb).label('off_reb'),
        func.sum(BlueCollarStats.misc).label('misc'),
        func.sum(BlueCollarStats.deflection).label('deflection'),
        func.sum(BlueCollarStats.steal).label('steal'),
        func.sum(BlueCollarStats.block).label('block'),
        func.sum(BlueCollarStats.floor_dive).label('floor_dive'),
        func.sum(BlueCollarStats.charge_taken).label('charge_taken'),
        func.sum(BlueCollarStats.reb_tip).label('reb_tip')
    ).filter(BlueCollarStats.game_id == game_id).one()

    opponent_blue_breakdown = db.session.query(
        func.sum(OpponentBlueCollarStats.def_reb).label('def_reb'),
        func.sum(OpponentBlueCollarStats.off_reb).label('off_reb'),
        func.sum(OpponentBlueCollarStats.misc).label('misc'),
        func.sum(OpponentBlueCollarStats.deflection).label('deflection'),
        func.sum(OpponentBlueCollarStats.steal).label('steal'),
        func.sum(OpponentBlueCollarStats.block).label('block'),
        func.sum(OpponentBlueCollarStats.floor_dive).label('floor_dive'),
        func.sum(OpponentBlueCollarStats.charge_taken).label('charge_taken'),
        func.sum(OpponentBlueCollarStats.reb_tip).label('reb_tip')
    ).filter(OpponentBlueCollarStats.game_id == game_id).one()

    possessions = Possession.query.filter_by(game_id=game_id).all()


    # ─── LOAD CSV & TAG PERIOD ────────────────────────────────────────────────
    csv_path = os.path.join(current_app.config['UPLOAD_FOLDER'], game.csv_filename)
    df = pd.read_csv(csv_path)
    # Grab just “1st Half”, “2nd Half”, or “Overtime”
    df['Period'] = (
        df['GAME SPLITS']
          .fillna('')
          .str.split(',', n=1).str[0]
          .str.strip()
    )

    # ─── POSSESSION BREAKDOWNS & LINEUPS (UNCHANGED) ──────────────────────────
    offensive_breakdown, defensive_breakdown, periodic_offense, periodic_defense = \
        get_possession_breakdown_detailed(df)
    uploaded_file = UploadedFile.query.filter_by(filename=game.csv_filename).first()
    lineup_efficiencies = (
        json.loads(uploaded_file.lineup_efficiencies)
        if uploaded_file and uploaded_file.lineup_efficiencies else {}
    )
    best_offense = {}
    worst_offense = {}
    best_defense = {}
    worst_defense = {}
    for size, sides in lineup_efficiencies.items():
        # Offense
        off = sides.get('offense', {})
        best_offense[size]  = sorted(off.items(), key=lambda x: x[1], reverse=True)[:5]
        worst_offense[size] = sorted(off.items(), key=lambda x: x[1])[:5]
        # Defense
        dfens = sides.get('defense', {})
        best_defense[size]  = sorted(dfens.items(), key=lambda x: x[1])[:5]
        worst_defense[size] = sorted(dfens.items(), key=lambda x: x[1], reverse=True)[:5]

    # ─── DEFENSIVE SECONDARY METRICS ──────────────────────────────────────────
    # We'll treat the opponent’s offense as “Defense rows” in the CSV:
    defense_rows = df[df['Row'] == "Defense"]

    # Helper: count tokens in the OPP STATS column
    def count_def_tokens(rows, tokens):
        return sum(
            1
            for _, r in rows.iterrows()
            for tok in extract_tokens(r.get("OPP STATS", ""))
            if tok in tokens
        )

    # 1) OREB % Allowed
    opp_atr_miss   = opponent_stats.total_atr_attempts - opponent_stats.total_atr_makes
    opp_fg2_miss   = opponent_stats.total_fg2_attempts - opponent_stats.total_fg2_makes
    opp_fg3_miss   = opponent_stats.total_fg3_attempts - opponent_stats.total_fg3_makes
    opp_reb_chance = opp_atr_miss + opp_fg2_miss + opp_fg3_miss
    opp_oreb_pct = (
        round(opponent_blue_breakdown.off_reb / opp_reb_chance * 100, 1)
        if opp_reb_chance > 0 else 0.0
    )

    # 2) FT Rate Allowed (FTA ÷ possessions)
    # count opponent free‐throw attempts
    opp_fta = opponent_stats.total_fta
    # opponent field‐goal attempts = ATR + 2FG + 3FG attempts
    opp_fga = (
        opponent_stats.total_atr_attempts
        + opponent_stats.total_fg2_attempts
        + opponent_stats.total_fg3_attempts
    )
    opp_ft_rate = (
        round(opp_fta / opp_fga * 100, 1)
        if opp_fga > 0 else 0.0
    )

    # 3) Good Shot % Allowed
    opp_good = (
        opponent_stats.total_fta
      + opponent_stats.total_atr_makes + opp_atr_miss
      + opponent_stats.total_fg3_makes + opp_fg3_miss
    )
    opp_bad = opponent_stats.total_fg2_makes + opp_fg2_miss
    opp_den = opp_good + opp_bad
    opp_good_shot_pct = (
        round(opp_good / opp_den * 100, 2)
        if opp_den > 0 else 0.0
    )

    # 4) Assist % Allowed (assists ÷ made FGs)
    opp_fgm_made   = (
        opponent_stats.total_atr_makes
      + opponent_stats.total_fg2_makes
      + opponent_stats.total_fg3_makes
    )
    opp_assist_pct = (
        round(opponent_stats.total_assists / opp_fgm_made * 100, 1)
        if opp_fgm_made > 0 else 0.0
    )

    # 5) Turnover % Allowed (TOs ÷ possessions)
    opp_turnover_pct = (
        round(opponent_stats.total_turnovers / opponent_stats.total_possessions * 100, 1)
        if opponent_stats.total_possessions > 0 else 0.0
    )

    # 6) PPP Allowed (points allowed ÷ possessions)
    opp_ppp = (
        round(opponent_stats.total_points / opponent_stats.total_possessions, 2)
        if opponent_stats.total_possessions > 0 else 0.0
    )

    # 7) TCR Allowed (transition conversions ÷ transition opportunities)
    # Denominator: made+missed FG + steals (from OPP STATS), minus neutrals
    made   = count_def_tokens(defense_rows, ("ATR+", "2FG+", "3FG+"))
    missed = count_def_tokens(defense_rows, ("ATR-", "2FG-", "3FG-"))
    steals = count_def_tokens(defense_rows, ("Steal",))
    neutrals = defense_rows[defense_rows['TEAM'].fillna('').str.contains("Neutral")]
    made_neu   = count_def_tokens(neutrals, ("ATR+", "2FG+", "3FG+"))
    missed_neu = count_def_tokens(neutrals, ("ATR-", "2FG-", "3FG-"))
    steals_neu = count_def_tokens(neutrals, ("Steal",))
    trans_opps = (made + missed + steals) - (made_neu + missed_neu + steals_neu)

    # Numerator: any OPP stat in transition that’s a conversion
    trans_rows = defense_rows[
        defense_rows['POSSESSION TYPE'].fillna('').str.contains("Transition")
    ]
    conv = count_def_tokens(trans_rows, (
        "ATR+", "ATR-",
        "2FG+", "2FG-",
        "3FG+", "3FG-",
        "FT+",   # free throws made
        "Fouled"
    ))
    opp_tcr_pct = (
        round(conv / trans_opps * 100, 1)
        if trans_opps > 0 else 0.0
    )

    # ─── RENDER ───────────────────────────────────────────────────────────────
    return render_template(
        'admin/game_stats.html',
        active_page='stats',
        game=game,
        team_stats=team_stats,
        opponent_stats=opponent_stats,
        player_stats=player_stats,
        blue_collar_stats=team_blue_breakdown,
        opponent_blue_coll_stats=opponent_blue_breakdown,
        possessions=possessions,

        # breakdowns
        offensive_breakdown=offensive_breakdown,
        defensive_breakdown=defensive_breakdown,
        periodic_offense=periodic_offense,
        periodic_defense=periodic_defense,

        # lineup efficiencies
        lineup_efficiencies=lineup_efficiencies,
        best_offense=best_offense,
        worst_offense=worst_offense,
        best_defense=best_defense,
        worst_defense=worst_defense,

        # defensive secondary metrics
        opp_oreb_pct=opp_oreb_pct,
        opp_ft_rate=opp_ft_rate,
        opp_good_shot_pct=opp_good_shot_pct,
        opp_assist_pct=opp_assist_pct,
        opp_turnover_pct=opp_turnover_pct,
        opp_ppp=opp_ppp,
        opp_tcr_pct=opp_tcr_pct,
    )




@admin_bp.route('/practice/<int:practice_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_practice(practice_id):
    practice    = Practice.query.get_or_404(practice_id)
    player_stats = PlayerStats.query.filter_by(practice_id=practice_id).all()
    blue_stats   = BlueCollarStats.query.filter_by(practice_id=practice_id).all()
    return render_template(
        'admin/edit_practice.html',
        practice     = practice,
        player_stats = player_stats,
        blue_stats   = blue_stats,
        active_page  = 'practices'
    )


@admin_bp.route('/practice-reports')
@login_required
def practice_reports():
    # filter to currently selected season (like games)
    sid = request.args.get('season_id', type=int)
    if not sid:
        sid = Season.query.order_by(Season.start_date.desc()).first().id
    practices = Practice.query \
                 .filter_by(season_id=sid) \
                 .order_by(Practice.date.desc()) \
                 .all()
    return render_template('admin/practice_reports.html',
                           practices=practices,
                           active_page='practice_reports')


@admin_bp.route('/game/<int:game_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_game(game_id):
    game = Game.query.get_or_404(game_id)  # load the game record
    if request.method == 'POST':
        try:
            date_str = request.form.get('game_date')
            if date_str:
                from datetime import datetime
                game.game_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            game.opponent_name = request.form.get('opponent_name')
            game.result = request.form.get('result')

            db.session.commit()
            flash("Game updated successfully!", "success")
            return redirect(url_for('admin.game_reports'))

        except Exception as e:
            db.session.rollback()
            flash(f"Error updating game: {e}", "error")

    return render_template('admin/edit_game.html', game=game)



# ─── Helper: aggregate stats for a list of PlayerStats records ─────────────────
def aggregate_stats(stats_list):
    """
    Given a list of PlayerStats records, compute totals, eFG%, points-per-shot,
    and assist/turnover ratios.
    """
    # 1) basic sums
    agg = {
        "points":          sum(s.points        or 0 for s in stats_list),
        "assists":         sum(s.assists       or 0 for s in stats_list),
        "turnovers":       sum(s.turnovers     or 0 for s in stats_list),
        "atr_attempts":    sum(s.atr_attempts or 0 for s in stats_list),
        "atr_makes":       sum(s.atr_makes    or 0 for s in stats_list),
        "fg2_attempts":    sum(s.fg2_attempts or 0 for s in stats_list),
        "fg2_makes":       sum(s.fg2_makes    or 0 for s in stats_list),
        "fg3_attempts":    sum(s.fg3_attempts or 0 for s in stats_list),
        "fg3_makes":       sum(s.fg3_makes    or 0 for s in stats_list),
        "fta":             sum(s.fta          or 0 for s in stats_list),
        "ftm":             sum(s.ftm          or 0 for s in stats_list),
        "second_assists":  sum(s.second_assists or 0 for s in stats_list),
        "pot_assists":     sum(s.pot_assists  or 0 for s in stats_list),
    }
    # 2) effective FG% and points/shot
    total_shots = agg["atr_attempts"] + agg["fg2_attempts"] + agg["fg3_attempts"]
    if total_shots:
        efg = (agg["atr_makes"] + agg["fg2_makes"] + 1.5 * agg["fg3_makes"]) / total_shots
        agg["efg_pct"]         = round(efg * 100, 1)
        agg["points_per_shot"] = round(efg * 2, 2)
    else:
        agg["efg_pct"] = 0.0
        agg["points_per_shot"] = 0.0

    if total_shots:
        agg["atr_freq_pct"] = round(100 * agg["atr_attempts"] / total_shots, 1)
        agg["fg3_freq_pct"] = round(100 * agg["fg3_attempts"] / total_shots, 1)
    else:
        agg["atr_freq_pct"] = 0.0
        agg["fg3_freq_pct"] = 0.0

    # shot percentages
    agg["atr_pct"] = round(agg["atr_makes"] / agg["atr_attempts"] * 100, 1) if agg["atr_attempts"] else 0.0
    agg["fg3_pct"] = round(agg["fg3_makes"] / agg["fg3_attempts"] * 100, 1) if agg["fg3_attempts"] else 0.0

    # ──────────────────────────────────────────────────────────────────────────
    # Pure FG2 metrics (exclude at-rim)
    fg2_att = agg.get('fg2_attempts', 0)
    fg2_made = agg.get('fg2_makes', 0)
    agg['two_fg_pct'] = round(fg2_made / fg2_att * 100, 1) if fg2_att else None
    agg['two_fg_freq_pct'] = round(fg2_att / total_shots * 100, 1) if total_shots else None

    # Normalize 3FG keys to match overall helper
    three_att = agg.get('fg3_attempts', 0)
    three_made = agg.get('fg3_makes', 0)
    agg['three_fg_pct'] = round(three_made / three_att * 100, 1) if three_att else None
    agg['three_fg_freq_pct'] = round(three_att / total_shots * 100, 1) if total_shots else None
    # ──────────────────────────────────────────────────────────────────────────
    # 3) assist/turnover ratios
    if agg["turnovers"]:
        agg["assist_turnover_ratio"]     = round(agg["assists"]     / agg["turnovers"], 2)
        total_ast = agg["assists"] + agg["second_assists"] + agg["pot_assists"]
        agg["adj_assist_turnover_ratio"] = round(total_ast            / agg["turnovers"], 2)
    else:
        agg["assist_turnover_ratio"]     = 0.0
        agg["adj_assist_turnover_ratio"] = 0.0

    return SimpleNamespace(**agg)


# ─── Helper: sum blue-collar stats for given PlayerStats records ──────────────

def get_blue_breakdown(stats_list, roster_id):
    """
    Given a list of PlayerStats for one player, sum their BlueCollarStats.
    If any stats_list rows have game_ids, we filter by those; otherwise by practice_ids.
    """
    if not stats_list:
        return SimpleNamespace(
            def_reb=0, off_reb=0, misc=0, deflection=0,
            steal=0, block=0, floor_dive=0,
            charge_taken=0, reb_tip=0, total_blue_collar=0
        )

    # collect ids
    game_ids     = [r.game_id     for r in stats_list if r.game_id]
    practice_ids = [r.practice_id for r in stats_list if r.practice_id]

    # pick filter: prefer games if present
    if game_ids:
        fk_cond = BlueCollarStats.game_id.in_(game_ids)
    else:
        fk_cond = BlueCollarStats.practice_id.in_(practice_ids)

    # now query
    bc = db.session.query(
        func.coalesce(func.sum(BlueCollarStats.def_reb),      0).label('def_reb'),
        func.coalesce(func.sum(BlueCollarStats.off_reb),      0).label('off_reb'),
        func.coalesce(func.sum(BlueCollarStats.misc),         0).label('misc'),
        func.coalesce(func.sum(BlueCollarStats.deflection),   0).label('deflection'),
        func.coalesce(func.sum(BlueCollarStats.steal),        0).label('steal'),
        func.coalesce(func.sum(BlueCollarStats.block),        0).label('block'),
        func.coalesce(func.sum(BlueCollarStats.floor_dive),   0).label('floor_dive'),
        func.coalesce(func.sum(BlueCollarStats.charge_taken), 0).label('charge_taken'),
        func.coalesce(func.sum(BlueCollarStats.reb_tip),      0).label('reb_tip'),
        func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label('total_blue_collar'),
    ).filter(
        BlueCollarStats.player_id == roster_id,
        fk_cond
    ).one()

    return SimpleNamespace(
        def_reb=bc.def_reb,
        off_reb=bc.off_reb,
        misc=bc.misc,
        deflection=bc.deflection,
        steal=bc.steal,
        block=bc.block,
        floor_dive=bc.floor_dive,
        charge_taken=bc.charge_taken,
        reb_tip=bc.reb_tip,
        total_blue_collar=bc.total_blue_collar
    )


# ─── Helper: compute filtered blue-collar totals from stat_details ────────────
def compute_filtered_blue(stats_records, label_set):
    """Return blue-collar counts filtered by drill label set."""
    if not stats_records:
        zeros = {k: 0 for k in blue_collar_values.keys()}
        zeros["total_blue_collar"] = 0
        return SimpleNamespace(**zeros)

    counts = {k: 0 for k in blue_collar_values.keys()}
    for rec in stats_records:
        if not rec.stat_details:
            continue
        details = (
            json.loads(rec.stat_details)
            if isinstance(rec.stat_details, str)
            else rec.stat_details
        )
        for ev in details:
            event = ev.get("event")
            if event not in counts:
                continue
            labels = {
                lbl.strip().upper()
                for lbl in ev.get("drill_labels", [])
                if isinstance(lbl, str) and lbl.strip()
            }
            if label_set and not (labels & label_set):
                continue
            counts[event] += 1

    counts["total_blue_collar"] = sum(
        counts[k] * blue_collar_values[k] for k in blue_collar_values.keys()
    )
    return SimpleNamespace(**counts)


# ─── Helper: compute filtered aggregate stats from details ─────────────
def compute_filtered_totals(stats_records, label_set):
    """Aggregate points and basic stats filtered by drill labels."""
    totals = {
        "points": 0,
        "assists": 0,
        "turnovers": 0,
        "pot_assists": 0,
        "second_assists": 0,
        "atr_makes": 0,
        "atr_attempts": 0,
        "fg2_makes": 0,
        "fg2_attempts": 0,
        "fg3_makes": 0,
        "fg3_attempts": 0,
        "ftm": 0,
        "fta": 0,
    }

    event_map = {
        "assists": "assists",
        "turnovers": "turnovers",
        "pot_assists": "pot_assists",
        "second_assists": "second_assists",
        "foul_by": "foul_by",
        "sprint_wins": "sprint_wins",
        "sprint_losses": "sprint_losses",
        "win": "practice_wins",
        "loss": "practice_losses",
    }

    for rec in stats_records:
        if rec.shot_type_details:
            shots = (
                json.loads(rec.shot_type_details)
                if isinstance(rec.shot_type_details, str)
                else rec.shot_type_details
            )
            for shot in shots:
                labels = {
                    lbl.strip().upper()
                    for lbl in re.split(r",", shot.get("possession_type", ""))
                    if lbl.strip()
                }
                labels.update(
                    lbl.strip().upper()
                    for lbl in shot.get("drill_labels", [])
                    if isinstance(lbl, str) and lbl.strip()
                )
                if label_set and not (labels & label_set):
                    continue
                sc = shot.get("shot_class", "").lower()
                made = shot.get("result") == "made"
                if sc == "atr":
                    totals["atr_attempts"] += 1
                    if made:
                        totals["atr_makes"] += 1
                        totals["points"] += 2
                elif sc == "2fg":
                    totals["fg2_attempts"] += 1
                    if made:
                        totals["fg2_makes"] += 1
                        totals["points"] += 2
                elif sc == "3fg":
                    totals["fg3_attempts"] += 1
                    if made:
                        totals["fg3_makes"] += 1
                        totals["points"] += 3
                elif sc == "ft":
                    totals["fta"] += 1
                    if made:
                        totals["ftm"] += 1
                        totals["points"] += 1

        if rec.stat_details:
            details = (
                json.loads(rec.stat_details)
                if isinstance(rec.stat_details, str)
                else rec.stat_details
            )
            for ev in details:
                lbls = {
                    lbl.strip().upper()
                    for lbl in ev.get("drill_labels", [])
                    if isinstance(lbl, str) and lbl.strip()
                }
                if label_set and not (lbls & label_set):
                    continue
                key = event_map.get(ev.get("event"))
                if key and key in totals:
                    totals[key] += 1

    total_shots = (
        totals["atr_attempts"] + totals["fg2_attempts"] + totals["fg3_attempts"]
    )
    if total_shots:
        efg = (
            totals["atr_makes"] + totals["fg2_makes"] + 1.5 * totals["fg3_makes"]
        ) / total_shots
        totals["efg_pct"] = round(efg * 100, 1)
        totals["points_per_shot"] = round(efg * 2, 2)
        totals["atr_freq_pct"] = round(totals["atr_attempts"] / total_shots * 100, 1)
        totals["fg3_freq_pct"] = round(totals["fg3_attempts"] / total_shots * 100, 1)
    else:
        totals["efg_pct"] = 0.0
        totals["points_per_shot"] = 0.0
        totals["atr_freq_pct"] = 0.0
        totals["fg3_freq_pct"] = 0.0

    totals["atr_pct"] = round(totals["atr_makes"] / totals["atr_attempts"] * 100, 1) if totals["atr_attempts"] else 0.0
    totals["fg3_pct"] = round(totals["fg3_makes"] / totals["fg3_attempts"] * 100, 1) if totals["fg3_attempts"] else 0.0

    # ──────────────────────────────────────────────────────────────────────────
    # Pure FG2 metrics (exclude at-rim)
    fg2_att = totals.get('fg2_attempts', 0)
    fg2_made = totals.get('fg2_makes', 0)
    totals['two_fg_pct'] = round(fg2_made / fg2_att * 100, 1) if fg2_att else None
    totals['two_fg_freq_pct'] = round(fg2_att / total_shots * 100, 1) if total_shots else None

    # Normalize 3FG keys to match overall helper
    three_att = totals.get('fg3_attempts', 0)
    three_made = totals.get('fg3_makes', 0)
    totals['three_fg_pct'] = round(three_made / three_att * 100, 1) if three_att else None
    totals['three_fg_freq_pct'] = round(three_att / total_shots * 100, 1) if total_shots else None
    # ──────────────────────────────────────────────────────────────────────────

    if totals["turnovers"]:
        totals["assist_turnover_ratio"] = round(
            totals["assists"] / totals["turnovers"], 2
        )
        total_ast = (
            totals["assists"] + totals["second_assists"] + totals["pot_assists"]
        )
        totals["adj_assist_turnover_ratio"] = round(total_ast / totals["turnovers"], 2)
    else:
        totals["assist_turnover_ratio"] = 0.0
        totals["adj_assist_turnover_ratio"] = 0.0

    return SimpleNamespace(**totals)


# ─── Helper: compute team shot-type aggregates and summaries ─────────────
def compute_team_shot_details(stats_records, label_set):
    """Return season shot totals and detail summaries for a list of PlayerStats."""
    all_details = []
    for rec in stats_records:
        if not rec.shot_type_details:
            continue
        js = (
            json.loads(rec.shot_type_details)
            if isinstance(rec.shot_type_details, str)
            else rec.shot_type_details
        )
        for shot in js:
            labels = {
                lbl.strip().upper()
                for lbl in re.split(r",", shot.get("possession_type", ""))
                if lbl.strip()
            }
            labels.update(
                lbl.strip().upper() for lbl in shot.get("drill_labels", []) if lbl.strip()
            )
            if label_set and not (labels & label_set):
                continue
            all_details.append(shot)

    makes_atr = sum(1 for s in all_details if s.get("shot_class", "").lower() == "atr" and s.get("result") == "made")
    att_atr   = sum(1 for s in all_details if s.get("shot_class", "").lower() == "atr")
    makes_fg2 = sum(1 for s in all_details if s.get("shot_class", "").lower() == "2fg" and s.get("result") == "made")
    att_fg2   = sum(1 for s in all_details if s.get("shot_class", "").lower() == "2fg")
    makes_fg3 = sum(1 for s in all_details if s.get("shot_class", "").lower() == "3fg" and s.get("result") == "made")
    att_fg3   = sum(1 for s in all_details if s.get("shot_class", "").lower() == "3fg")

    total_att = att_atr + att_fg2 + att_fg3
    raw_totals = SimpleNamespace(
        atr=SimpleNamespace(
            makes=makes_atr,
            attempts=att_atr,
            fg_pct=(makes_atr / att_atr * 100) if att_atr else 0,
            pps=round((makes_atr * 2) / att_atr, 2) if att_atr else 0,
            freq=(att_atr / total_att * 100) if total_att else 0,
        ),
        fg2=SimpleNamespace(
            makes=makes_fg2,
            attempts=att_fg2,
            fg_pct=(makes_fg2 / att_fg2 * 100) if att_fg2 else 0,
            pps=round((makes_fg2 * 2) / att_fg2, 2) if att_fg2 else 0,
            freq=(att_fg2 / total_att * 100) if total_att else 0,
        ),
        fg3=SimpleNamespace(
            makes=makes_fg3,
            attempts=att_fg3,
            fg_pct=(makes_fg3 / att_fg3 * 100) if att_fg3 else 0,
            pps=round((makes_fg3 * 3) / att_fg3, 2) if att_fg3 else 0,
            freq=(att_fg3 / total_att * 100) if total_att else 0,
        ),
    )

    detail_counts = {"atr": {}, "fg2": {}, "fg3": {}}
    cls_map = {"atr": "atr", "2fg": "fg2", "3fg": "fg3"}

    for shot in all_details:
        sc = shot.get("shot_class", "").lower()
        shot_cls = cls_map.get(sc)
        if not shot_cls:
            continue

        made = shot.get("result") == "made"
        raw = shot.get("possession_type", "").strip().lower()
        if "trans" in raw:
            ctx = "transition"
        elif "half" in raw:
            ctx = "halfcourt"
        else:
            ctx = "total"

        labels_for_this_shot = []
        if shot.get("Assisted"):
            labels_for_this_shot.append("Assisted")
        else:
            labels_for_this_shot.append("Non-Assisted")

        if sc in ("atr", "2fg"):
            suffix_keys = ["Type", "Defenders", "Dribble", "Feet", "Hands", "Other", "PA", "RA"]
            for suffix in suffix_keys:
                val = shot.get(f"{sc}_{suffix.lower().replace(' ', '_')}", "")
                if val:
                    labels_for_this_shot.extend([lbl.strip() for lbl in re.split(r",", str(val)) if lbl.strip()])
        else:
            suffix_keys = ["Contest", "Footwork", "Good/Bad", "Line", "Move", "Pocket", "Shrink", "Type"]
            for suffix in suffix_keys:
                key = f"{sc}_{suffix.lower().replace('/', '_').replace(' ', '_')}"
                val = shot.get(key, "")
                if val:
                    labels_for_this_shot.extend([lbl.strip() for lbl in re.split(r",", str(val)) if lbl.strip()])

        for scheme in ("scheme_attack", "scheme_drive", "scheme_pass"):
            val = shot.get(f"{sc}_{scheme}", "")
            if val:
                labels_for_this_shot.extend([lbl.strip() for lbl in re.split(r",", str(val)) if lbl.strip()])

        for lbl in set(labels_for_this_shot):
            ent = detail_counts[shot_cls].setdefault(
                lbl,
                {
                    "total": {"attempts": 0, "makes": 0},
                    "transition": {"attempts": 0, "makes": 0},
                    "halfcourt": {"attempts": 0, "makes": 0},
                },
            )
            ent["total"]["attempts"] += 1
            if made:
                ent["total"]["makes"] += 1
            if ctx in ("transition", "halfcourt"):
                ent[ctx]["attempts"] += 1
                if made:
                    ent[ctx]["makes"] += 1

    for stype, bucket in detail_counts.items():
        for data in bucket.values():
            total_att = data["total"]["attempts"] or 1
            pts = 2 if stype in ("atr", "fg2") else 3
            for ctx in ("total", "transition", "halfcourt"):
                a = data[ctx]["attempts"]
                m = data[ctx]["makes"]
                fg = (m / a) if a else 0
                data[ctx]["fg_pct"] = fg
                data[ctx]["pps"] = round(pts * fg, 2) if a else 0
                data[ctx]["freq_pct"] = a / total_att

    shot_summaries = {}
    for stype, bucket in detail_counts.items():
        for lbl in ("Assisted", "Non-Assisted"):
            bucket.setdefault(
                lbl,
                {
                    "total": {"attempts": 0, "makes": 0, "fg_pct": 0, "pps": 0, "freq_pct": 0},
                    "transition": {"attempts": 0, "makes": 0, "fg_pct": 0, "pps": 0, "freq_pct": 0},
                    "halfcourt": {"attempts": 0, "makes": 0, "fg_pct": 0, "pps": 0, "freq_pct": 0},
                },
            )

        cats = {
            lbl: SimpleNamespace(
                total=SimpleNamespace(**data["total"]),
                transition=SimpleNamespace(**data["transition"]),
                halfcourt=SimpleNamespace(**data["halfcourt"]),
            )
            for lbl, data in bucket.items()
        }

        ta = sum(d["total"]["attempts"] for d in bucket.values()) or 1
        tm = sum(d["total"]["makes"] for d in bucket.values())
        pts = 2 if stype in ("atr", "fg2") else 3

        shot_summaries[stype] = SimpleNamespace(
            total=SimpleNamespace(
                attempts=ta,
                makes=tm,
                fg_pct=(tm / ta * 100),
                pps=round(pts * tm / ta, 2),
            ),
            cats=cats,
            transition=SimpleNamespace(
                attempts=sum(d["transition"]["attempts"] for d in bucket.values()),
                makes=sum(d["transition"]["makes"] for d in bucket.values()),
                fg_pct=sum(d["transition"]["makes"] for d in bucket.values())
                / (sum(d["transition"]["attempts"] for d in bucket.values()) or 1),
                pps=round(
                    pts
                    * sum(d["transition"]["makes"] for d in bucket.values())
                    / (sum(d["transition"]["attempts"] for d in bucket.values()) or 1),
                    2,
                ),
            ),
            halfcourt=SimpleNamespace(
                attempts=sum(d["halfcourt"]["attempts"] for d in bucket.values()),
                makes=sum(d["halfcourt"]["makes"] for d in bucket.values()),
                fg_pct=sum(d["halfcourt"]["makes"] for d in bucket.values())
                / (sum(d["halfcourt"]["attempts"] for d in bucket.values()) or 1),
                pps=round(
                    pts
                    * sum(d["halfcourt"]["makes"] for d in bucket.values())
                    / (sum(d["halfcourt"]["attempts"] for d in bucket.values()) or 1),
                    2,
                ),
            ),
        )

    return raw_totals, shot_summaries


# ─── Helper: collect all drill labels from practice stats ──────────────
def collect_practice_labels(stats_records):
    """Return the fixed set of drill labels used for filtering practice stats."""
    return [
        "TRANSITION SERIES",
        "ADVANTAGE DRILLS",
        "3V3 DRILLS",
        "4V4 DRILLS",
        "5V5 DRILLS",
    ]




@admin_bp.route('/player/<player_name>', methods=['GET', 'POST'])
@login_required
def player_detail(player_name):

    player = Roster.query.filter_by(player_name=player_name).first_or_404()

    current_season = Season.query.order_by(Season.start_date.desc()).first()
    development_plan = None
    if current_season:
        development_plan = (
            PlayerDevelopmentPlan.query
            .filter_by(player_name=player_name, season_id=current_season.id)
            .first()
        )

    # Rebuild shot_map/label_map to ensure Free Throws category exists
    local_shot_map = dict(shot_map)
    local_shot_map.setdefault('ft', ['Free Throws'])
    local_label_map = dict(label_map)
    local_label_map.setdefault('ft', 'Free Throws')

    # ─── Handle Skill‐Development form submission ───────────────────────
    if request.method == 'POST':
        if not (current_user.is_admin or current_user.player_name == player_name):
            flash('Only admins or the player may modify skill-development entries.', 'error')
            return redirect(
                url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment'
            )
        # 1) Try the “Skill Name / Value” form first
        shot_date   = date.fromisoformat(request.form.get('date'))
        skill_name  = request.form.get('skill_name', '').strip()
        value_str   = request.form.get('value', '').strip()

        if skill_name and value_str.isdigit():
            # Insert a generic SkillEntry (e.g. “Free Throws” or anything else)
            db.session.add(
                SkillEntry(
                    player_id   = player.id,
                    date        = shot_date,
                    skill_name  = skill_name,
                    value       = int(value_str),
                    shot_class  = None,
                    subcategory = None,
                    makes       = 0,
                    attempts    = 0
                )
            )
            db.session.commit()
            return redirect(
                url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment'
            )

        # 2) Otherwise, fall back to the drill‐by‐drill loop (including 'ft')
        shot_date = date.fromisoformat(request.form.get('date'))
        human     = {k: local_label_map[k] for k in local_shot_map}

        for cls, subs in local_shot_map.items():
            for sub in subs:
                key      = sub.replace(' ', '_')
                makes    = int(request.form.get(f"{cls}_{key}_makes", '0') or '0')
                attempts = int(request.form.get(f"{cls}_{key}_attempts", '0') or '0')

                if makes or attempts:
                    entry = SkillEntry(
                        player_id   = player.id,
                        date        = shot_date,
                        skill_name  = human[cls],
                        value       = attempts,
                        shot_class  = cls,
                        subcategory = sub,
                        makes       = makes,
                        attempts    = attempts
                    )
                    db.session.add(entry)

        db.session.commit()
        return redirect(
            url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment'
        )

    # ─── Read optional date‐range filters ────────────────────────────────
    start_date = request.args.get('start_date')
    end_date   = request.args.get('end_date')
    start_dt = None
    end_dt   = None
    if start_date:
        try:
            start_dt = date.fromisoformat(start_date)
        except ValueError:
            start_dt = None
    if end_date:
        try:
            end_dt = date.fromisoformat(end_date)
        except ValueError:
            end_dt = None

    # ─── Load & filter SkillEntry rows ─────────────────────────────────
    q = SkillEntry.query.filter_by(player_id=player.id)
    if start_dt:
        q = q.filter(SkillEntry.date >= start_dt)
    if end_dt:
        q = q.filter(SkillEntry.date <= end_dt)
    all_entries = q.order_by(SkillEntry.date.desc()).all()
    nba100_entries = [e for e in all_entries if e.skill_name == "NBA 100"]
    entries_list  = [e for e in all_entries if e.skill_name != "NBA 100"]

    # ─── Group by date & compute totals ─────────────────────────────────
    # We’ll pass `entries_list` straight to Jinja and do groupby('date') there.
    grouped = {}
    for e in entries_list:
        grouped.setdefault(e.date, []).append(e)

    # Build a nested dict for drill totals using SimpleNamespace
    shot_totals = {
        cls: {sub: SimpleNamespace(makes=0, attempts=0) for sub in subs}
        for cls, subs in local_shot_map.items()
    }
    for e in entries_list:
        if e.shot_class in shot_totals and e.subcategory in shot_totals[e.shot_class]:
            shot_totals[e.shot_class][e.subcategory].makes += e.makes
            shot_totals[e.shot_class][e.subcategory].attempts += e.attempts

    # Build a separate “generic_totals” for any entry where shot_class is None
    generic_totals = {}
    for e in entries_list:
        if not e.shot_class and e.skill_name:
            generic_totals[e.skill_name] = generic_totals.get(e.skill_name, 0) + e.value

    # ─── Fetch ALL stats for this player ────────────────────────────────
    all_stats_records = PlayerStats.query.filter_by(player_name=player_name).all()
    if start_dt or end_dt:
        filtered_records = []
        for rec in all_stats_records:
            keep = True
            if start_dt:
                if rec.practice_id:
                    pr = db.session.get(Practice, rec.practice_id)
                    if not (pr and pr.date >= start_dt):
                        keep = False
                elif rec.game_id:
                    gm = db.session.get(Game, rec.game_id)
                    if not (gm and gm.game_date >= start_dt):
                        keep = False
            if end_dt and keep:
                if rec.practice_id:
                    pr = db.session.get(Practice, rec.practice_id)
                    if not (pr and pr.date <= end_dt):
                        keep = False
                elif rec.game_id:
                    gm = db.session.get(Game, rec.game_id)
                    if not (gm and gm.game_date <= end_dt):
                        keep = False
            if keep:
                filtered_records.append(rec)
        all_stats_records = filtered_records
    has_stats = bool(all_stats_records)
    if not all_stats_records:
        flash("No stats found for this player.", "info")

    # ─── Split into Game vs Practice records ────────────────────────────
    game_stats_records     = [r for r in all_stats_records if r.game_id]
    practice_stats_records = [r for r in all_stats_records if r.practice_id]

   # ─── Read blanket‐tab mode (‘game’ or ‘practice’), but if no games exist, switch to practice ──
    requested_mode = request.args.get('mode', None)
    if requested_mode in ("game", "practice"):
        mode = requested_mode
    elif requested_mode == "development":
        mode = "development"
    else:
        # If the player has no game entries but does have practice entries,
        # force “practice” mode. Otherwise default to “game.”
        if game_stats_records:
            mode = "game"
        elif practice_stats_records:
            mode = "practice"
        else:
            mode = "game"

    # ─── Compute BOTH aggregations for Season Totals ───────────────────
    aggregated_game     = aggregate_stats(game_stats_records)
    aggregated_practice = aggregate_stats(practice_stats_records)

    label_options = collect_practice_labels(practice_stats_records)
    selected_labels = [
        lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}

    # ─── Direct PnR totals for this player ─────────────────────────────
    pnrs = PnRStats.query.filter_by(player_id=player.id).all()
    total_pnrs = len(pnrs)
    pnrs_as_bh = sum(1 for p in pnrs if p.role == 'BH')
    pnrs_as_screener = sum(1 for p in pnrs if p.role == 'Screener')
    adv_plus = sum(1 for p in pnrs if p.advantage_created == 'Adv+')
    direct = [p for p in pnrs if p.direct]
    direct_count = len(direct)
    direct_points = sum(p.points_scored or 0 for p in direct)
    direct_turnovers = sum(1 for p in direct if p.turnover_occurred)
    direct_assists = sum(1 for p in direct if p.assist_occurred)
    pct_adv_plus = adv_plus / total_pnrs if total_pnrs else 0
    direct_points_per = direct_points / direct_count if direct_count else 0
    pnr_totals = SimpleNamespace(
        total_pnrs=total_pnrs,
        pnrs_as_bh=pnrs_as_bh,
        pnrs_as_screener=pnrs_as_screener,
        pct_adv_plus=pct_adv_plus,
        direct_pnr_points_per=round(direct_points_per, 3) if direct_count else 0,
        direct_pnr_turnovers=direct_turnovers,
        direct_pnr_assists=direct_assists,
    )

    # ─── On-court offensive metrics (replicated from player_view) ──────────
    on_q = (
        db.session.query(
            func.count(PlayerPossession.id),
            func.coalesce(func.sum(Possession.points_scored), 0)
        )
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(
            PlayerPossession.player_id == player.id,
            Possession.possession_side == 'Offense'
        )
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        on_q = on_q.filter(or_(*clauses))
    ON_poss, ON_pts = on_q.one()

    team_q = (
        db.session.query(
            func.count(Possession.id),
            func.coalesce(func.sum(Possession.points_scored), 0)
        )
        .filter(Possession.possession_side == 'Offense')
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        team_q = team_q.filter(or_(*clauses))
    TEAM_poss, TEAM_pts = team_q.one()

    OFF_poss = TEAM_poss - ON_poss
    OFF_pts  = TEAM_pts - ON_pts

    PPP_ON  = ON_pts / ON_poss if ON_poss else 0
    PPP_OFF = OFF_pts / OFF_poss if OFF_poss else 0

    def count_event(ev_type):
        q = (
            db.session.query(func.count(ShotDetail.id))
            .join(Possession, ShotDetail.possession_id == Possession.id)
            .join(PlayerPossession, Possession.id == PlayerPossession.possession_id)
            .filter(
                PlayerPossession.player_id == player.id,
                Possession.possession_side == 'Offense',
                ShotDetail.event_type == ev_type,
            )
        )
        if label_set:
            clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
            q = q.filter(or_(*clauses))
        return q.scalar() or 0

    FGM2_ON = count_event('ATR+') + count_event('2FG+')
    FGM3_ON = count_event('3FG+')
    FGA_ON  = sum(count_event(e) for e in ['ATR+','ATR-','2FG+','2FG-','3FG+','3FG-'])
    EFG_ON  = (FGM2_ON + 1.5 * FGM3_ON) / FGA_ON if FGA_ON else 0
    ATR_pct = count_event('ATR+') / (count_event('ATR+') + count_event('ATR-')) if (count_event('ATR+') + count_event('ATR-')) else 0
    FG2_pct = count_event('2FG+') / (count_event('2FG+') + count_event('2FG-')) if (count_event('2FG+') + count_event('2FG-')) else 0
    FG3_pct = count_event('3FG+') / (count_event('3FG+') + count_event('3FG-')) if (count_event('3FG+') + count_event('3FG-')) else 0

    turnover_rate    = count_event('Turnover') / ON_poss if ON_poss else 0
    off_reb_rate     = count_event('Off Rebound') / ON_poss if ON_poss else 0
    fouls_drawn_rate = count_event('Fouled') / ON_poss if ON_poss else 0


    # ─── Compute blue‐collar via raw SQL (instead of get_blue_breakdown) ───
    zero_blue = SimpleNamespace(
        def_reb=0, off_reb=0, misc=0, deflection=0,
        steal=0, block=0, floor_dive=0,
        charge_taken=0, reb_tip=0, total_blue_collar=0
    )

    if game_stats_records:
        game_ids = [s.game_id for s in game_stats_records if s.game_id]
        bc_game = (
            db.session.query(
                func.coalesce(func.sum(BlueCollarStats.def_reb),      0).label('def_reb'),
                func.coalesce(func.sum(BlueCollarStats.off_reb),      0).label('off_reb'),
                func.coalesce(func.sum(BlueCollarStats.misc),         0).label('misc'),
                func.coalesce(func.sum(BlueCollarStats.deflection),   0).label('deflection'),
                func.coalesce(func.sum(BlueCollarStats.steal),        0).label('steal'),
                func.coalesce(func.sum(BlueCollarStats.block),        0).label('block'),
                func.coalesce(func.sum(BlueCollarStats.floor_dive),   0).label('floor_dive'),
                func.coalesce(func.sum(BlueCollarStats.charge_taken), 0).label('charge_taken'),
                func.coalesce(func.sum(BlueCollarStats.reb_tip),      0).label('reb_tip'),
                func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label('total_blue_collar'),
            )
            .filter(BlueCollarStats.player_id == player.id)
            .filter(BlueCollarStats.game_id.in_(game_ids))
            .one()
        )
        player_blue_breakdown_game = SimpleNamespace(
            def_reb           = bc_game.def_reb,
            off_reb           = bc_game.off_reb,
            misc              = bc_game.misc,
            deflection        = bc_game.deflection,
            steal             = bc_game.steal,
            block             = bc_game.block,
            floor_dive        = bc_game.floor_dive,
            charge_taken      = bc_game.charge_taken,
            reb_tip           = bc_game.reb_tip,
            total_blue_collar = bc_game.total_blue_collar
        )
    else:
        player_blue_breakdown_game = zero_blue

    if practice_stats_records:
        practice_ids = [s.practice_id for s in practice_stats_records if s.practice_id]
        bc_practice = (
            db.session.query(
                func.coalesce(func.sum(BlueCollarStats.def_reb),      0).label('def_reb'),
                func.coalesce(func.sum(BlueCollarStats.off_reb),      0).label('off_reb'),
                func.coalesce(func.sum(BlueCollarStats.misc),         0).label('misc'),
                func.coalesce(func.sum(BlueCollarStats.deflection),   0).label('deflection'),
                func.coalesce(func.sum(BlueCollarStats.steal),        0).label('steal'),
                func.coalesce(func.sum(BlueCollarStats.block),        0).label('block'),
                func.coalesce(func.sum(BlueCollarStats.floor_dive),   0).label('floor_dive'),
                func.coalesce(func.sum(BlueCollarStats.charge_taken), 0).label('charge_taken'),
                func.coalesce(func.sum(BlueCollarStats.reb_tip),      0).label('reb_tip'),
                func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label('total_blue_collar'),
            )
            .filter(BlueCollarStats.player_id == player.id)
            .filter(BlueCollarStats.practice_id.in_(practice_ids))
            .one()
        )
        player_blue_breakdown_practice = SimpleNamespace(
            def_reb           = bc_practice.def_reb,
            off_reb           = bc_practice.off_reb,
            misc              = bc_practice.misc,
            deflection        = bc_practice.deflection,
            steal             = bc_practice.steal,
            block             = bc_practice.block,
            floor_dive        = bc_practice.floor_dive,
            charge_taken      = bc_practice.charge_taken,
            reb_tip           = bc_practice.reb_tip,
            total_blue_collar = bc_practice.total_blue_collar
        )
    else:
        player_blue_breakdown_practice = zero_blue
    # ─── Now pick which “blue” to pass to the template ───
    if mode == "game":
        agg  = aggregated_game
        blue = player_blue_breakdown_game
    else:
        if label_set:
            agg  = compute_filtered_totals(practice_stats_records, label_set)
            blue = compute_filtered_blue(practice_stats_records, label_set)
        else:
            agg  = aggregated_practice
            blue = player_blue_breakdown_practice

    player_stats_map = agg.__dict__ if hasattr(agg, '__dict__') else dict(agg)




    # ─── Prepare Shot-Type Season Totals & Summaries ───────────────────
    stats_for_shot = game_stats_records if mode == 'game' else practice_stats_records


# … earlier in player_detail …

    # ─── Gather every shot‐detail JSON blob ─────────────────────
    all_details = []
    for rec in stats_for_shot:
        if rec.shot_type_details:
            js = (
                json.loads(rec.shot_type_details)
                if isinstance(rec.shot_type_details, str)
                else rec.shot_type_details
            )
            for shot in js:
                labels = {
                    lbl.strip().upper()
                    for lbl in re.split(r',', shot.get('possession_type', ''))
                    if lbl.strip()
                }
                labels.update(
                    lbl.strip().upper() for lbl in shot.get('drill_labels', []) if lbl.strip()
                )
                if label_set and not (labels & label_set):
                    continue
                all_details.append(shot)

    # ─── Compute raw season totals directly from all_details ─────────────────
    makes_atr  = sum(1 for shot in all_details if shot.get('shot_class','').lower() == 'atr' and shot.get('result') == 'made')
    att_atr    = sum(1 for shot in all_details if shot.get('shot_class','').lower() == 'atr')
    makes_fg2  = sum(1 for shot in all_details if shot.get('shot_class','').lower() == '2fg' and shot.get('result') == 'made')
    att_fg2    = sum(1 for shot in all_details if shot.get('shot_class','').lower() == '2fg')
    makes_fg3  = sum(1 for shot in all_details if shot.get('shot_class','').lower() == '3fg' and shot.get('result') == 'made')
    att_fg3    = sum(1 for shot in all_details if shot.get('shot_class','').lower() == '3fg')

    fg_pct_atr  = (makes_atr / att_atr * 100) if att_atr else 0
    fg_pct_fg2  = (makes_fg2 / att_fg2 * 100) if att_fg2 else 0
    fg_pct_fg3  = (makes_fg3 / att_fg3 * 100) if att_fg3 else 0

    total_att = att_atr + att_fg2 + att_fg3
    pps_atr = round((makes_atr * 2) / att_atr, 2) if att_atr else 0
    pps_fg2 = round((makes_fg2 * 2) / att_fg2, 2) if att_fg2 else 0
    pps_fg3 = round((makes_fg3 * 3) / att_fg3, 2) if att_fg3 else 0
    freq_atr = (att_atr / total_att * 100) if total_att else 0
    freq_fg2 = (att_fg2 / total_att * 100) if total_att else 0
    freq_fg3 = (att_fg3 / total_att * 100) if total_att else 0

    raw_season_totals = SimpleNamespace(
        atr  = SimpleNamespace(makes=makes_atr,  attempts=att_atr,  fg_pct=fg_pct_atr,
                              pps=pps_atr, freq=freq_atr),
        fg2  = SimpleNamespace(makes=makes_fg2,  attempts=att_fg2,  fg_pct=fg_pct_fg2,
                              pps=pps_fg2, freq=freq_fg2),
        fg3  = SimpleNamespace(makes=makes_fg3,  attempts=att_fg3,  fg_pct=fg_pct_fg3,
                              pps=pps_fg3, freq=freq_fg3)
    )

    # ─── Initialize counters ───────────────────────────────────────────────────
    detail_counts = {'atr': {}, 'fg2': {}, 'fg3': {}}
    cls_map       = {'atr':'atr','2fg':'fg2','3fg':'fg3'}

    # … continue with your detail_counts + shot_summaries logic …


    # ─── Populate detail_counts: one attempt per shot per distinct label ───────
    for shot in all_details:
        sc = shot.get('shot_class', '').lower()       # e.g. "2fg", "3fg", or "atr"
        shot_cls = cls_map.get(sc)                    # e.g. "fg2", "fg3", or "atr"

        if not shot_cls:
            continue

        made = (shot.get('result') == 'made')
        raw  = shot.get('possession_type', '').strip().lower()
        if 'trans' in raw:
            ctx = 'transition'
        elif 'half' in raw:
            ctx = 'halfcourt'
        else:
            ctx = 'total'

        # 1) Collect all labels for this shot
        labels_for_this_shot = []

        # a) Assisted vs Non-Assisted
        if shot.get('Assisted'):
            labels_for_this_shot.append('Assisted')
        else:
            labels_for_this_shot.append('Non-Assisted')

        # b) All HUDL suffix fields for this shot
        if sc in ('atr', '2fg'):
            # The parser stored all ATR & 2FG subfields under "2FG (...)" columns,
            # with prefix "2fg_" in JSON. So we look up keys under "2fg_*"
            suffix_keys = ["Type", "Defenders", "Dribble", "Feet", "Hands", "Other", "PA", "RA"]
            for suffix in suffix_keys:
                old_key = f"{sc}_{suffix.lower().replace(' ', '_')}"
                val = shot.get(old_key, "")
                if val:
                    sublabels = [lbl.strip() for lbl in re.split(r',', str(val)) if lbl.strip()]
                    labels_for_this_shot.extend(sublabels)
        else:  # sc == '3fg'
            suffix_keys = ["Contest", "Footwork", "Good/Bad", "Line", "Move", "Pocket", "Shrink", "Type"]
            for suffix in suffix_keys:
                old_key = f"{sc}_{suffix.lower().replace('/', '_').replace(' ', '_')}"
                val = shot.get(old_key, "")
                if val:
                    sublabels = [lbl.strip() for lbl in re.split(r',', str(val)) if lbl.strip()]
                    labels_for_this_shot.extend(sublabels)

        # ─── Now pull in every “_scheme_attack” / “_scheme_drive” / “_scheme_pass” tag ───────────
        #   e.g. "2fg_scheme_attack", "2fg_scheme_drive", "2fg_scheme_pass" or
        #   "3fg_scheme_attack", "3fg_scheme_drive", "3fg_scheme_pass"

        for scheme in ("scheme_attack", "scheme_drive", "scheme_pass"):
            old_key = f"{sc}_{scheme}"
            val = shot.get(old_key, "")
            if val:
                sublabels = [lbl.strip() for lbl in re.split(r',', str(val)) if lbl.strip()]
                labels_for_this_shot.extend(sublabels)


        # 2) Use a set() so each distinct label is counted once
        unique_labels = set(labels_for_this_shot)

        # 3) Increment each label exactly once
        for lbl in unique_labels:
            ent = detail_counts[shot_cls].setdefault(lbl, {
                'total':     {'attempts': 0, 'makes': 0},
                'transition':{'attempts': 0, 'makes': 0},
                'halfcourt': {'attempts': 0, 'makes': 0},
            })
            # Grand total: +1 attempt for this shot under that label
            ent['total']['attempts'] += 1
            if made:
                ent['total']['makes'] += 1

            # Context‐specific (e.g. transition or halfcourt)
            if ctx in ('transition','halfcourt'):
                ent[ctx]['attempts'] += 1
                if made:
                    ent[ctx]['makes'] += 1

    # ─── Compute fg_pct, pps & freq_pct ────────────────────────────────────────
    for shot_type, bucket in detail_counts.items():
        for data in bucket.values():
            total_att = data['total']['attempts'] or 1
            pts = 2 if shot_type in ('atr','fg2') else 3
            for ctx in ('total','transition','halfcourt'):
                a = data[ctx]['attempts']
                m = data[ctx]['makes']
                fg = (m / a) if a else 0
                data[ctx]['fg_pct']   = fg
                data[ctx]['pps']      = round(pts * fg, 2) if a else 0
                data[ctx]['freq_pct'] = a / total_att

    # ─── Build shot_summaries ────────────────────────────────────────────────
    shot_summaries = {}
    for shot_type, bucket in detail_counts.items():
        # (1) Ensure Assisted/Non-Assisted keys exist
        for lbl in ('Assisted','Non-Assisted'):
            bucket.setdefault(lbl, {
                'total':     {'attempts': 0,'makes': 0,'fg_pct':0,'pps':0,'freq_pct':0},
                'transition':{'attempts': 0,'makes': 0,'fg_pct':0,'pps':0,'freq_pct':0},
                'halfcourt': {'attempts': 0,'makes': 0,'fg_pct':0,'pps':0,'freq_pct':0},
            })

        # (2) Build a namespace for each label/category
        cats = {
            lbl: SimpleNamespace(
                total      = SimpleNamespace(**data['total']),
                transition = SimpleNamespace(**data['transition']),
                halfcourt  = SimpleNamespace(**data['halfcourt'])
            )
            for lbl, data in bucket.items()
        }

        # (3) Sum up totals and compute fg_pct, pps for this shot_type
        ta  = sum(d['total']['attempts'] for d in bucket.values()) or 1
        tm  = sum(d['total']['makes']    for d in bucket.values())
        pts = 2 if shot_type in ('atr','fg2') else 3

        shot_summaries[shot_type] = SimpleNamespace(
            total      = SimpleNamespace(
                attempts=ta,
                makes=tm,
                fg_pct=(tm / ta * 100),
                pps=round(pts * tm / ta, 2),
            ),
            cats       = cats,
            transition = SimpleNamespace(
                attempts = sum(d['transition']['attempts'] for d in bucket.values()),
                makes    = sum(d['transition']['makes']    for d in bucket.values()),
                fg_pct   = (
                    sum(d['transition']['makes'] for d in bucket.values()) /
                    (sum(d['transition']['attempts'] for d in bucket.values()) or 1)
                ),
                pps = round(
                    pts * sum(d['transition']['makes'] for d in bucket.values()) /
                    (sum(d['transition']['attempts'] for d in bucket.values()) or 1),
                    2,
                )
            ),
            halfcourt  = SimpleNamespace(
                attempts = sum(d['halfcourt']['attempts'] for d in bucket.values()),
                makes    = sum(d['halfcourt']['makes']    for d in bucket.values()),
                fg_pct   = (
                    sum(d['halfcourt']['makes'] for d in bucket.values()) /
                    (sum(d['halfcourt']['attempts'] for d in bucket.values()) or 1)
                ),
                pps = round(
                    pts * sum(d['halfcourt']['makes'] for d in bucket.values()) /
                    (sum(d['halfcourt']['attempts'] for d in bucket.values()) or 1),
                    2,
                )
            )
        )
    # ←─── this “for shot_type…” loop ends here



    # ─── Game‐by‐game breakdown for sub‐tab (recompute points) ──────────────
    game_breakdown = {}
    game_details   = {}

    for s in game_stats_records:
        gid = s.game_id

        js = []
        if s.shot_type_details:
            js = (
                json.loads(s.shot_type_details)
                if isinstance(s.shot_type_details, str)
                else s.shot_type_details
            )

        # count makes for each class
        made_atr  = sum(1 for shot in js if shot.get('shot_class','').lower() == 'atr' and shot.get('result') == 'made')
        made_fg2  = sum(1 for shot in js if shot.get('shot_class','').lower() == '2fg' and shot.get('result') == 'made')
        made_fg3  = sum(1 for shot in js if shot.get('shot_class','').lower() == '3fg' and shot.get('result') == 'made')

        # free throws made
        ft_made   = s.ftm or 0

        # TOTAL POINTS for this game:
        pts_for_game = (2 * made_atr) + (2 * made_fg2) + (3 * made_fg3) + ft_made

        # count attempts from JSON
        att_atr   = sum(1 for shot in js if shot.get('shot_class','').lower() == 'atr')
        att_fg2   = sum(1 for shot in js if shot.get('shot_class','').lower() == '2fg')
        att_fg3   = sum(1 for shot in js if shot.get('shot_class','').lower() == '3fg')

        # build the row
        game_breakdown[gid] = {
            "points":         pts_for_game,
            "assists":        s.assists or 0,
            "turnovers":      s.turnovers or 0,
            "pot_assists":    s.pot_assists or 0,
            "second_assists": s.second_assists or 0,

            "atr_makes":      made_atr,
            "atr_attempts":   att_atr,

            "fg2_makes":      made_fg2,
            "fg2_attempts":   att_fg2,

            "fg3_makes":      made_fg3,
            "fg3_attempts":   att_fg3,

            "ftm":            ft_made,
            "fta":            s.fta or 0
        }

        # date/opponent details remain unchanged
        g = s.game
        game_details[gid] = {
            "opponent_name": g.opponent_name if g else "Unknown",
            "game_date":     g.game_date.strftime("%b %d") if g and g.game_date else "",
            "sort_date":     g.game_date.strftime("%Y%m%d") if g and g.game_date else "0"
        }

    # ─── Practice-by-practice breakdown (recompute points) ───
    practice_breakdown = {}
    practice_details   = {}

    for s in practice_stats_records:
        pid = s.practice_id
        if label_set:
            row_totals = compute_filtered_totals([s], label_set)
        else:
            row_totals = aggregate_stats([s])
        practice_breakdown[pid] = {
            "points":         row_totals.points,
            "assists":        row_totals.assists,
            "turnovers":      row_totals.turnovers,
            "pot_assists":    row_totals.pot_assists,
            "second_assists": row_totals.second_assists,
            "atr_makes":      row_totals.atr_makes,
            "atr_attempts":   row_totals.atr_attempts,
            "fg2_makes":      row_totals.fg2_makes,
            "fg2_attempts":   row_totals.fg2_attempts,
            "fg3_makes":      row_totals.fg3_makes,
            "fg3_attempts":   row_totals.fg3_attempts,
            "ftm":            row_totals.ftm,
            "fta":            row_totals.fta
        }
        pr = s.practice
        practice_details[pid] = {
            "game_date":     pr.date.strftime("%b %d") if pr and pr.date else "",
            "opponent_name": pr.category if pr else "",
            "sort_date":     pr.date.strftime("%Y%m%d") if pr and pr.date else "0"
        }



    # ─── Finally, render template with BOTH modes & all context ─────────
    return render_template(
        'admin/player_detail.html',
        player_name                        = player_name,
        mode                               = mode,
        agg                                = agg,
        blue                               = blue,
        aggregated_game                    = aggregated_game,
        aggregated_practice                = aggregated_practice,
        player_blue_breakdown_game         = player_blue_breakdown_game,
        player_blue_breakdown_practice     = player_blue_breakdown_practice,
        game_stats_records                 = game_stats_records,
        practice_stats_records             = practice_stats_records,
        stats_records                      = game_stats_records if mode=='game' else practice_stats_records,

        # ─── Pass the flat list of all SkillEntry rows (so template can group by date) ───
        entries_list                       = entries_list,
        # ─── “Drill‐by‐drill” totals for shot_map (so template can show totals row) ───
        shot_totals                        = shot_totals,
        totals                             = shot_totals,
        # ── Pass the separate NBA 100 list to the template ────────────────
        nba100_entries                     = nba100_entries,

        shot_map                           = local_shot_map,
        label_map                          = local_label_map,
        generic_totals                     = generic_totals,   # e.g. {"Free Throws":123}

        # ── all your existing context for stats, shot summaries, etc. ─────────
        start_date                         = start_date or '',
        end_date                           = end_date   or '',
        shot_type_totals                   = raw_season_totals,
        shot_summaries                     = shot_summaries,
        game_breakdown                     = game_breakdown,
        game_details                       = game_details,
        practice_breakdown                 = practice_breakdown,
        practice_details                   = practice_details,
        player                             = player,
        has_stats                          = has_stats,
        label_options                      = label_options,
        selected_labels                    = selected_labels,
        pnr_totals                         = pnr_totals,
        development_plan                   = development_plan,
        player_stats                       = player_stats_map,

        offensive_possessions              = ON_poss,
        ppp_on                             = round(PPP_ON,2),
        ppp_off                            = round(PPP_OFF,2),
        efg_on                             = round(EFG_ON*100,1),
        atr_pct                            = round(ATR_pct*100,1),
        two_fg_pct                         = round(FG2_pct*100,1),
        three_fg_pct                       = round(FG3_pct*100,1),
        turnover_rate                      = round(turnover_rate*100,1),
        off_reb_rate                       = round(off_reb_rate*100,1),
        fouls_drawn_rate                   = round(fouls_drawn_rate*100,1)
    )





# ... [remaining routes unchanged below] ...


#─ Delete all entries for a given date ───────────────────────────
@admin_bp.route(
    '/admin/player/<player_name>/skill-entry/<entry_date>/delete',
    methods=['POST']
)
@login_required
def delete_skill_entry(player_name, entry_date):
    if not (current_user.is_admin or current_user.player_name == player_name):
        abort(403)
    # parse the incoming date
    target_date = date.fromisoformat(entry_date)
    # delete every SkillEntry for that player on that date
    SkillEntry.query.filter_by(player_id=Roster.query.filter_by(player_name=player_name).first_or_404().id,
                                date=target_date
                               ).delete(synchronize_session=False)
    db.session.commit()
    flash('All skill‐development entries deleted for that date.', 'success')
    return redirect(
        url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment'
    )


# ─── Edit all entries for a given date ─────────────────────────────
@admin_bp.route(
    '/admin/player/<player_name>/skill-entry/<entry_date>/edit',
    methods=['GET', 'POST']
)
@login_required
def edit_skill_entry(player_name, entry_date):
    if not (current_user.is_admin or current_user.player_name == player_name):
        abort(403)
    """Edit all skill-development entries for a given date."""

    # Parse the date and load the roster & any existing entries
    target_date = date.fromisoformat(entry_date)
    roster = Roster.query.filter_by(player_name=player_name).first_or_404()
    entries = SkillEntry.query.filter_by(
        player_id=roster.id,
        date=target_date
    ).all()

    # Ensure shot_map includes Free Throws
    local_shot_map = dict(shot_map)
    local_shot_map.setdefault('ft', ['Free Throws'])
    local_label_map = dict(label_map)
    local_label_map.setdefault('ft', 'Free Throws')

    # If there are no entries at all for that date, flash & redirect
    if not entries:
        flash('No entries found for that date.', 'error')
        return redirect(
            url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment'
        )

    if request.method == 'POST':
        # Loop through every (shot_class, subcategory) in shot_map,
        # creating or updating a SkillEntry accordingly.
        for cls, subs in local_shot_map.items():
            for sub in subs:
                field_key = sub.replace(' ', '_')
                makes    = int(request.form.get(f"{cls}_{field_key}_makes", '0') or '0')
                attempts = int(request.form.get(f"{cls}_{field_key}_attempts", '0') or '0')

                existing = SkillEntry.query.filter_by(
                    player_id   = roster.id,
                    date        = target_date,
                    shot_class  = cls,
                    subcategory = sub
                ).first()

                if existing:
                    existing.makes    = makes
                    existing.attempts = attempts
                else:
                    new_entry = SkillEntry(
                        player_id   = roster.id,
                        date        = target_date,
                        shot_class  = cls,
                        subcategory = sub,
                        makes       = makes,
                        attempts    = attempts
                    )
                    db.session.add(new_entry)

        db.session.commit()
        flash('Skill‐development entries updated.', 'success')
        return redirect(
            url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment'
        )

    # GET: render the edit form, passing everything the template needs
    return render_template(
        'admin/edit_skill_entry.html',
        player_name=player_name,
        entries=entries,
        entry_date=entry_date,
        shot_map=local_shot_map,
        label_map=local_label_map
    )


@admin_bp.route(
    '/admin/player/<player_name>/nba100',
    methods=['POST']
)
@login_required
@admin_required
def add_nba100_entry(player_name):
    """
    Handle the NBA 100 form:
      - Reads date and makes (0–100) from request.form
      - Creates a SkillEntry(skill_name="NBA 100", value=makes)
      - Redirects back to the Skill Development tab
    """
    from datetime import date

    # 1) Look up the player
    roster = Roster.query.filter_by(player_name=player_name).first_or_404()

    # 2) Get form data
    form_date = request.form.get('date')
    makes_str = request.form.get('makes', '0')

    # 3) Validate the date
    try:
        target_date = date.fromisoformat(form_date)
    except (TypeError, ValueError):
        flash('Invalid date for NBA 100 entry.', 'error')
        return redirect(url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment')

    # 4) Validate “makes” is int between 0 and 100
    try:
        makes = int(makes_str)
        if makes < 0 or makes > 100:
            raise ValueError()
    except ValueError:
        flash('“Makes” must be an integer between 0 and 100.', 'error')
        return redirect(url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment')

    # 5) Insert a new SkillEntry with skill_name="NBA 100"
    new_entry = SkillEntry(
        player_id   = roster.id,
        date        = target_date,
        skill_name  = "NBA 100",
        value       = makes,
        shot_class  = None,
        subcategory = None,
        makes       = 0,
        attempts    = 0
    )
    db.session.add(new_entry)
    db.session.commit()

    flash(f'NBA 100 entry saved: {makes}/100 on {target_date.isoformat()}.', 'success')
    return redirect(url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment')


@admin_bp.route('/player/<player_name>/nba100/<int:entry_id>/delete', methods=['POST'])
@login_required
@admin_required
def delete_nba100_entry(player_name, entry_id):
    """Delete a single NBA 100 entry."""
    roster = Roster.query.filter_by(player_name=player_name).first_or_404()
    entry = (
        SkillEntry.query
        .filter_by(id=entry_id, player_id=roster.id, skill_name="NBA 100")
        .first_or_404()
    )
    db.session.delete(entry)
    db.session.commit()
    flash('NBA 100 entry deleted.', 'success')
    return redirect(url_for('admin.player_detail', player_name=player_name) + '#skillDevelopment')


@admin_bp.route('/player/<player_name>/skill', methods=['GET', 'POST'])
@login_required
def player_skill(player_name):
    """View and manage generic skill-development entries for a player."""
    player = Roster.query.filter_by(player_name=player_name).first_or_404()

    if request.method == 'POST':
        if not current_user.is_admin:
            flash('Only admins may modify skill-development entries.', 'error')
            return redirect(url_for('admin.player_skill', player_name=player_name))

        shot_date = date.fromisoformat(request.form.get('date'))
        skill_name = request.form.get('skill_name', '').strip()
        value_str = request.form.get('value', '').strip()

        if skill_name and value_str.isdigit():
            db.session.add(
                SkillEntry(
                    player_id=player.id,
                    date=shot_date,
                    skill_name=skill_name,
                    value=int(value_str),
                    shot_class=None,
                    subcategory=None,
                    makes=0,
                    attempts=0,
                )
            )
            db.session.commit()
        return redirect(url_for('admin.player_skill', player_name=player_name))

    q = SkillEntry.query.filter_by(player_id=player.id).order_by(SkillEntry.date.desc())
    entries = [e for e in q.all() if not e.shot_class and e.skill_name != 'NBA 100']

    totals = {}
    for e in entries:
        totals[e.skill_name] = totals.get(e.skill_name, 0) + e.value

    return render_template(
        'admin/player_skill.html',
        player=player,
        entries=entries,
        totals=totals,
    )


@admin_bp.route('/admin/player_development/<player_name>', methods=['GET', 'POST'])
@login_required
@admin_required
def player_development(player_name):
    """Create or edit a player's development plan for the current season."""
    current_season = Season.query.order_by(Season.start_date.desc()).first()
    if not current_season:
        flash('No seasons found.', 'error')
        return redirect(url_for('admin.player_detail', player_name=player_name))

    plan = (
        PlayerDevelopmentPlan.query
        .filter_by(player_name=player_name, season_id=current_season.id)
        .first()
    )

    if request.method == 'POST':
        if not plan:
            plan = PlayerDevelopmentPlan(
                player_name=player_name,
                season_id=current_season.id,
            )
            db.session.add(plan)

        plan.stat_1_name = request.form.get('stat_1_name') or None
        plan.stat_1_goal = request.form.get('stat_1_goal') or None
        plan.stat_2_name = request.form.get('stat_2_name') or None
        plan.stat_2_goal = request.form.get('stat_2_goal') or None
        plan.stat_3_name = request.form.get('stat_3_name') or None
        plan.stat_3_goal = request.form.get('stat_3_goal') or None
        plan.note_1 = request.form.get('note_1') or None
        plan.note_2 = request.form.get('note_2') or None
        plan.note_3 = request.form.get('note_3') or None

        db.session.commit()
        flash('Development plan saved.', 'success')
        return redirect(url_for('admin.player_detail', player_name=player_name))

    if not plan:
        plan = PlayerDevelopmentPlan(player_name=player_name, season_id=current_season.id)

    return render_template('admin/player_development.html', plan=plan, player_name=player_name)


@admin_bp.route('/player/<player_name>/development')
@login_required
def player_development_view(player_name):
    """Display a player's development plan with current season stats."""
    player = Roster.query.filter_by(player_name=player_name).first_or_404()
    current_season = Season.query.order_by(Season.start_date.desc()).first()

    plan = None
    if current_season:
        plan = (
            PlayerDevelopmentPlan.query
            .filter_by(player_name=player_name, season_id=current_season.id)
            .first()
        )
        stats_records = (
            PlayerStats.query
            .filter_by(player_name=player_name, season_id=current_season.id)
            .all()
        )
    else:
        stats_records = []

    agg = aggregate_stats(stats_records)
    player_stats_map = agg.__dict__ if hasattr(agg, '__dict__') else dict(agg)

    return render_template(
        'admin/player_development_view.html',
        player_name=player_name,
        plan=plan,
        player_stats=player_stats_map,
        player=player,
    )


@admin_bp.route('/player/<player_name>/headshot', methods=['GET', 'POST'])
@login_required
@admin_required
def upload_headshot(player_name):
    """Upload a headshot image for a player."""
    roster_entry = Roster.query.filter_by(player_name=player_name).first_or_404()
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)

        filename = secure_filename(file.filename)
        if not allowed_image(filename):
            flash('Please upload a .png or .jpg image.', 'error')
            return redirect(request.url)

        folder = os.path.join(current_app.static_folder, 'headshots')
        os.makedirs(folder, exist_ok=True)
        file.save(os.path.join(folder, filename))

        roster_entry.headshot_filename = filename
        db.session.commit()
        flash('Headshot uploaded.', 'success')
        return redirect(url_for('admin.player_detail', player_name=player_name))

    return render_template('admin/upload_headshot.html', player=roster_entry)



@admin_bp.route('/roster', methods=['GET', 'POST'])
@admin_required
def roster():
    # 1) Load seasons
    seasons = Season.query.order_by(Season.start_date.desc()).all()
    if not seasons:
        flash("Please create at least one season first.", "warning")
        return redirect(url_for('admin.create_season'))

    # 2) Which season is selected?
    selected_id = request.args.get('season_id', type=int) or seasons[0].id

    # 3) Handle new roster entry
    if request.method == 'POST':
        name = request.form['player_name'].strip()
        if name:
            db.session.add(Roster(season_id=selected_id, player_name=name))
            db.session.commit()
            flash(f"Added {name} to {db.session.get(Season, selected_id).season_name}.", "success")
        return redirect(url_for('admin.roster', season_id=selected_id))

    # 4) Fetch only this season’s roster
    roster_entries = Roster.query \
                          .filter_by(season_id=selected_id) \
                          .order_by(Roster.player_name) \
                          .all()

    return render_template(
        'admin/roster.html',
        seasons=seasons,
        selected_season=selected_id,
        roster_entries=roster_entries
    )


@admin_bp.route('/season/create', methods=['GET', 'POST'])
@admin_required
def create_season():
    if request.method == 'POST':
        name = request.form.get('season_name', '').strip()
        if not name:
            flash("Season name can't be blank.", "error")
            return redirect(url_for('admin.create_season'))

        new_season = Season(season_name=name)
        db.session.add(new_season)
        db.session.commit()

        flash(f"Season '{name}' created!", "success")
        return redirect(url_for('admin.roster', season_id=new_season.id))

    return render_template("admin/create_season.html")




@admin_bp.route('/roster/delete/<int:id>', methods=['POST'])
@login_required
def delete_roster(id):
    entry = Roster.query.get_or_404(id)
    season_id = entry.season_id
    db.session.delete(entry)
    db.session.commit()
    flash(f"Removed {entry.player_name} from roster.", "success")
    return redirect(url_for('admin.roster', season_id=season_id))


@admin_bp.route('/sessions', methods=['GET', 'POST'])
@login_required
@admin_required
def sessions():
    season_id = request.args.get('season_id', type=int) or Season.query.order_by(Season.start_date.desc()).first().id
    seasons = Season.query.order_by(Season.start_date.desc()).all()
    selected = Season.query.get_or_404(season_id)

    if request.method == 'POST':
        name = request.form['name']
        start = datetime_module.date.fromisoformat(request.form['start_date'])
        end = datetime_module.date.fromisoformat(request.form['end_date'])
        new = Session(name=name, start_date=start, end_date=end, season_id=season_id)
        db.session.add(new)
        db.session.commit()
        flash(f'Session "{name}" added.', 'success')
        return redirect(url_for('admin.sessions', season_id=season_id))

    sessions = Session.query.filter_by(season_id=season_id).order_by(Session.start_date).all()
    return render_template('admin/sessions.html', seasons=seasons, selected=selected, sessions=sessions)


@admin_bp.route('/session/<int:id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_session(id):
    sess = Session.query.get_or_404(id)
    if request.method == 'POST':
        sess.name = request.form['name']
        sess.start_date = request.form['start_date']
        sess.end_date = request.form['end_date']
        db.session.commit()
        flash(f'Session "{sess.name}" updated.', 'success')
        return redirect(url_for('admin.sessions', season_id=sess.season_id))
    seasons = Season.query.order_by(Season.start_date.desc()).all()
    return render_template('admin/edit_session.html', sess=sess, seasons=seasons)


@admin_bp.route('/session/delete/<int:id>', methods=['POST'])
@login_required
@admin_required
def delete_session(id):
    sess = Session.query.get_or_404(id)
    season_id = sess.season_id
    db.session.delete(sess)
    db.session.commit()
    flash(f'Session "{sess.name}" deleted.', 'warning')
    return redirect(url_for('admin.sessions', season_id=season_id))

@admin_bp.context_processor
def inject_seasons():
    # grab all seasons, most‐recent first
    seasons = Season.query.order_by(Season.start_date.desc()).all()
    # read ?season_id= or fall back to the first in the list
    selected = request.args.get('season_id', type=int) or (seasons[0].id if seasons else None)
    return {
        'all_seasons':    seasons,
        'selected_season': selected
    }


@admin_bp.context_processor
def inject_last_stats_update():
    """Provide the file_date of the last parsed file."""
    last_file = (
        UploadedFile.query
        .filter(UploadedFile.last_parsed.isnot(None))
        .order_by(UploadedFile.last_parsed.desc())
        .first()
    )
    if last_file and last_file.file_date:
        def ordinal(n):
            return "%d%s" % (
                n,
                "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"),
            )
        fd = last_file.file_date
        formatted = f"Through {fd.strftime('%B')} {ordinal(fd.day)}"
    else:
        formatted = "Never"
    return {"last_stats_update": formatted}





@admin_bp.route('/players')
@login_required
def players_list():
    season_id = request.args.get('season_id', type=int)
    seasons = Season.query.order_by(Season.id.desc()).all()
    
    if not season_id and seasons:
        season_id = seasons[0].id

    import re
    def sort_key(name):
        m = re.match(r'#(\d+)', name)
        return int(m.group(1)) if m else 9999

    if season_id:
        roster_entries = Roster.query.filter_by(season_id=season_id).all()
        players = [r.player_name for r in roster_entries]
    else:
        players = [p[0] for p in db.session.query(PlayerStats.player_name).distinct().all()]

    players.sort(key=sort_key)
        
    return render_template('admin/players.html',
                           players=players,
                           seasons=seasons,
                           selected_season=season_id,
                           active_page='players')


@admin_bp.route('/player/<player_name>/shot-type')
@login_required
def player_shot_type(player_name):
    """
    Render the per–player shot type breakdown page.
    """
    # Fetch all the PlayerStats rows so we can reassemble their shot_type_details
    stats_records = PlayerStats.query.filter_by(player_name=player_name).all()
    if not stats_records:
        flash("No stats found for this player.", "error")
        return redirect(url_for('admin.players_list'))

    # Collect every shot_type_details entry across every game
    # Build shot_details using only the “game” or “practice” subset (stats_for_shot)
    shot_details = []
    for s in stats_for_shot:
        if s.shot_type_details:
            shot_details.extend(json.loads(s.shot_type_details))


    # Now you can group or filter shot_details however you like:
    atr_details  = [d for d in shot_details if d.get('shot_class') == 'ATR']
    fg2_details  = [d for d in shot_details if d.get('shot_class') == '2FG']
    fg3_details  = [d for d in shot_details if d.get('shot_class') == '3FG']

    return render_template(
        'admin/player_shot_type.html',
        player_name=player_name,
        atr_details=atr_details,
        fg2_details=fg2_details,
        fg3_details=fg3_details,
    )


@admin_bp.route('/player/<player_name>/sessions', methods=['GET'])
@login_required
@admin_required
def player_session_report(player_name):
    """Compare a player's first two sessions using canonical helpers."""
    player = Roster.query.filter_by(player_name=player_name).first()
    if not player:
        abort(404, description=f'Player {player_name} not found')

    sessions = (
        Session.query.filter_by(season_id=player.season_id)
        .order_by(Session.start_date)
        .all()
    )

    labels = request.args.getlist('labels')
    if len(labels) == 1 and ',' in labels[0]:
        labels = [l.strip() for l in labels[0].split(',') if l.strip()]

    all_records = (
        PlayerStats.query
        .filter_by(player_name=player_name, season_id=player.season_id)
        .all()
    )

    def normalize(stats_dict):
        mapping = {
            'assist_turnover_ratio': 'ast_to_to_ratio',
            'adj_assist_turnover_ratio': 'adj_ast_to_to',
            'steal': 'steals',
            'block': 'blocks',
            'deflection': 'deflections',
            'reb_tip': 'reb_tips',
            'charge_taken': 'charges',
            'floor_dive': 'floor_dives',
            'total_blue_collar': 'blue_collar_points',
        }
        out = {}
        for k, v in stats_dict.items():
            out[mapping.get(k, k)] = v
        return out

    def collect_labels(records):
        lbls = set()
        for r in records:
            for blob in [r.shot_type_details, r.stat_details]:
                if not blob:
                    continue
                data = json.loads(blob) if isinstance(blob, str) else blob
                for item in data:
                    for lbl in item.get('drill_labels', []):
                        if isinstance(lbl, str) and lbl.strip():
                            lbls.add(lbl.strip().upper())
        return lbls

    all_labels = collect_labels(all_records)

    for sess in sessions:
        sess.stats = get_player_stats_for_date_range(
            player.id,
            sess.start_date,
            sess.end_date,
            labels=labels
        ).__dict__
        # Merge in per-session on-court metrics, including Off Reb % and Fouls Drawn %
        on_court = get_on_court_metrics(
            player.id,
            start_date=sess.start_date,
            end_date=sess.end_date,
            labels=labels
        )
        sess.stats.update(on_court)
        sess.stats = normalize(sess.stats)

    overall_stats = normalize(
        get_player_overall_stats(player.id, labels=labels).__dict__
    )

    lower_better = {'team_turnover_rate_on', 'indiv_turnover_rate'}

    def compute_improved_flag(key, v1, v2):
        if v1 is None or v2 is None or key == 'offensive_poss_on':
            return None
        if key in lower_better:
            return v2 < v1
        return v2 > v1

    stats_keys = [
      ('efg_pct', 'Effective FG%'),
      ('points_per_shot', 'PPS'),
      ('atr_pct', 'ATR%'),
      ('atr_freq_pct', 'ATR Freq%'),
      ('two_fg_pct', '2FG%'),
      ('two_fg_freq_pct', '2FG Freq%'),
      ('three_fg_pct', '3FG%'),
      ('three_fg_freq_pct', '3FG Freq%'),
      ('ast_to_to_ratio', 'AST/TO'),
      ('adj_ast_to_to', 'Adj AST/TO'),
      ('offensive_poss_on', 'Team Poss'),
      ('ppp_on', 'PPP On'),
      ('team_turnover_rate_on', 'TO Rate'),
      ('indiv_turnover_rate', 'Ind TO%'),
      ('ind_off_reb_pct', 'Ind Off Reb%'),
      ('ind_fouls_drawn_pct', 'Ind Fouls Drawn%'),
      ('steals', 'Steals'),
      ('blocks', 'Blocks'),
      ('deflections', 'Deflections'),
      ('off_reb', 'Offensive Rebs'),
      ('def_reb', 'Defensive Rebs'),
      ('reb_tips', 'Rebound Tips'),
      ('charges', 'Charges Taken'),
      ('floor_dives', 'Floor Dives'),
      ('blue_collar_points', 'Blue-Collar Total'),
    ]

    display_stats = []
    for key, label in stats_keys:
        v1 = sessions[0].stats.get(key) if len(sessions) >= 1 else None
        v2 = sessions[1].stats.get(key) if len(sessions) >= 2 else None
        ov = overall_stats.get(key)
        if ov is None:
            continue
        improved = compute_improved_flag(key, v1, v2)
        display_stats.append({
            'key': key,
            'label': label,
            'session_values': [v1, v2],
            'overall_value': ov,
            'improved': improved,
        })

    return render_template(
        'admin/player_session_report.html',
        player_name=player_name,
        sessions=sessions,
        overall_stats=overall_stats,
        display_stats=display_stats,
        all_labels=sorted(all_labels),
        labels=labels,
    )



@admin_bp.route('/skill_totals')
@login_required
def skill_totals():
    """Display total skill-development shot counts for each player."""
    season_id = request.args.get('season_id', type=int)
    seasons = Season.query.order_by(Season.id.desc()).all()
    if not season_id and seasons:
        season_id = seasons[0].id
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if start_date:
        start_date = date.fromisoformat(start_date)
    if end_date:
        end_date = date.fromisoformat(end_date)

    if season_id:
        roster_entries = Roster.query.filter_by(season_id=season_id).all()
    else:
        roster_entries = Roster.query.all()

    def sort_key(name):
        m = re.match(r'#(\d+)', name)
        return int(m.group(1)) if m else 9999

    summary = []
    for r in sorted(roster_entries, key=lambda x: sort_key(x.player_name)):
        totals = {cls: {sub: {'makes': 0, 'attempts': 0} for sub in subs} for cls, subs in shot_map.items()}
        total_shots = 0
        q = SkillEntry.query.filter_by(player_id=r.id)
        if start_date:
            q = q.filter(SkillEntry.date >= start_date)
        if end_date:
            q = q.filter(SkillEntry.date <= end_date)
        for e in q.all():
            if e.shot_class in totals and e.subcategory in totals[e.shot_class]:
                t = totals[e.shot_class][e.subcategory]
                t['makes'] += e.makes
                t['attempts'] += e.attempts
                total_shots += e.attempts
        summary.append({'player_name': r.player_name, 'totals': totals, 'total_shots': total_shots})

    return render_template(
        'admin/skill_totals.html',
        players_summary=summary,
        seasons=seasons,
        selected_season=season_id,
        start_date=start_date.isoformat() if start_date else '',
        end_date=end_date.isoformat() if end_date else '',
        shot_map=shot_map,
        label_map=label_map,
        active_page='skill_totals'
    )


@admin_bp.route('/nba100_scores')
@login_required
def nba100_scores():
    """Display NBA 100 scores for all players on a specific date or each player's best."""
    best = request.args.get('best')
    date_str = request.args.get('date')
    target_date = None

    if not best and date_str:
        try:
            target_date = date.fromisoformat(date_str)
        except ValueError:
            target_date = None

    if best:
        ranked = (
            db.session.query(
                SkillEntry.player_id,
                SkillEntry.value,
                SkillEntry.date,
                func.row_number().over(
                    partition_by=SkillEntry.player_id,
                    order_by=(SkillEntry.value.desc(), SkillEntry.date.asc())
                ).label("rnk"),
            )
            .filter(SkillEntry.skill_name == 'NBA 100')
        ).subquery()
        scores_q = (
            db.session.query(Roster.player_name, ranked.c.value, ranked.c.date)
            .join(ranked, Roster.id == ranked.c.player_id)
            .filter(ranked.c.rnk == 1)
            .order_by(Roster.player_name)
        )
    else:
        scores_q = (
            db.session.query(Roster.player_name, SkillEntry.value)
            .join(Roster, SkillEntry.player_id == Roster.id)
            .filter(SkillEntry.skill_name == 'NBA 100')
        )
        if target_date:
            scores_q = scores_q.filter(SkillEntry.date == target_date)
        scores_q = scores_q.order_by(Roster.player_name)

    scores = scores_q.all()
    player_names = [s[0] for s in scores]
    player_scores = [s[1] for s in scores]

    return render_template(
        'admin/nba100_scores.html',
        target_date=date_str or '',
        player_names=player_names,
        player_scores=player_scores,
        best=bool(best),
        active_page='skill_totals'
    )


@admin_bp.route('/team_totals')
@login_required
def team_totals():
    """Aggregate all PlayerStats for a given season."""
    season_id = request.args.get('season_id', type=int)
    seasons = Season.query.order_by(Season.id.desc()).all()
    if not season_id and seasons:
        season_id = seasons[0].id
    practice_categories = [
        r[0] for r in db.session.query(Practice.category).distinct().order_by(Practice.category).all()
    ]

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    start_dt = end_dt = None
    if start_date:
        try:
            start_dt = date.fromisoformat(start_date)
        except ValueError:
            start_date = ''
    if end_date:
        try:
            end_dt = date.fromisoformat(end_date)
        except ValueError:
            end_date = ''

    trend_season_id = request.args.get('trend_season_id', type=int) or season_id
    trend_start_date = request.args.get('trend_start_date', start_date)
    trend_end_date = request.args.get('trend_end_date', end_date)
    trend_window = request.args.get('trend_window', type=int)
    trend_selected_categories = request.args.getlist('trend_category')
    trend_start_dt = trend_end_dt = None
    if trend_start_date:
        try:
            trend_start_dt = date.fromisoformat(trend_start_date)
        except ValueError:
            trend_start_date = ''
    if trend_end_date:
        try:
            trend_end_dt = date.fromisoformat(trend_end_date)
        except ValueError:
            trend_end_date = ''

    last_n = request.args.get('last', type=int)
    if last_n:
        dates = (
            Practice.query.filter_by(season_id=season_id)
            .order_by(Practice.date.desc())
            .limit(last_n)
            .with_entities(Practice.date)
            .all()
        )
        if dates:
            start_dt = dates[-1].date
            start_date = start_dt.isoformat()

    q = PlayerStats.query.filter(PlayerStats.practice_id != None)
    if season_id:
        q = q.filter_by(season_id=season_id)
    if start_dt or end_dt:
        q = q.join(Practice, PlayerStats.practice_id == Practice.id)
        if start_dt:
            q = q.filter(Practice.date >= start_dt)
        if end_dt:
            q = q.filter(Practice.date <= end_dt)
    stats_list = q.all()

    label_options = collect_practice_labels(stats_list)
    selected_labels = [
        lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}

    trend_selected_labels = [
        lbl for lbl in request.args.getlist('trend_label') if lbl.upper() in label_options
    ]
    trend_label_set = {lbl.upper() for lbl in trend_selected_labels}

    if label_set:
        totals = compute_filtered_totals(stats_list, label_set)
        blue_totals = compute_filtered_blue(stats_list, label_set)
    else:
        totals = aggregate_stats(stats_list)

        bc_query = db.session.query(
            func.coalesce(func.sum(BlueCollarStats.def_reb), 0).label('def_reb'),
            func.coalesce(func.sum(BlueCollarStats.off_reb), 0).label('off_reb'),
            func.coalesce(func.sum(BlueCollarStats.misc), 0).label('misc'),
            func.coalesce(func.sum(BlueCollarStats.deflection), 0).label('deflection'),
            func.coalesce(func.sum(BlueCollarStats.steal), 0).label('steal'),
            func.coalesce(func.sum(BlueCollarStats.block), 0).label('block'),
            func.coalesce(func.sum(BlueCollarStats.floor_dive), 0).label('floor_dive'),
            func.coalesce(func.sum(BlueCollarStats.charge_taken), 0).label('charge_taken'),
            func.coalesce(func.sum(BlueCollarStats.reb_tip), 0).label('reb_tip'),
            func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label('total_blue_collar'),
        ).filter(BlueCollarStats.practice_id != None)
        if season_id:
            bc_query = bc_query.filter(BlueCollarStats.season_id == season_id)
        if start_dt or end_dt:
            bc_query = bc_query.join(Practice, BlueCollarStats.practice_id == Practice.id)
            if start_dt:
                bc_query = bc_query.filter(Practice.date >= start_dt)
            if end_dt:
                bc_query = bc_query.filter(Practice.date <= end_dt)
        bc = bc_query.one()
        blue_totals = SimpleNamespace(
            def_reb=bc.def_reb,
            off_reb=bc.off_reb,
            misc=bc.misc,
            deflection=bc.deflection,
            steal=bc.steal,
            block=bc.block,
            floor_dive=bc.floor_dive,
            charge_taken=bc.charge_taken,
            reb_tip=bc.reb_tip,
            total_blue_collar=bc.total_blue_collar,
        )

    pt_query = db.session.query(
        func.coalesce(Possession.paint_touches, '').label('pt'),
        func.coalesce(func.sum(Possession.points_scored), 0).label('points'),
        func.count(Possession.id).label('poss'),
    ).filter(Possession.practice_id != None)
    if season_id:
        pt_query = pt_query.filter(Possession.season_id == season_id)
    if start_dt or end_dt:
        pt_query = pt_query.join(Practice, Possession.practice_id == Practice.id)
        if start_dt:
            pt_query = pt_query.filter(Practice.date >= start_dt)
        if end_dt:
            pt_query = pt_query.filter(Practice.date <= end_dt)
    pt_rows = pt_query.group_by(Possession.paint_touches).all()
    buckets = {0: {'pts': 0, 'poss': 0}, 1: {'pts': 0, 'poss': 0}, 2: {'pts': 0, 'poss': 0}, 3: {'pts': 0, 'poss': 0}}
    for r in pt_rows:
        try:
            val = int(float(str(r.pt).strip() or '0'))
        except ValueError:
            continue
        key = 3 if val >= 3 else val
        buckets[key]['pts'] += r.points
        buckets[key]['poss'] += r.poss
    paint_ppp = SimpleNamespace(
        zero=round(buckets[0]['pts'] / buckets[0]['poss'], 2) if buckets[0]['poss'] else 0.0,
        one=round(buckets[1]['pts'] / buckets[1]['poss'], 2) if buckets[1]['poss'] else 0.0,
        two=round(buckets[2]['pts'] / buckets[2]['poss'], 2) if buckets[2]['poss'] else 0.0,
        three=round(buckets[3]['pts'] / buckets[3]['poss'], 2) if buckets[3]['poss'] else 0.0,
    )

    shot_type_totals, shot_summaries = compute_team_shot_details(stats_list, label_set)

    # ─── Build trend data by date ───────────────────────────────────────────
    # Trend graph aggregates all players; player filters removed

    allowed_stats = {
        'points','assists','turnovers','atr_makes','atr_attempts','fg2_makes',
        'fg2_attempts','fg3_makes','fg3_attempts','ftm','fta','atr_pct','fg3_pct',
        'atr_freq_pct','fg3_freq_pct',
        'efg_pct','points_per_shot','assist_turnover_ratio','adj_assist_turnover_ratio',
        'second_assists','pot_assists','ft_pct','fg_pct','fg2_pct',
        'total_blue_collar','deflection','steal','block'
    }
    selected_stats = [s for s in request.args.getlist('trend_stat') if s in allowed_stats]
    if not selected_stats:
        selected_stats = ['points']

    selected_set = set(selected_stats)
    query_stats = set(selected_stats)
    if 'atr_pct' in query_stats:
        query_stats.update({'atr_makes','atr_attempts'})
    if 'fg3_pct' in query_stats:
        query_stats.update({'fg3_makes','fg3_attempts'})
    if query_stats & {'atr_freq_pct','fg3_freq_pct'}:
        query_stats.update({'atr_attempts','fg2_attempts','fg3_attempts'})
    if query_stats & {'efg_pct','points_per_shot','fg_pct','fg2_pct'}:
        query_stats.update({'atr_makes','atr_attempts','fg2_makes','fg2_attempts','fg3_makes','fg3_attempts'})
    if 'ft_pct' in query_stats:
        query_stats.update({'ftm','fta'})
    if query_stats & {'assist_turnover_ratio','adj_assist_turnover_ratio'}:
        query_stats.update({'assists','turnovers','second_assists','pot_assists'})

    bc_fields = {'total_blue_collar','deflection','steal','block'}
    computed_fields = {
        'atr_pct','fg3_pct','atr_freq_pct','fg3_freq_pct',
        'efg_pct','points_per_shot','assist_turnover_ratio',
        'adj_assist_turnover_ratio','fg_pct','fg2_pct','ft_pct'
    }

    sql_fields = [
        func.coalesce(func.sum(getattr(PlayerStats, s)), 0).label(s)
        for s in query_stats
        if s not in computed_fields and s not in bc_fields
    ]

    bc_alias = aliased(BlueCollarStats)
    roster_alias = aliased(Roster)
    bc_sql_fields = [
        func.coalesce(func.sum(getattr(bc_alias, s)), 0).label(s)
        for s in (query_stats & bc_fields)
    ]

    trend_query = (
        db.session.query(
            Practice.date.label('dt'),
            *sql_fields,
            *bc_sql_fields
        )
        .select_from(PlayerStats)
        .join(Practice, PlayerStats.practice_id == Practice.id)
    )
    if bc_sql_fields:
        trend_query = trend_query.join(
            roster_alias,
            and_(
                roster_alias.season_id == PlayerStats.season_id,
                roster_alias.player_name == PlayerStats.player_name,
            ),
        ).outerjoin(
            bc_alias,
            and_(
                bc_alias.practice_id == Practice.id,
                bc_alias.player_id == roster_alias.id,
                bc_alias.season_id == PlayerStats.season_id,
            ),
        )
    trend_query = trend_query.filter(PlayerStats.practice_id != None)
    if trend_season_id:
        trend_query = trend_query.filter(PlayerStats.season_id == trend_season_id)
    if trend_start_dt:
        trend_query = trend_query.filter(Practice.date >= trend_start_dt)
    if trend_end_dt:
        trend_query = trend_query.filter(Practice.date <= trend_end_dt)
    if trend_selected_categories:
        trend_query = trend_query.filter(Practice.category.in_(trend_selected_categories))
    # No player-level filtering
    trend_rows = []
    for r in trend_query.group_by(Practice.date).order_by(Practice.date):
        base = {s: getattr(r, s) for s in query_stats if s not in computed_fields}
        if 'atr_pct' in selected_stats:
            att = base.get('atr_attempts', 0)
            pct = round(base.get('atr_makes', 0) / att * 100, 1) if att else 0.0
            base['atr_pct'] = pct
        if 'fg3_pct' in selected_stats:
            att = base.get('fg3_attempts', 0)
            pct = round(base.get('fg3_makes', 0) / att * 100, 1) if att else 0.0
            base['fg3_pct'] = pct
        if 'ft_pct' in selected_stats:
            att = base.get('fta', 0)
            pct = round(base.get('ftm', 0) / att * 100, 1) if att else 0.0
            base['ft_pct'] = pct
        if selected_set & {'efg_pct','points_per_shot','fg_pct'}:
            total_shots = base.get('atr_attempts',0)+base.get('fg2_attempts',0)+base.get('fg3_attempts',0)
            if total_shots:
                efg = (base.get('atr_makes',0)+base.get('fg2_makes',0)+1.5*base.get('fg3_makes',0))/total_shots
                if 'efg_pct' in selected_stats:
                    base['efg_pct'] = round(efg*100,1)
                if 'points_per_shot' in selected_stats:
                    base['points_per_shot'] = round(efg*2,2)
                if 'fg_pct' in selected_stats:
                    fg = (base.get('atr_makes',0)+base.get('fg2_makes',0)+base.get('fg3_makes',0))/total_shots
                    base['fg_pct'] = round(fg*100,1)
                if selected_set & {'atr_freq_pct','fg3_freq_pct'}:
                    if 'atr_freq_pct' in selected_stats:
                        base['atr_freq_pct'] = round(base.get('atr_attempts',0)/total_shots*100,1)
                    if 'fg3_freq_pct' in selected_stats:
                        base['fg3_freq_pct'] = round(base.get('fg3_attempts',0)/total_shots*100,1)
            else:
                if 'efg_pct' in selected_stats:
                    base['efg_pct'] = 0.0
                if 'points_per_shot' in selected_stats:
                    base['points_per_shot'] = 0.0
                if 'fg_pct' in selected_stats:
                    base['fg_pct'] = 0.0
                if 'atr_freq_pct' in selected_stats:
                    base['atr_freq_pct'] = 0.0
                if 'fg3_freq_pct' in selected_stats:
                    base['fg3_freq_pct'] = 0.0
        if 'fg2_pct' in selected_stats:
            att = base.get('fg2_attempts',0)
            pct = round(base.get('fg2_makes',0)/att*100,1) if att else 0.0
            base['fg2_pct'] = pct
        if 'assist_turnover_ratio' in selected_stats:
            tos = base.get('turnovers',0)
            base['assist_turnover_ratio'] = round(base.get('assists',0)/tos,2) if tos else 0.0
        if 'adj_assist_turnover_ratio' in selected_stats:
            tos = base.get('turnovers',0)
            total_ast = base.get('assists',0)+base.get('second_assists',0)+base.get('pot_assists',0)
            base['adj_assist_turnover_ratio'] = round(total_ast/tos,2) if tos else 0.0
        trend_rows.append({'date': r.dt.isoformat(), **{s: base.get(s, 0) for s in selected_stats}})

    if trend_window and trend_window > 1:
        aggregated = []
        for i in range(len(trend_rows)):
            subset = trend_rows[max(0, i - trend_window + 1) : i + 1]
            row = {'date': trend_rows[i]['date']}
            for stat in selected_stats:
                vals = [d.get(stat, 0) for d in subset]
                row[stat] = round(sum(vals) / len(subset), 2)
            aggregated.append(row)
        trend_rows = aggregated

    return render_template(
        'admin/team_totals.html',
        totals=totals,
        blue_totals=blue_totals,
        paint_ppp=paint_ppp,
        shot_type_totals=shot_type_totals,
        shot_summaries=shot_summaries,
        seasons=seasons,
        selected_season=season_id,
        start_date=start_date or '',
        end_date=end_date or '',
        label_options=label_options,
        selected_labels=selected_labels,
        trend_rows=trend_rows,
        trend_selected_stats=selected_stats,
        trend_stat_options=sorted(allowed_stats),
        trend_selected_season=trend_season_id,
        trend_start_date=trend_start_date or '',
        trend_end_date=trend_end_date or '',
        trend_window=trend_window,
        trend_selected_labels=trend_selected_labels,
        practice_categories=practice_categories,
        trend_selected_categories=trend_selected_categories,
        active_page='team_totals',
    )


@admin_bp.route('/leaderboard')
@login_required
def leaderboard():
    """Show season leaderboard separate from the dashboard."""
    sid = request.args.get('season_id', type=int)
    if not sid:
        latest = Season.query.order_by(Season.start_date.desc()).first()
        sid = latest.id if latest else None

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    start_dt = end_dt = None
    if start_date:
        try:
            start_dt = date.fromisoformat(start_date)
        except ValueError:
            start_date = ''
    if end_date:
        try:
            end_dt = date.fromisoformat(end_date)
        except ValueError:
            end_date = ''

    stat_key = request.args.get('stat') or request.args.get('base_stat')
    if not stat_key:
        stat_key = LEADERBOARD_STATS[0]['key']

    q = PlayerStats.query.filter(PlayerStats.season_id == sid)
    if start_dt or end_dt:
        q = q.outerjoin(Game, PlayerStats.game_id == Game.id).outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        if start_dt:
            q = q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_dt),
                    and_(PlayerStats.practice_id != None, Practice.date >= start_dt),
                )
            )
        if end_dt:
            q = q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_dt),
                    and_(PlayerStats.practice_id != None, Practice.date <= end_dt),
                )
            )
    stats_list = q.all()

    label_options = collect_practice_labels(stats_list)
    selected_labels = [lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options]
    label_set = {lbl.upper() for lbl in selected_labels}

    cfg, rows = compute_leaderboard(stat_key, sid, start_dt, end_dt, label_set if label_set else None)

    all_seasons = Season.query.order_by(Season.start_date.desc()).all()

    return render_template(
        'admin/leaderboard.html',
        all_seasons=all_seasons,
        selected_season=sid,
        stats_config=LEADERBOARD_STATS,
        selected=cfg,
        rows=rows,
        start_date=start_date or '',
        end_date=end_date or '',
        label_options=label_options,
        selected_labels=selected_labels,
        active_page='leaderboard'
    )


@admin_bp.route('/usage')
@login_required
@admin_required
def usage_report():
    if current_user.username != 'bgoka21':
        abort(403)
    start = request.args.get('start_date')
    end = request.args.get('end_date')
    query = PageView.query
    if start:
        query = query.filter(PageView.timestamp >= start)
    if end:
        query = query.filter(PageView.timestamp <= end)
    user_stats = (
        query.outerjoin(User, PageView.user_id == User.id)
        .with_entities(User.id, User.username, db.func.count(PageView.id))
        .group_by(User.id, User.username)
        .all()
    )
    page_stats = (
        query.with_entities(PageView.endpoint, db.func.count(PageView.id))
        .group_by(PageView.endpoint)
        .all()
    )
    return render_template(
        'usage_report.html',
        user_stats=user_stats,
        page_stats=page_stats,
        start=start,
        end=end,
        active_page='usage'
    )


@admin_bp.route('/usage/user/<int:user_id>')
@login_required
@admin_required
def user_usage_report(user_id):
    if current_user.username != 'bgoka21':
        abort(403)
    start = request.args.get('start_date')
    end = request.args.get('end_date')
    user = User.query.get_or_404(user_id)
    query = PageView.query.filter(PageView.user_id == user_id)
    if start:
        query = query.filter(PageView.timestamp >= start)
    if end:
        query = query.filter(PageView.timestamp <= end)
    logs = query.order_by(PageView.timestamp.desc()).all()
    page_counts = (
        query.with_entities(PageView.endpoint, db.func.count(PageView.id))
        .group_by(PageView.endpoint)
        .all()
    )
    return render_template(
        'usage_user.html',
        user=user,
        logs=logs,
        page_counts=page_counts,
        start=start,
        end=end,
        active_page='usage'
    )


# --- Draft Upload ---
ALLOWED_DRAFT_EXTENSIONS = {'xlsx'}

def allowed_draft_file(fname):
    return fname and '.' in fname and fname.rsplit('.', 1)[1].lower() in ALLOWED_DRAFT_EXTENSIONS


@admin_bp.route('/draft-upload', methods=['GET', 'POST'])
@admin_required
def draft_upload():
    """Simple admin interface for uploading draft data spreadsheets."""
    upload_folder = current_app.config.get(
        'UPLOAD_FOLDER', os.path.join(current_app.root_path, 'uploads')
    )
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or not allowed_draft_file(file.filename):
            flash('Please upload a valid .xlsx file', 'draft-error')
            return redirect(url_for('admin.draft_upload'))

        filename = secure_filename(file.filename)
        os.makedirs(upload_folder, exist_ok=True)
        dest = os.path.join(upload_folder, filename)
        file.save(dest)

        # TODO: parse & save rows to PlayerDraftStock
        flash('File received! (parsing logic coming next)', 'draft-success')
        return redirect(url_for('admin.draft_upload'))

    return render_template('admin/draft_upload.html')
