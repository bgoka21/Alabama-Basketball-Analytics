"""
Synergy Stats Blueprint
Integrates Synergy Sports API into Alabama Basketball Analytics
"""

from flask import Blueprint, render_template, jsonify, request, flash, redirect, url_for
from flask_login import login_required, current_user
from datetime import datetime, timedelta
from models.database import db
from .synergy_client import SynergyClient
from models.database import SynergyCache, SynergyPnRStats
import json

synergy_bp = Blueprint(
    'synergy',
    __name__,
    url_prefix='/synergy',
    template_folder='templates/synergy'
)




# ============================================================================
# DASHBOARD & MAIN PAGES
# ============================================================================

@synergy_bp.route('/')
@login_required
def dashboard():
    """Synergy Stats Dashboard - Overview of all Synergy data"""
    
    # Get cached data info
    cache_info = get_cache_status()
    
    # Get summary stats
    summary = get_pnr_summary()
    
    return render_template(
        'synergy/dashboard.html',
        cache_info=cache_info,
        summary=summary
    )


@synergy_bp.route('/pnr')
@login_required
def pnr_analysis():
    """Pick & Roll Analysis Page"""
    
    # Get PnR stats from database
    ball_handlers = SynergyPnRStats.query.filter_by(
        play_type='PandRBallHandler',
        defensive=False
    ).order_by(SynergyPnRStats.possessions.desc()).limit(15).all()
    
    roll_men = SynergyPnRStats.query.filter_by(
        play_type='PandRRollMan',
        defensive=False
    ).order_by(SynergyPnRStats.possessions.desc()).limit(15).all()
    
    # Calculate team totals
    bh_totals = calculate_team_totals(ball_handlers)
    rm_totals = calculate_team_totals(roll_men)
    
    cache_info = get_cache_status()
    
    return render_template(
        'synergy/pnr_analysis.html',
        ball_handlers=ball_handlers,
        roll_men=roll_men,
        bh_totals=bh_totals,
        rm_totals=rm_totals,
        cache_info=cache_info
    )


@synergy_bp.route('/player/<player_id>')
@login_required
def player_detail(player_id):
    """Individual player Synergy stats"""
    
    # Get all stats for this player
    stats = SynergyPnRStats.query.filter_by(
        player_id=player_id
    ).all()
    
    if not stats:
        flash('No Synergy data found for this player', 'warning')
        return redirect(url_for('synergy.pnr_analysis'))
    
    player_name = stats[0].player_name if stats else 'Unknown'
    
    return render_template(
        'synergy/player_detail.html',
        stats=stats,
        player_name=player_name,
        player_id=player_id
    )


# ============================================================================
# API ENDPOINTS (For AJAX calls)
# ============================================================================

@synergy_bp.route('/api/pnr/ball-handlers')
@login_required
def api_ball_handlers():
    """API endpoint for ball handler stats"""
    
    stats = SynergyPnRStats.query.filter_by(
        play_type='PandRBallHandler',
        defensive=False
    ).order_by(SynergyPnRStats.possessions.desc()).all()
    
    return jsonify({
        'success': True,
        'data': [stat.to_dict() for stat in stats],
        'count': len(stats)
    })


@synergy_bp.route('/api/pnr/roll-men')
@login_required
def api_roll_men():
    """API endpoint for roll man stats"""
    
    stats = SynergyPnRStats.query.filter_by(
        play_type='PandRRollMan',
        defensive=False
    ).order_by(SynergyPnRStats.possessions.desc()).all()
    
    return jsonify({
        'success': True,
        'data': [stat.to_dict() for stat in stats],
        'count': len(stats)
    })


@synergy_bp.route('/api/pnr/summary')
@login_required
def api_summary():
    """API endpoint for PnR team summary"""
    
    summary = get_pnr_summary()
    
    return jsonify({
        'success': True,
        'data': summary
    })


@synergy_bp.route('/api/refresh', methods=['POST'])
@login_required
def api_refresh():
    """Force refresh Synergy data from API"""
    
    if not current_user.is_admin:
        return jsonify({
            'success': False,
            'error': 'Admin access required'
        }), 403
    
    try:
        # Fetch fresh data
        client = SynergyClient()
        context = client.get_team_context()
        
        # Fetch PnR stats
        client = SynergyClient()
        ball_handler_data = client.get_player_playtype_stats(
            season_id=context['season_id'],
            team_id=context['team_id'],
            play_type='PandRBallHandler',
            defensive=False
        )
        
        roll_man_data = client.get_player_playtype_stats(
            season_id=context['season_id'],
            team_id=context['team_id'],
            play_type='PandRRollMan',
            defensive=False
        )
        
        # Store in database
        store_pnr_stats(ball_handler_data, 'PandRBallHandler', context)
        store_pnr_stats(roll_man_data, 'PandRRollMan', context)
        
        # Update cache timestamp
        update_cache_timestamp('pnr_stats')
        
        return jsonify({
            'success': True,
            'message': 'Data refreshed successfully',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_cache_status():
    """Get cache information for display"""
    
    cache = SynergyCache.query.filter_by(cache_key='pnr_stats').first()
    
    if not cache:
        return {
            'exists': False,
            'last_updated': None,
            'age_hours': None,
            'is_stale': True
        }
    
    age = datetime.utcnow() - cache.updated_at
    age_hours = age.total_seconds() / 3600
    
    return {
        'exists': True,
        'last_updated': cache.updated_at,
        'age_hours': age_hours,
        'is_stale': age_hours > 24  # Consider stale after 24 hours
    }


def get_pnr_summary():
    """Calculate PnR summary statistics"""
    
    # Ball Handler totals
    ball_handlers = SynergyPnRStats.query.filter_by(
        play_type='PandRBallHandler',
        defensive=False
    ).all()
    
    # Roll Man totals
    roll_men = SynergyPnRStats.query.filter_by(
        play_type='PandRRollMan',
        defensive=False
    ).all()
    
    return {
        'ball_handler': calculate_team_totals(ball_handlers),
        'roll_man': calculate_team_totals(roll_men),
        'player_count_bh': len(ball_handlers),
        'player_count_rm': len(roll_men)
    }


def calculate_team_totals(stats_list):
    """Calculate team totals from list of player stats"""
    
    if not stats_list:
        return {
            'possessions': 0,
            'points': 0,
            'ppp': 0,
            'fg_made': 0,
            'fg_attempt': 0,
            'fg_pct': 0,
            'turnovers': 0,
            'to_pct': 0
        }
    
    totals = {
        'possessions': sum(s.possessions for s in stats_list),
        'points': sum(s.points for s in stats_list),
        'fg_made': sum(s.fg_made for s in stats_list),
        'fg_attempt': sum(s.fg_attempt for s in stats_list),
        'turnovers': sum(s.turnovers for s in stats_list)
    }
    
    totals['ppp'] = totals['points'] / totals['possessions'] if totals['possessions'] > 0 else 0
    totals['fg_pct'] = (totals['fg_made'] / totals['fg_attempt'] * 100) if totals['fg_attempt'] > 0 else 0
    totals['to_pct'] = (totals['turnovers'] / totals['possessions'] * 100) if totals['possessions'] > 0 else 0
    
    return totals


def store_pnr_stats(api_data, play_type, context):
    """Store PnR stats in database"""
    # Clear existing stats for this play type
    SynergyPnRStats.query.filter_by(
        play_type=play_type,
        season_id=context['season_id']
    ).delete()
    
    # Parse and store new data
    data = api_data.get('data', [])
    
    for item in data:
        player_obj = item.get('data', item)
        stats = player_obj.get('stats', {})
        player_info = player_obj.get('player', {})
        
        # DEBUG: Print Philon's stats to see what fields exist
        if player_info.get('name') == 'Labaron Philon':
            print("=" * 60)
            print(f"DEBUG PHILON STATS:")
            print(json.dumps(stats, indent=2))
            print("=" * 60)
        
        stat_entry = SynergyPnRStats(
            player_id=player_info.get('id'),
            player_name=player_info.get('name'),
            season_id=context['season_id'],
            team_id=context['team_id'],
            play_type=play_type,
            defensive=False,
            possessions=stats.get('possessions', 0),
            points=stats.get('points', 0),
            ppp=stats.get('ppp', 0),
            fg_made=stats.get('fgMade', 0),
            fg_attempt=stats.get('fgAttempt', 0),
            fg_percent=stats.get('fgPercent', 0),
            turnovers=stats.get('turnover', 0),  # Note: singular!
            fouls=stats.get('fouls', 0),
            games_played=stats.get('gp', 0),
            raw_data=json.dumps(stats)
        )
        
        db.session.add(stat_entry)
    
    db.session.commit()


def update_cache_timestamp(cache_key):
    """Update cache timestamp"""
    
    cache = SynergyCache.query.filter_by(cache_key=cache_key).first()
    
    if not cache:
        cache = SynergyCache(cache_key=cache_key)
        db.session.add(cache)
    
    cache.updated_at = datetime.utcnow()
    db.session.commit()
