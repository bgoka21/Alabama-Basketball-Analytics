from .database import Possession, PlayerPossession as PossessionPlayer, ShotDetail
# Scout module models (isolated from main stats)
from .scout import ScoutTeam, ScoutGame, ScoutPossession  # noqa: F401
# Ensure new AAU/EYBL models are discoverable by migrations
from .eybl import ExternalIdentityMap, UnifiedStats, IdentitySynonym  # noqa: F401
