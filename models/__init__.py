from .database import Possession, PlayerPossession as PossessionPlayer, ShotDetail
# Ensure new AAU/EYBL models are discoverable by migrations
from .eybl import ExternalIdentityMap, UnifiedStats  # noqa: F401
