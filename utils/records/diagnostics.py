"""Utilities for validating record stat-key coverage."""
from __future__ import annotations

import logging
from typing import Iterable, List

from utils.records.candidate_builder import get_missing_stat_keys
from utils.records.stat_keys import get_all_stat_keys

logger = logging.getLogger(__name__)


def validate_stat_key_coverage(registry_keys: Iterable[str] | None = None) -> List[str]:
    """Print missing stat-key mappings and return the list for diagnostics."""
    keys = set(registry_keys) if registry_keys is not None else get_all_stat_keys()
    missing = get_missing_stat_keys(keys)
    if missing:
        message = "Missing candidate_builder mappings for stat keys: " + ", ".join(missing)
        print(message)
        logger.warning(message)
    else:
        print("All stat keys have candidate_builder mappings.")
    return missing
