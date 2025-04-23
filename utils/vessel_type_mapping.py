import csv
import os
import logging
from typing import Dict, Set, Optional

# Set up logger
logger = logging.getLogger("vessel_type_mapping")

# Path to the mapping file (relative to this utils folder)
MAPPING_FILE = os.path.join(os.path.dirname(__file__), "vessel_type_mapping.csv")

# vessel_type (col 1, lowercased/trimmed) -> set of lvl3 types (col 3, lowercased/trimmed)
_vessel_type_to_lvl3: Dict[str, Set[str]] = {}
_lvl3_values: Set[str] = set()
_loaded = False

def _load_mapping():
    global _vessel_type_to_lvl3, _lvl3_values, _loaded
    _vessel_type_to_lvl3 = {}
    _lvl3_values = set()
    try:
        with open(MAPPING_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                vessel_type = row['vessel_type'].strip().lower()
                lvl3 = row['vessel_type_level3'].strip().lower()
                if vessel_type and lvl3:
                    _vessel_type_to_lvl3.setdefault(vessel_type, set()).add(lvl3)
                    _lvl3_values.add(lvl3)
        _loaded = True
    except Exception as e:
        logger.warning(f"Failed to load vessel type mapping: {e}")
        _loaded = False

def ensure_loaded():
    if not _loaded:
        _load_mapping()

def vessel_type_matches_lvl3(vessel_type: Optional[str], allowed_lvl3: Set[str]) -> bool:
    """
    Returns True if vessel_type (from API) maps to any of the allowed lvl3 values.
    vessel_type: string from API (column 1)
    allowed_lvl3: set of lvl3 values (already normalized)
    """
    ensure_loaded()
    if not vessel_type:
        return False
    vessel_type_key = vessel_type.strip().lower()
    lvl3_set = _vessel_type_to_lvl3.get(vessel_type_key)
    if not lvl3_set:
        return False
    return not lvl3_set.isdisjoint(allowed_lvl3)

def validate_lvl3_values(requested_lvl3: Set[str]) -> Set[str]:
    """
    Returns the set of valid lvl3 values from the mapping, logs a warning for unknowns.
    """
    ensure_loaded()
    unknowns = requested_lvl3 - _lvl3_values
    if unknowns:
        logger.warning(f"Unknown vessel_type_level3 values in incl_vessel_type_lvl3: {unknowns}")
    return requested_lvl3 & _lvl3_values
