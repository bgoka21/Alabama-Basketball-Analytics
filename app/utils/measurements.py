import re


def parse_feet_inches(value):
    """Parse strings like 6'11, 6’11, 6-11 -> inches (float). Returns None if unknown."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = re.match(r"^\s*(\d+)\s*'\s*(\d+)\s*$", s)  # 6'11
    if m:
        return int(m.group(1)) * 12 + int(m.group(2))

    m = re.match(r"^\s*(\d+)\s*[’′-]\s*(\d+)\s*$", s)  # 6’11 or 6-11
    if m:
        return int(m.group(1)) * 12 + int(m.group(2))

    try:
        return float(s.replace('"', ''))
    except Exception:
        return None
