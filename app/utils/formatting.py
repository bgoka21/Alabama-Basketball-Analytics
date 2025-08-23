def fmt_money(n):
    try:
        n = float(n or 0)
    except Exception:
        return "—"
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1_000_000_000: s = f"{n/1_000_000_000:.1f}B"
    elif n >= 1_000_000:   s = f"{n/1_000_000:.1f}M"
    elif n >= 1_000:       s = f"{n/1_000:.1f}K"
    else:                  s = f"{n:.0f}"
    return f"{sign}${s}"


def posneg_class(n):
    try:
        n = float(n or 0)
    except Exception:
        n = 0
    return "text-green-600" if n > 0 else ("text-red-600" if n < 0 else "text-gray-600")


from jinja2 import filters as _jinja_filters
_jinja_filters.FILTERS.setdefault("fmt_money", fmt_money)
_jinja_filters.FILTERS.setdefault("posneg", posneg_class)
