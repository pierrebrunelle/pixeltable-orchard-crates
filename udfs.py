"""Pixeltable UDFs for the orchard-crates batch ETL (module path e.g. `udfs.grade_lane`)."""

from __future__ import annotations

import pixeltable as pxt


@pxt.udf
def grade_lane(variety: str, weight_kg: float) -> str:
    """Map variety + weight into a packing lane label."""
    v = (variety or 'unknown').strip().lower().replace(' ', '-')
    w = float(weight_kg) if weight_kg is not None else 0.0
    if w <= 0:
        bucket = 'reject'
    elif w < 16:
        bucket = 'light'
    elif w < 20:
        bucket = 'mid'
    else:
        bucket = 'heavy'
    return f'{v}:{bucket}'


@pxt.udf
def moisture_ok(moisture_pct: float) -> float:
    """Return moisture as-is, or raise if outside a physical range (for on_error=ignore)."""
    if moisture_pct is None:
        raise ValueError('moisture_pct is required')
    m = float(moisture_pct)
    if m < 0 or m > 100:
        raise ValueError(f'moisture_pct out of range: {m}')
    return m


@pxt.udf
def crate_tag(crate_code: str, orchard: str) -> str:
    """Compact tag like 'C-1001@willamette'."""
    code = (crate_code or 'UNK').strip().upper()
    orch = (orchard or 'field').strip().lower().replace(' ', '-')
    return f'{code}@{orch}'
