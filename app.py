"""Orchard Crates batch ETL built with Pixeltable.

Import harvest crate rows from CSV, derive packing math with pixeltable.functions.math
(abs/ceil/floor/round/sqrt/log/pow), validate moisture with an on_error=ignore UDF, then
export an enriched CSV. Schema evolution drops the temporary ingest_batch column and adds
shelf_life_days + density_pow. Drive with `python run.py` — no HTTP router.

Layout: UDFs live in udfs.py; this file holds table models and @pxt.query functions.

Run locally:
    pxt schema update app.py orchard
    python run.py
"""
from __future__ import annotations

import pixeltable as pxt
import pixeltable.functions as pxtf
from pixeltable.functions import math as pxtm

from udfs import crate_tag, grade_lane, moisture_ok

TableModel = pxt.model_base()


class Crates(TableModel, name='crates', has_default_idxs=False):
    """One orchard harvest crate with packing math and moisture validation."""

    crate_id = pxt.Column(value=pxtf.uuid.uuid7(), primary_key=True)
    crate_code: pxt.String
    orchard: pxt.String
    variety: pxt.String
    weight_kg: pxt.Float
    length_cm: pxt.Float
    width_cm: pxt.Float
    height_cm: pxt.Float
    moisture_pct: pxt.Float

    # --- fn.math primary helpers ---
    volume_cm3 = length_cm * width_cm * height_cm
    abs_weight = pxtm.abs(weight_kg)
    weight_ceil = pxtm.ceil(weight_kg)
    weight_floor = pxtm.floor(weight_kg)
    weight_rounded = pxtm.round(weight_kg, 1)
    side_sqrt = pxtm.sqrt(pxtm.abs(length_cm))
    volume_log = pxtm.log(pxtm.abs(volume_cm3) + 1.0)
    density = weight_kg / (volume_cm3 / 1000.0)  # kg / liter
    density_pow = pxtm.pow(pxtm.abs(density) + 0.01, 0.5)  # added in schema evolution
    shelf_life_days = pxtm.floor(30.0 - pxtm.abs(moisture_pct - 12.0))  # evolution add

    # --- on_error UDF path ---
    moisture_checked = moisture_ok(moisture_pct)
    lane = grade_lane(variety, weight_kg)
    tag = crate_tag(crate_code, orchard)

    __indexes__ = [
        pxt.BtreeIndex(orchard),
        pxt.BtreeIndex(variety),
        pxt.BtreeIndex(lane),
    ]


@pxt.query
def by_orchard(orchard: str):
    """Crates for one orchard (orchard index)."""
    return (
        Crates.where(Crates.orchard == orchard)
        .select(
            Crates.crate_code,
            Crates.variety,
            Crates.weight_kg,
            Crates.density,
            Crates.lane,
            Crates.tag,
            Crates.shelf_life_days,
        )
        .order_by(Crates.crate_code)
    )


@pxt.query
def heavy_lanes(min_weight: float):
    """Crates at or above min_weight with packing math."""
    return (
        Crates.where(Crates.weight_kg >= min_weight)
        .select(
            Crates.crate_code,
            Crates.orchard,
            Crates.weight_rounded,
            Crates.volume_cm3,
            Crates.density,
            Crates.density_pow,
            Crates.lane,
        )
        .order_by(Crates.weight_kg, asc=False)
    )


@pxt.query
def moisture_failures():
    """Rows where moisture_ok failed (on_error=ignore → errortype set)."""
    return (
        Crates.where(Crates.moisture_checked.errortype != None)  # noqa: E711
        .select(
            Crates.crate_code,
            Crates.moisture_pct,
            Crates.moisture_checked,
            err=Crates.moisture_checked.errortype,
            msg=Crates.moisture_checked.errormsg,
        )
        .order_by(Crates.crate_code)
    )
