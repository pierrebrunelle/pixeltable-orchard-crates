"""Stage-1 schema for orchard crates (pre-evolution). Applied by run.py before the final app.py."""
from __future__ import annotations

import pixeltable as pxt
import pixeltable.functions as pxtf
from pixeltable.functions import math as pxtm

from udfs import crate_tag, grade_lane, moisture_ok

TableModel = pxt.model_base()


class Crates(TableModel, name='crates', has_default_idxs=False):
    """Orchard harvest crate (stage 1: base + core math)."""

    crate_id = pxt.Column(value=pxtf.uuid.uuid7(), primary_key=True)
    crate_code: pxt.String
    orchard: pxt.String
    variety: pxt.String
    weight_kg: pxt.Float
    length_cm: pxt.Float
    width_cm: pxt.Float
    height_cm: pxt.Float
    moisture_pct: pxt.Float
    ingest_batch: pxt.String  # dropped in final schema (schema evolution)

    # fn.math
    volume_cm3 = length_cm * width_cm * height_cm
    abs_weight = pxtm.abs(weight_kg)
    weight_ceil = pxtm.ceil(weight_kg)
    weight_floor = pxtm.floor(weight_kg)
    weight_rounded = pxtm.round(weight_kg, 1)
    side_sqrt = pxtm.sqrt(pxtm.abs(length_cm))
    volume_log = pxtm.log(pxtm.abs(volume_cm3) + 1.0)
    density = weight_kg / (volume_cm3 / 1000.0)  # kg per liter approx

    # UDFs (moisture_ok raises on bad rows → insert with on_error='ignore')
    moisture_checked = moisture_ok(moisture_pct)
    lane = grade_lane(variety, weight_kg)
    tag = crate_tag(crate_code, orchard)

    __indexes__ = [
        pxt.BtreeIndex(orchard),
        pxt.BtreeIndex(variety),
        pxt.BtreeIndex(lane),
    ]
