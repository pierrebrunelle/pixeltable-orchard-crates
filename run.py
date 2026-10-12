#!/usr/bin/env python3
"""Orchard Crates batch-etl driver (no HTTP service).

Workflow: schema evolution
1. Apply stage-1 schema (includes temporary ingest_batch)
2. import_csv staging table from data/crates.csv
3. Insert into modeled crates with on_error='ignore' (moisture_ok failures)
4. Inspect errortype/errormsg; exercise math columns
5. export_csv enriched view
6. Evolve to final app.py (drop ingest_batch, add density_pow + shelf_life_days)
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pixeltable as pxt
from pixeltable.io import export_csv, import_csv

import udfs  # noqa: F401 — register UDF module path

ROOT = Path(__file__).resolve().parent
CSV_IN = ROOT / 'data' / 'crates.csv'
CSV_OUT = ROOT / 'data' / 'crates_enriched.csv'
LOG = ROOT / 'logs' / 'run.jsonl'
VENV_PXT = Path(__file__).resolve().parents[2] / '.venv' / 'bin' / 'pxt'
CATALOG = 'orchard'


def log(event: str, **payload) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open('a') as f:
        f.write(json.dumps({'event': event, **payload}, default=str) + '\n')
    print(event, json.dumps(payload, default=str)[:320], flush=True)


def run_schema(schema_file: str, *, allow_destructive: bool = False) -> None:
    cmd = [str(VENV_PXT), 'schema', 'update', schema_file, CATALOG, '-f']
    if allow_destructive:
        cmd.append('--allow-destructive')
    log('schema_update_start', schema=schema_file, allow_destructive=allow_destructive)
    t0 = time.time()
    r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    dt = round(time.time() - t0, 2)
    log(
        'schema_update_done',
        schema=schema_file,
        exit=r.returncode,
        seconds=dt,
        stdout=(r.stdout or '')[-500:],
        stderr=(r.stderr or '')[-500:],
    )
    if r.returncode != 0:
        raise SystemExit(f'schema update failed: {r.returncode}')


def main() -> int:
    if LOG.exists():
        LOG.unlink()

    # --- stage 1 schema ---
    run_schema('schema_stage1.py')

    # clean prior staging / export leftovers from re-runs
    if f'{CATALOG}/staging' in pxt.list_tables(CATALOG):
        pxt.drop_table(f'{CATALOG}/staging', force=True)

    # --- feat.io.csv: import ---
    t0 = time.time()
    staging = import_csv(
        f'{CATALOG}/staging',
        str(CSV_IN),
        schema_overrides={
            'crate_code': pxt.String,
            'orchard': pxt.String,
            'variety': pxt.String,
            'weight_kg': pxt.Float,
            'length_cm': pxt.Float,
            'width_cm': pxt.Float,
            'height_cm': pxt.Float,
            'moisture_pct': pxt.Float,
        },
    )
    log('import_csv', rows=staging.count(), seconds=round(time.time() - t0, 2))

    crates = pxt.get_table(f'{CATALOG}.crates')
    if crates.count():
        # re-run safety: empty modeled table via delete-all if supported
        try:
            crates.delete()
            log('crates_cleared', remaining=crates.count())
        except Exception as e:
            log('crates_clear_skip', error=str(e)[:200])

    rows = []
    for r in staging.collect():
        rows.append(
            {
                'crate_code': r['crate_code'],
                'orchard': r['orchard'],
                'variety': r['variety'],
                'weight_kg': float(r['weight_kg']),
                'length_cm': float(r['length_cm']),
                'width_cm': float(r['width_cm']),
                'height_cm': float(r['height_cm']),
                'moisture_pct': float(r['moisture_pct']),
                'ingest_batch': '2026-10-12-am',
            }
        )

    t0 = time.time()
    status = crates.insert(rows, on_error='ignore')
    log(
        'insert_on_error_ignore',
        seconds=round(time.time() - t0, 2),
        status=str(status),
        count=crates.count(),
    )

    # inspect moisture failures (feat.computed.on_error)
    fail_q = (
        crates.where(crates.moisture_checked.errortype != None)  # noqa: E711
        .select(
            crates.crate_code,
            crates.moisture_pct,
            err=crates.moisture_checked.errortype,
            msg=crates.moisture_checked.errormsg,
        )
        .collect()
    )
    log('moisture_failures', n=len(fail_q), sample=list(fail_q)[:3])

    # math sample
    math_sample = (
        crates.select(
            crates.crate_code,
            crates.weight_kg,
            crates.abs_weight,
            crates.weight_ceil,
            crates.weight_floor,
            crates.weight_rounded,
            crates.side_sqrt,
            crates.volume_cm3,
            crates.volume_log,
            crates.density,
            crates.lane,
            crates.tag,
        )
        .limit(5)
        .collect()
    )
    log('math_sample', n=len(math_sample), sample=list(math_sample)[:2])

    # --- feat.io.csv: export ---
    if CSV_OUT.exists():
        CSV_OUT.unlink()
    t0 = time.time()
    export_csv(
        crates.select(
            crates.crate_code,
            crates.orchard,
            crates.variety,
            crates.weight_rounded,
            crates.volume_cm3,
            crates.density,
            crates.lane,
            crates.tag,
            crates.moisture_pct,
        ).order_by(crates.crate_code),
        str(CSV_OUT),
    )
    log('export_csv', path=str(CSV_OUT.name), bytes=CSV_OUT.stat().st_size, seconds=round(time.time() - t0, 2))

    # --- schema evolution: stage1 → final app.py (drop ingest_batch, add cols) ---
    run_schema('app.py', allow_destructive=True)

    crates = pxt.get_table(f'{CATALOG}.crates')
    cols = list(crates.get_metadata()['columns'].keys())
    log(
        'evolved_schema',
        has_ingest_batch=('ingest_batch' in cols),
        has_density_pow=('density_pow' in cols),
        has_shelf_life=('shelf_life_days' in cols),
        columns=cols,
    )

    evolved = (
        crates.select(
            crates.crate_code,
            crates.density,
            crates.density_pow,
            crates.shelf_life_days,
            crates.lane,
        )
        .limit(5)
        .collect()
    )
    log('evolved_sample', sample=list(evolved)[:3])

    # Mirror @pxt.query helpers via table filters (query templates are for schema/serving)
    orch = list(
        crates.where(crates.orchard == 'willamette')
        .select(crates.crate_code, crates.variety, crates.density, crates.lane, crates.shelf_life_days)
        .order_by(crates.crate_code)
        .collect()
    )
    heavy = list(
        crates.where(crates.weight_kg >= 18.0)
        .select(crates.crate_code, crates.weight_rounded, crates.density_pow, crates.lane)
        .order_by(crates.weight_kg, asc=False)
        .collect()
    )
    fails = list(
        crates.where(crates.moisture_checked.errortype != None)  # noqa: E711
        .select(crates.crate_code, crates.moisture_pct, err=crates.moisture_checked.errortype)
        .collect()
    )
    import app as orchard_app
    assert hasattr(orchard_app, 'by_orchard') and hasattr(orchard_app, 'heavy_lanes')
    log(
        'queries',
        by_orchard=len(orch),
        heavy=len(heavy),
        moisture_failures=len(fails),
        query_templates=['by_orchard', 'heavy_lanes', 'moisture_failures'],
    )

    log(
        'done',
        crates=crates.count(),
        staging=staging.count(),
        export=CSV_OUT.name,
        moisture_error_rows=len(fails),
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
