#!/usr/bin/env python3
"""
enrich_aqi.py
Reads a raw AQI JSON file (expected structure like `full_aqi_sample_11hr.json`),
adds an object identifier and audit + data-quality (DQ) fields for each record,
and writes an enriched JSON output.

Usage:
  python enrich_aqi.py --input full_aqi_sample_11hr.json --output full_aqi_sample_11hr_enriched.json

The script produces a JSON with the same top-level keys, but replaces `records`
with `records_enriched` where each record contains two new keys:
  - `audit`: {ingested_at, source_file, record_hash, object_id}
  - `dq`: {passed: bool, issues: [strings]}

It also writes a small `dq_summary` object at the top-level with counts.
"""

from __future__ import annotations
import argparse
import json
import hashlib
import uuid
from datetime import datetime
from typing import Any, Dict, List, Tuple
import sys


def record_hash(record: Dict[str, Any]) -> str:
    # Stable hash: canonical JSON (sorted keys) -> sha256
    compact = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(compact.encode('utf-8')).hexdigest()


def parse_last_update(val: str) -> Tuple[bool, str]:
    """Try to parse last_update with common formats. Return (ok, iso_ts_or_error)."""
    if val is None:
        return False, "missing"
    val = val.strip()
    # Try dd-mm-YYYY HH:MM:SS (sample shows '08-01-2024 11:00:00')
    fmts = ["%d-%m-%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%d/%m/%Y %H:%M:%S"]
    for f in fmts:
        try:
            dt = datetime.strptime(val, f)
            return True, dt.isoformat() + "Z"
        except Exception:
            continue
    return False, f"unparseable:{val}"


def is_floatish(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return True
    try:
        s = str(v).strip()
        # disallow empty and NA
        if s.upper() == 'NA' or s == '':
            return False
        float(s)
        return True
    except Exception:
        return False


def dq_checks(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    # required fields
    required = [
        'country', 'state', 'city', 'station', 'last_update',
        'latitude', 'longitude', 'pollutant_id', 'pollutant_avg'
    ]
    for f in required:
        if f not in record or record.get(f) is None or str(record.get(f)).strip() == '':
            issues.append(f"missing:{f}")

    # last_update parse
    if 'last_update' in record:
        ok, info = parse_last_update(str(record.get('last_update')))
        if not ok:
            issues.append(f"last_update_unparseable:{info}")

    # latitude/longitude numeric & within range
    lat = record.get('latitude')
    lon = record.get('longitude')
    if not is_floatish(lat):
        issues.append(f"latitude_not_numeric:{lat}")
    else:
        try:
            fv = float(lat)
            if not (-90.0 <= fv <= 90.0):
                issues.append(f"latitude_out_of_range:{fv}")
        except Exception:
            issues.append(f"latitude_parse_error:{lat}")
    if not is_floatish(lon):
        issues.append(f"longitude_not_numeric:{lon}")
    else:
        try:
            fv = float(lon)
            if not (-180.0 <= fv <= 180.0):
                issues.append(f"longitude_out_of_range:{fv}")
        except Exception:
            issues.append(f"longitude_parse_error:{lon}")

    # pollutant_avg numeric or NA acceptable but flagged
    pa = record.get('pollutant_avg')
    if pa is None or str(pa).strip() == '':
        issues.append('pollutant_avg_missing')
    else:
        if str(pa).strip().upper() == 'NA':
            issues.append('pollutant_avg_NA')
        else:
            if not is_floatish(pa):
                issues.append(f"pollutant_avg_not_numeric:{pa}")

    passed = len(issues) == 0
    return passed, issues


def enrich_records(records: List[Dict[str, Any]], source_file: str) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    enriched = []
    counts = {'total': 0, 'dq_passed': 0, 'dq_failed': 0}
    for rec in records:
        counts['total'] += 1
        # compute stable hash for object id
        rhash = record_hash(rec)
        obj_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, rhash))
        ingested_at = datetime.utcnow().isoformat() + 'Z'
        passed, issues = dq_checks(rec)
        if passed:
            counts['dq_passed'] += 1
        else:
            counts['dq_failed'] += 1
        rec_enriched = dict(rec)  # shallow copy
        rec_enriched['audit'] = {
            'ingested_at': ingested_at,
            'source_file': source_file,
            'record_hash': rhash,
            'object_id': obj_uuid
        }
        rec_enriched['dq'] = {
            'passed': passed,
            'issues': issues
        }
        enriched.append(rec_enriched)
    return enriched, counts


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description='Enrich AQI JSON with audit and DQ fields')
    p.add_argument('--input', '-i', required=True, help='Input JSON file path')
    p.add_argument('--output', '-o', required=True, help='Output enriched JSON file path')
    args = p.parse_args(argv)

    input_path = args.input
    output_path = args.output

    print(f"Reading {input_path} ...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if 'records' not in data or not isinstance(data['records'], list):
        print("Input JSON doesn't contain a top-level 'records' array. Aborting.")
        return 2

    records = data['records']
    print(f"Found {len(records)} records. Running enrichment and DQ checks...")

    enriched, counts = enrich_records(records, source_file=input_path)

    # Build output structure
    out = dict(data)  # copy all top-level fields
    out['records_enriched'] = enriched
    # Remove original records array to avoid duplication
    if 'records' in out:
        del out['records']

    out['dq_summary'] = counts
    out['enriched_at'] = datetime.utcnow().isoformat() + 'Z'

    print(f"Writing enriched data to {output_path} ...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("Done.")
    print(f"Total: {counts['total']}, Passed: {counts['dq_passed']}, Failed: {counts['dq_failed']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
