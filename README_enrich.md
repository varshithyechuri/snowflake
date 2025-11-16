enrich_aqi.py — README

Purpose

This small utility enriches an air-quality JSON dataset (like `full_aqi_sample_11hr.json`) by adding:
- a stable object identifier per record (`object_id`),
- an `audit` block with ingestion timestamp, source file and a stable hash,
- a `dq` block listing data-quality results and issues,
- a top-level `dq_summary` with counts.

Usage

Run from the repository root (Windows PowerShell / bash):

```powershell
python .\enrich_aqi.py --input .\full_aqi_sample_11hr.json --output .\full_aqi_sample_11hr_enriched.json
```

Notes and assumptions
- The script expects a top-level `records` array in the input JSON.
- The `object_id` is generated deterministically from a SHA-256 hash of the record JSON (sorted keys), so the same record content yields the same object id.
- DQ checks are conservative: non-numeric lat/lon, unparseable `last_update`, missing fields, and `pollutant_avg` being `NA` are reported.
- The script copies all other top-level fields and adds `records_enriched` (replaces `records` to avoid duplication), `dq_summary`, and `enriched_at`.

Next steps / improvements
- Add streaming JSON processing for very large files (memory-safe).
- Add configurable DQ rules via a YAML file.
- Add unit tests and CI workflow to run the script against a sample.
