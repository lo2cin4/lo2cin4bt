# Audit Output Contract

This contract defines where auditability data should live when semantic-native runtime adds source provenance.

## Principle
- keep primary user-facing outputs concise
- keep detailed provenance machine-readable
- keep AI / script consumers able to reconstruct feature lineage

## Placement
- primary parquet outputs keep only summary index fields
- metadata sidecars keep compact run-level summaries
- detailed provenance goes to `*_audit.json` and/or `*_audit.parquet`
- large detailed payloads should stay machine-friendly via chunked `*_audit_rows_XXX.jsonl`
- detailed parquet sidecars should use explicit compression to avoid runaway artifact size

## UX Rule
- detailed audit data must not become the default newbie-facing surface
- default plotter / report views should remain summary-oriented
