"""profile config search (plan .claude/tune-config-search-plan.md).

Thin-shell architecture: ProfileEngine sits on top of BenchmarkEngine + all
targets, owning config application (Admin ReloadModel + server-side disk
re-read), search (nested grid / quick), constraint filtering, and
recommendations. Submodules:
- grid: nested-grid enumeration and declaration-state constraints (§2.3/§2.4)
- config_writer: atomic config.yaml rewrite and backup/restore (§2.6.1/§2.6.2)
- preflight: preflight gates (§2.4 hard checks)
- checkpoint: trial records and campaign hashing (§2.8)
- engine: ProfileEngine main loop (§2.6 five-step sequence)
"""
