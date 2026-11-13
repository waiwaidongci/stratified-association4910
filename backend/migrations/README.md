# Initial schema
SQLAlchemy metadata.create_all initializes the five business tables plus users and audit on first start. Unique keys: study.code, (stratum.study_id,stratum.code), count.stratum_id, run.name, note.title. Foreign keys form study→stratum→count and study→run→note. This is not a production schema migration mechanism; use a versioned migration tool before evolving persisted deployments.

## association_runs recovery columns
`attempts, max_attempts, lease_owner, lease_expires_at, fencing_token, last_attempt_at` support crash recovery and concurrent-worker fencing (see README “中断恢复与并发语义”). create_all adds them on a fresh database; `src/config/schema.py::ensure_run_columns` runs at API startup and idempotently issues ALTER TABLE ADD COLUMN for databases created by the earlier single-worker version, so existing persisted rows default to attempts=0 / max_attempts=3 and keep their original status and result.
