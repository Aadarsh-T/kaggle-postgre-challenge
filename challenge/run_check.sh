#!/usr/bin/env bash
# End-to-end check of the parallel Expand on the OFFICIAL generator data, without the GUI.
# Needs: Linux/WSL, PostgreSQL 16 + psql, python3 (3.10+) with the repo requirements, a role allowed to create databases.
# Connection via libpq env (PGHOST, PGPORT, PGUSER, PGPASSWORD or ~/.pgpass). Usage: challenge/run_check.sh [ROWS] (default 300000)
# Steps: 1 upstream + new unit tests, 2 generate kaggle_challenge, 3 serial baseline (the app's single REPEATABLE READ batch,
# recipes 01..06), 4 parallel script, 5 id-insensitive digests of all six tables must match, 6 crash + resume test.
set -euo pipefail
cd "$(dirname "$0")/.."
ROWS=${1:-300000}
OUT=${OUT_DIR:-challenge/out}; mkdir -p "$OUT"
export PGDATABASE=kaggle_challenge
PSQL="psql -X -qAt -v ON_ERROR_STOP=1"
now() { date +%s.%N; }

if [ -z "${SKIP_GEN:-}" ]; then  # SKIP_GEN=1: reuse an existing kaggle_challenge database
echo "== 1. unit tests"; python3 -m unittest discover -s tests -v 2>&1 | tail -5

echo "== 2. generate $ROWS rows"
python3 generate_database.py --rows "$ROWS" --replace --user "${PGUSER:-$USER}" ${PGHOST:+--host "$PGHOST"} ${PGPORT:+--postgres_port "$PGPORT"}
fi
HI=$($PSQL -c "SELECT max(raw_id) FROM public.raw_data")
$PSQL -c "CREATE TABLE IF NOT EXISTS public.pgdm_table_row_counts (schema_name text NOT NULL, table_name text NOT NULL,
  row_count bigint NOT NULL CHECK (row_count >= 0), initialized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), last_reconciled_at timestamptz, PRIMARY KEY (schema_name, table_name))"
reset() {
  $PSQL -c "TRUNCATE public.table2, public.table5, public.table6, public.table1, public.table3, public.table4 RESTART IDENTITY CASCADE;
    DROP TABLE IF EXISTS public.pgdm_expand_progress;
    INSERT INTO public.pgdm_table_row_counts (schema_name, table_name, row_count)
    SELECT 'public', t, 0 FROM unnest(ARRAY['table1','table2','table3','table4','table5','table6']) t
    ON CONFLICT (schema_name, table_name) DO UPDATE SET row_count = 0"
  $PSQL -c "VACUUM ANALYZE public.raw_data" >/dev/null
}
SRC="(SELECT * FROM \"public\".\"raw_data\" WHERE \"raw_schema\" IN ('group001'))"

echo "== 3. serial baseline (one REPEATABLE READ transaction, recipes 01..06)"
reset
{ echo "BEGIN ISOLATION LEVEL REPEATABLE READ;"
  for f in 01_table3 02_table4 03_table1 04_table5 05_table6 06_table2; do
    t=${f#*_}; sed "s|{{source}}|$SRC|g" "SQL files/$f.sql" | sed '$s/;[[:space:]]*$//'; echo ";"
    echo "UPDATE public.pgdm_table_row_counts SET row_count = (SELECT count(*) FROM public.$t) WHERE table_name = '$t';"
  done; echo "COMMIT;"; } > "$OUT/baseline.sql"
t0=$(now); $PSQL -f "$OUT/baseline.sql"; t1=$(now)
echo "baseline_seconds $(echo "$t1 - $t0" | bc)" | tee "$OUT/times.txt"
$PSQL -f challenge/digest.sql > "$OUT/digest_baseline.txt"; $PSQL -c "SELECT table_name, row_count FROM public.pgdm_table_row_counts ORDER BY 1" > "$OUT/counts_baseline.txt"

echo "== 4. parallel expansion"
reset
DB=$PGDATABASE HI=$HI WORKERS=${WORKERS:-$(nproc)} CHUNKS=${CHUNKS:-$((4 * $(nproc)))} OUT="$OUT/parallel.sh" python3 challenge/bench_harness.py "SQL files" >/dev/null
t0=$(now); bash "$OUT/parallel.sh" > "$OUT/parallel_stdout.txt"; t1=$(now)
echo "parallel_seconds $(echo "$t1 - $t0" | bc)" | tee -a "$OUT/times.txt"
$PSQL -f challenge/digest.sql > "$OUT/digest_parallel.txt"; $PSQL -c "SELECT table_name, row_count FROM public.pgdm_table_row_counts ORDER BY 1" > "$OUT/counts_parallel.txt"

echo "== 5. equivalence"
diff "$OUT/digest_baseline.txt" "$OUT/digest_parallel.txt" && diff "$OUT/counts_baseline.txt" "$OUT/counts_parallel.txt" && echo "DIGESTS_AND_COUNTERS_IDENTICAL"

echo "== 6. crash + resume"
reset
( sleep "${KILL_AFTER:-3}"; $PSQL -c "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'DO \$pgdm%' AND pid <> pg_backend_pid()" ) &
if bash "$OUT/parallel.sh" > "$OUT/crash_stdout.txt" 2> "$OUT/crash_stderr.txt"; then echo "run finished before the kill: raise KILL_AFTER or ROWS"; else echo "crashed as intended (exit $?)"; fi
wait
bash "$OUT/parallel.sh" > "$OUT/resume_stdout.txt"
$PSQL -f challenge/digest.sql > "$OUT/digest_resume.txt"; $PSQL -c "SELECT table_name, row_count FROM public.pgdm_table_row_counts ORDER BY 1" > "$OUT/counts_resume.txt"
diff "$OUT/digest_baseline.txt" "$OUT/digest_resume.txt" && diff "$OUT/counts_baseline.txt" "$OUT/counts_resume.txt" && echo "RESUME_IDENTICAL"
cat "$OUT/times.txt"
