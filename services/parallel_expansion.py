"""Parallel, chunked, resumable Expand executor (drop-in module for services/postgres_service.py).

The app keeps its Windows -> SSH -> Linux architecture: instead of one psql script it sends ONE bash script to `bash -s`
on the server. The script needs only bash, coreutils, xargs and psql there (no server-side Python).

  coordinator psql session (whole run):
    BEGIN (read committed)
    LOCK source IN SHARE MODE        -> source rows frozen for the whole run (writers wait), so the manifest and every
                                        chunk see exactly the same rows (what the original REPEATABLE READ gave)
    session advisory lock per destination (blocks a second concurrent Expand of the same tables)
    manifest SQL (unchanged)
    \\! bash phases.sh              -> worker psql sessions, `xargs -P workers`
    verify every planned chunk committed for this run key, else RAISE 'expansion incomplete ... rerun to resume'
    COMMIT
  phases.sh:
    defer   : save + drop deferrable constraints (FKs not referencing the source; UNIQUEs on destinations that no other
              recipe reads and no FK references); definitions are saved in the progress table first, same txn
    serial  : DISTINCT-style recipes (destinations that other recipes read and that are not chunkable) as one chunk each
    waves   : chunked recipes in dependency waves; chunk = raw_id range; each chunk is ONE txn: rows + row-count counter
              update (the app's own managed DML block) + progress row. Crash -> only whole chunks remain; rerunning the
              SAME script skips committed chunks (resume, no double insert)
    rebuild : re-add saved constraints that are missing (one ALTER per table, validated in one pass), in parallel

Timing (MEASURED PROCESSING TOTAL): the worker phase runs in parallel, so per-chunk seconds must NOT be summed into the
total (that would report CPU-sum, ~ the serial time). The caller records the envelope's remote wall time as the counted
item and per-phase / per-chunk numbers as include_in_total=False diagnostics.
"""
from __future__ import annotations

import hashlib
import json
import re

PROGRESS_TABLE = "public.pgdm_expand_progress"
DATA_TABLE_ROW_COUNTS = '"public"."pgdm_table_row_counts"'  # same as postgres_service.DATA_TABLE_ROW_COUNTS
PSQL = "psql -X -qAt -v ON_ERROR_STOP=1"
# synchronous_commit=off only: chunks are checkpointed by the progress table, and a lost async commit is just a chunk that
# reruns. Bigger work_mem gave no speed at 300k rows (16.7 vs 18.7 s) and costs RAM per worker, so it stays at the default.
SESSION_GUCS = "-c synchronous_commit=off"
REBUILD_MAINTENANCE_WORK_MEM = "256MB"  # index builds for the re-added UNIQUE constraints sort the whole table


def _lit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _heredoc(path: str, text: str) -> str:
    assert "\nPGDM_EOF\n" not in text
    return f"cat > {path} <<'PGDM_EOF'\n{text.rstrip()}\nPGDM_EOF\n"


def run_key(database_name: str, source_relation_sql: str, destinations: list[dict]) -> str:
    """Stable id of one logical Expand request: same request -> same key -> resume; any change -> fresh run."""
    payload = json.dumps([database_name, source_relation_sql,
                          [[d["table_name"], d["statements"]] for d in destinations]], sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:24]


_NOT_CHUNKABLE = re.compile(
    r"\b(distinct|group\s+by|having|over|window|limit|offset|union|except|intersect|count|sum|avg|min|max|array_agg|"
    r"string_agg|json_agg|jsonb_agg|json_object_agg|jsonb_object_agg|bool_and|bool_or|every)\b", re.IGNORECASE)
_SQL_STRING = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def is_chunkable(statement: str, source_relation_sql: str) -> bool:
    """A recipe may run per raw_id range only if the union of chunk results equals the full result: exactly one
    source reference and no aggregation / dedup / set operation / row limiting (conservative keyword test)."""
    if statement.count(source_relation_sql) != 1:
        return False
    return not _NOT_CHUNKABLE.search(_SQL_STRING.sub("''", statement.replace(source_relation_sql, "")))


def plan_waves(destinations: list[dict], source_relation_sql: str) -> list[list[dict]]:
    """Dependency levels from read_relations (a recipe only reads EARLIER destinations, enforced by the validator);
    every destination of a level runs in the same parallel wave. Returns [[{table_name, chunked}]]."""
    level, names = {}, {(d["schema_name"], d["pure_table_name"]): d["table_name"] for d in destinations}
    for d in destinations:
        deps = [names[tuple(r)] for r in d.get("read_relations") or [] if tuple(r) in names]
        level[d["table_name"]] = 1 + max((level[x] for x in deps), default=-1)
    waves = [[] for _ in range(max(level.values()) + 1)]
    for d in destinations:
        waves[level[d["table_name"]]].append({
            "table_name": d["table_name"],
            "chunked": all(is_chunkable(st, source_relation_sql) for st in d["statements"])})
    for w in waves:  # unchunked (single big statements) first so they overlap with the chunk stream
        w.sort(key=lambda j: j["chunked"])
    return waves


def read_by_others(destinations: list[dict]) -> set[str]:
    names = {(d["schema_name"], d["pure_table_name"]): d["table_name"] for d in destinations}
    return {names[tuple(r)] for d in destinations for r in d.get("read_relations") or [] if tuple(r) in names}


def chunk_ranges(lo: int, hi: int, chunks: int) -> list[tuple[int, int]]:
    """(lo, hi] raw_id ranges covering (min-1, max]."""
    step = max(1, -(-(hi - lo) // chunks))
    out, a = [], lo
    while a < hi:
        out.append((a, min(hi, a + step)))
        a += step
    return out or [(lo, hi)]


TIMING_TABLE_DDL = """CREATE TEMP TABLE IF NOT EXISTS pgdm_expansion_timings (
    sequence_number integer PRIMARY KEY, stage_key text NOT NULL, item_name text NOT NULL,
    started_at timestamptz NOT NULL, finished_at timestamptz, rows_affected bigint,
    destination_name text, statement_index integer) ON COMMIT PRESERVE ROWS;"""


def _chunk_sql(key, dest, idx, lo, hi, statement, dml_builder, ranged_source, source_relation_sql):
    """One chunk = one txn: the app's managed DML block (insert + row-count counter + timing row in this worker
    session's temp table), then the progress row built from that timing row."""
    if ranged_source is not None:
        statement = statement.replace(source_relation_sql, ranged_source.format(lo=lo, hi=hi))
    dml = dml_builder(statement, dest, idx)
    unit = f"{dest['table_name']}#{idx}"
    return (
        f"\\set VERBOSITY verbose\n{TIMING_TABLE_DDL}\nTRUNCATE pg_temp.pgdm_expansion_timings;\n"
        "BEGIN;\n"
        # serialises the same chunk across runs (e.g. an orphaned worker of a dropped SSH session vs the resumed run):
        # the second waiter then sees the first one's committed progress row and skips
        f"SELECT pg_advisory_xact_lock(hashtextextended({_lit(f'pgdm_chunk:{key}:{unit}:{lo}')}, 0));\n"
        f"SELECT NOT EXISTS (SELECT 1 FROM {PROGRESS_TABLE} WHERE run_key = {_lit(key)} AND kind = 'chunk' "
        f"AND unit = {_lit(unit)} AND lo = {lo}) AS pgdm_todo \\gset\n"
        "\\if :pgdm_todo\n"
        f"{dml}\n"
        f"INSERT INTO {PROGRESS_TABLE} (run_key, kind, unit, lo, hi, rows, seconds, payload) SELECT {_lit(key)}, 'chunk', "
        f"{_lit(unit)}, {lo}, {hi}, rows_affected, EXTRACT(EPOCH FROM finished_at - started_at), txid_current()::text "
        "FROM pg_temp.pgdm_expansion_timings;\n"
        "\\endif\n"
        "COMMIT;\n"
    )


def _defer_sql(key, dests, source_regclass):
    """Save + drop deferrable constraints in ONE txn (catalog-driven, so it adapts to the real DDL)."""
    rows = []
    for d in dests:
        rows.append(f"({_lit(d['qualified_name'])}::regclass, {'true' if d['read_by_others'] else 'false'})")
    return f"""
BEGIN;
SET LOCAL search_path = pg_catalog, pg_temp;  -- saved names and definitions come out fully schema-qualified
CREATE TEMP TABLE pgdm_dest (rel regclass, read_by_others boolean) ON COMMIT DROP;
INSERT INTO pgdm_dest VALUES {', '.join(rows)};
INSERT INTO {PROGRESS_TABLE} (run_key, kind, unit, lo, hi, payload)
SELECT {_lit(key)}, 'constraint', c.conname, 0, 0,
       json_build_object('table', c.conrelid::regclass::text, 'def', pg_get_constraintdef(c.oid))::text
FROM pg_constraint c JOIN pgdm_dest d ON d.rel = c.conrelid
WHERE (c.contype = 'f' AND c.confrelid <> {_lit(source_regclass)}::regclass)
   OR (c.contype = 'u' AND NOT d.read_by_others
       AND NOT EXISTS (SELECT 1 FROM pg_constraint r WHERE r.contype = 'f' AND r.conindid = c.conindid))
ON CONFLICT DO NOTHING;
DO $d$ DECLARE r record; BEGIN
  FOR r IN SELECT unit, payload::json->>'table' AS tbl FROM {PROGRESS_TABLE}
           WHERE run_key = {_lit(key)} AND kind = 'constraint' LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I', r.tbl, r.unit);
  END LOOP;
END $d$;
COMMIT;
""".strip()


def _rebuild_sql(key, table):
    return f"""
SET maintenance_work_mem = {_lit(REBUILD_MAINTENANCE_WORK_MEM)};
DO $r$ DECLARE r record; parts text[] := '{{}}'; BEGIN
  FOR r IN SELECT unit, payload::json->>'def' AS def FROM {PROGRESS_TABLE}
           WHERE run_key = {_lit(key)} AND kind = 'constraint' AND payload::json->>'table' = {_lit(table)}
             AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = unit AND c.conrelid = {_lit(table)}::regclass)
           ORDER BY unit LOOP
    parts := parts || format('ADD CONSTRAINT %I %s', r.unit, r.def);
  END LOOP;
  IF cardinality(parts) > 0 THEN EXECUTE format('ALTER TABLE %s %s', {_lit(table)}, array_to_string(parts, ', ')); END IF;
END $r$;
""".strip()


# SQLSTATE classes that fail the same way on every retry (data exception, integrity, syntax/access, PL/pgSQL raise):
# such a run is undone instead of offered for resume. Anything else (killed backend, restart, deadlock) stays resumable.
FATAL_SQLSTATE_RE = "ERROR:  (22|23|42|P0)[0-9A-Z]{3}:"


def _undo_sql(key, waves, tables, by_name):
    """Remove every row this run committed (rows carry their chunk's xid as xmin), restore the row counters and the
    deferred constraints, and forget the checkpoint: the database ends as it was before the Expand."""
    parts = []
    for w in reversed(waves):  # children first (FKs)
        for job in w:
            t, d = job["table_name"], by_name[job["table_name"]]
            parts.append(f"""
DELETE FROM {t} WHERE xmin::text::bigint IN (SELECT payload::bigint % 4294967296 FROM {PROGRESS_TABLE}
  WHERE run_key = {_lit(key)} AND kind = 'chunk' AND unit LIKE {_lit(t + '#%')});
UPDATE {DATA_TABLE_ROW_COUNTS} c SET row_count = c.row_count - s.n, updated_at = clock_timestamp()
FROM (SELECT coalesce(sum(rows), 0) AS n FROM {PROGRESS_TABLE} WHERE run_key = {_lit(key)} AND kind = 'chunk'
      AND unit LIKE {_lit(t + '#%')}) s
WHERE c.schema_name = {_lit(d['schema_name'])} AND c.table_name = {_lit(d['pure_table_name'])};""")
    parts += [_rebuild_sql(key, t) for t in tables]
    parts.append(f"DELETE FROM {PROGRESS_TABLE} WHERE run_key = {_lit(key)};")
    return "\n".join(p.strip() for p in parts)


def build_parallel_expansion_script(*, database_name, sql_username, source_qualified, source_relation_sql, manifest_sql,
                                    destinations, waves, dml_builder, lo, hi, workers=4, chunks=16, key=None, psql_cmd=None,
                                    timing_table_sql=None, timing_result_sql=None):
    """destinations: [{table_name, qualified_name, schema_name, pure_table_name, statements, read_by_others}]
    waves: plan_waves(...) output; waves run in order, the jobs of one wave run in parallel (xargs -P workers). dml_builder(statement, destination, statement_index) -> the app's _build_managed_dml_sql(..., 'add',
    timing_sequence=1, ...) block (counter update + timing row)."""
    key = key or run_key(database_name, source_relation_sql, destinations)
    by_name = {d["table_name"]: d for d in destinations}
    # chunk filter appended inside the app's guarded source subquery
    assert source_relation_sql.rstrip().endswith(")")
    ranged = source_relation_sql.rstrip()[:-1].rstrip() + "\n      AND raw_id > {lo} AND raw_id <= {hi}\n)"
    ranges = chunk_ranges(lo, hi, chunks)
    psql = psql_cmd or f"{PSQL} -h localhost -U {sql_username} -d {database_name}"
    out = ["#!/bin/bash", "set -euo pipefail", f"export PGOPTIONS='{SESSION_GUCS}'",
           'W=$(mktemp -d); trap \'rm -rf "$W"\' EXIT', "export W", ""]
    expected = []
    phase_lines = ["set -euo pipefail", "ts() { date +%s%N; }",
                   'exec 3>&2 2>>"$W/stderr"',
                   f"trap 'grep -m1 -E \"{FATAL_SQLSTATE_RE}\" \"$W/stderr\" | sed \"s/^psql:[^ ]* //\" > \"$W/fatal\" || true; cat \"$W/stderr\" >&3' EXIT",
                   'phase() { local t=$(ts) n=$1; shift; "$@"; '
                   'echo "{\\"pgdm_phase\\": \\"$n\\", \\"seconds\\": $(( ($(ts) - t) / 1000000 ))e-3}"; }']
    out.append(_heredoc("$W/defer.sql", _defer_sql(key, destinations, source_qualified)))
    phase_lines.append(f'phase "Defer destination constraints" {psql} -f "$W/defer.sql"')
    for w, jobs in enumerate(waves):
        files = []
        for job in jobs:
            name = job["table_name"]
            d = by_name[name]
            for si, stmt in enumerate(d["statements"], start=1):
                parts = ranges if job["chunked"] else [(lo, hi)]
                for ci, (a, b) in enumerate(parts):
                    f = f"$W/w{w}_{re.sub(r'[^A-Za-z0-9]', '_', name)}_{si}_{ci:05d}.sql"
                    out.append(_heredoc(f, _chunk_sql(key, d, si, a, b, stmt, dml_builder,
                                                      ranged if job["chunked"] else None, source_relation_sql)))
                    files.append(f)
                    expected.append((f"{name}#{si}", a))
        listing = " ; ".join(f"echo \"{f}\"" for f in files)
        label = ", ".join(j["table_name"] + ("" if j["chunked"] else " (single statement)") for j in jobs)
        phase_lines.append(f"phase \"Wave {w}: {label}\" sh -c '{{ {listing} ; }} | xargs -P {workers} -I{{}} "
                           f"{psql} -f {{}}'")
    tables = sorted({d["qualified_name"] for d in destinations})
    for i, t in enumerate(tables):
        out.append(_heredoc(f"$W/rebuild_{i}.sql", _rebuild_sql(key, t)))
    phase_lines.append(f"phase \"Rebuild deferred constraints\" sh -c 'ls \"$W\"/rebuild_*.sql | xargs -P {workers} "
                       f"-I{{}} {psql} -f {{}}'")
    out.append(_heredoc("$W/phases.sh", "\n".join(phase_lines)))
    exp_values = ", ".join(f"({_lit(u)}, {a})" for u, a in expected)
    locks = "".join(f"SELECT pg_advisory_lock(hashtextextended({_lit('pgdm_expand:' + d['qualified_name'])}, 0));\n"
                    for d in sorted(destinations, key=lambda d: d["qualified_name"]))
    def _t(seq, name):  # same server-side timing rows as the serial batch (diagnostic in the parallel path)
        return (f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({seq}, 'execute', {_lit(name)}, clock_timestamp(), "
                "NULL, NULL, NULL, NULL);\n") if timing_table_sql else ""

    def _t_end(seq):
        return (f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = {seq};\n"
                if timing_table_sql else "")

    undo = _undo_sql(key, waves, tables, by_name)
    coord = f"""
CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} (run_key text, kind text, unit text, lo bigint, hi bigint,
    rows bigint, seconds double precision, payload text, done_at timestamptz DEFAULT clock_timestamp(), PRIMARY KEY (run_key, kind, unit, lo));
{timing_table_sql or ""}
BEGIN;
{_t(2, "Acquire destination table locks")}LOCK TABLE {source_qualified} IN SHARE MODE;
{locks}{_t_end(2)}{_t(3, "Scan source and build dependency manifest")}{manifest_sql}
{_t_end(3)}\\! bash "$W/phases.sh"
\\set pgdm_fatal `cat "$W/fatal" 2>/dev/null || true`
SELECT :'pgdm_fatal' <> '' AS pgdm_is_fatal, set_config('pgdm.fatal', :'pgdm_fatal', false) AS pgdm_fatal_set \\gset
\\if :pgdm_is_fatal
{undo}
COMMIT;
DO $f$ BEGIN RAISE EXCEPTION 'expansion failed, all rows of this run were removed and nothing was registered: %', current_setting('pgdm.fatal'); END $f$;
\\endif
DO $v$ DECLARE missing bigint; BEGIN
  SELECT count(*) INTO missing FROM (VALUES {exp_values}) e(unit, lo) WHERE NOT EXISTS (SELECT 1 FROM {PROGRESS_TABLE} p
    WHERE p.run_key = {_lit(key)} AND p.kind = 'chunk' AND p.unit = e.unit AND p.lo = e.lo);
  IF missing > 0 THEN RAISE EXCEPTION 'expansion incomplete: % chunk(s) not committed; rerun the same Expand to resume', missing; END IF;
  SELECT count(*) INTO missing FROM {PROGRESS_TABLE} p WHERE p.run_key = {_lit(key)} AND p.kind = 'constraint'
    AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conname = p.unit AND c.conrelid = (p.payload::json->>'table')::regclass);
  IF missing > 0 THEN RAISE EXCEPTION 'expansion incomplete: % deferred constraint(s) not rebuilt; rerun to resume', missing; END IF;
END $v$;
SELECT json_build_object('pgdm_chunks', json_agg(json_build_object('unit', unit, 'lo', lo, 'rows', rows, 'seconds', seconds)))::text
FROM {PROGRESS_TABLE} WHERE run_key = {_lit(key)} AND kind = 'chunk';
-- completed: forget the checkpoint in the same commit, so only an INCOMPLETE run is ever resumed and a later identical
-- request runs fresh (as the original app does)
DELETE FROM {PROGRESS_TABLE} WHERE run_key = {_lit(key)};
{_t(9000, "Commit expansion transaction")}COMMIT;
{_t_end(9000)}{timing_result_sql or ""}
""".strip()
    out.append(_heredoc("$W/coord.sql", coord))
    out.append(f'{psql} -f "$W/coord.sql"')
    return "\n".join(out) + "\n", key
