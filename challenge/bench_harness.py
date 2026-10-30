"""Bench harness: build the parallel expansion script (services/parallel_expansion.py) for the six recipes in "SQL files"
without the GUI. Usage: python challenge/bench_harness.py "SQL files" (env DB, HI = max raw_id, WORKERS, CHUNKS, OUT)."""
import os, subprocess, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "services"))
import parallel_expansion as pe

RECIPES = sys.argv[1]
SRC = "(\n    SELECT *\n    FROM \"public\".\"raw_data\"\n    WHERE \"raw_schema\" IN ('group001')\n)"
ORDER = [("table3", "01_table3"), ("table4", "02_table4"), ("table1", "03_table1"),
         ("table2", "06_table2"), ("table5", "04_table5"), ("table6", "05_table6")]


def dml(statement, dest, idx):  # same shape as the app's _build_managed_dml_sql(..., "add", timing_sequence=1, ...)
    schema, table = dest["schema_name"], dest["pure_table_name"]
    return (f"DO $pgdm_managed_sql$\nDECLARE pgdm_affected_rows bigint;\n    pgdm_timing_started_at timestamptz := clock_timestamp();\nBEGIN\n    {statement}\n    ;\n"
            "    GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;\n"
            "    UPDATE public.pgdm_table_row_counts SET row_count = row_count + pgdm_affected_rows, updated_at = clock_timestamp()\n"
            f"    WHERE schema_name = '{schema}' AND table_name = '{table}';\n"
            "    IF NOT FOUND THEN RAISE EXCEPTION 'Contador ausente'; END IF;\n"
            "    INSERT INTO pg_temp.pgdm_expansion_timings VALUES (1, 'execute', 'chunk', pgdm_timing_started_at, "
            f"clock_timestamp(), pgdm_affected_rows, '{dest['table_name']}', {idx});\nEND\n$pgdm_managed_sql$;")


dests = []
for t, f in ORDER:
    stmt = open(os.path.join(RECIPES, f + ".sql")).read().replace("{{source}}", SRC).rstrip().rstrip(";").strip()
    dests.append({"table_name": f"public.{t}", "qualified_name": f"public.{t}", "schema_name": "public",
                  "pure_table_name": t, "statements": [stmt], "read_by_others": t in ("table1", "table3", "table4"),
                  "read_relations": {"table1": [["public", "table3"]], "table6": [["public", "table1"], ["public", "table4"]]}.get(
                      t, [["public", "table1"]] if t in ("table2", "table5") else [])})
waves = pe.plan_waves(dests, SRC)
print(waves)
assert {d["table_name"] for d in dests if d["read_by_others"]} == pe.read_by_others(dests)
script, key = pe.build_parallel_expansion_script(
    database_name=os.environ.get("DB", "kaggle_challenge"), sql_username="x", source_qualified="public.raw_data", source_relation_sql=SRC,
    manifest_sql="SELECT json_build_object('expected_row_count', count(*))::text FROM public.raw_data WHERE raw_schema IN ('group001');",
    destinations=dests, waves=waves,
    dml_builder=dml, lo=0, hi=int(os.environ.get("HI", "300000")), workers=int(os.environ.get("WORKERS", "4")), chunks=int(os.environ.get("CHUNKS", "16")), psql_cmd=pe.PSQL,
    timing_table_sql="CREATE TEMP TABLE pgdm_expansion_timings (sequence_number integer PRIMARY KEY, stage_key text NOT NULL, item_name text NOT NULL, started_at timestamptz NOT NULL, finished_at timestamptz, rows_affected bigint, destination_name text, statement_index integer) ON COMMIT PRESERVE ROWS;",
    timing_result_sql="SELECT json_build_object('pgdm_expansion_timings', json_agg(json_build_object('name', item_name, 'duration_seconds', EXTRACT(EPOCH FROM finished_at - started_at)) ORDER BY sequence_number))::text FROM pg_temp.pgdm_expansion_timings;")
open(os.environ.get("OUT", "remote_port.sh"), "w").write(script)
print("key", key, "bytes", len(script))
