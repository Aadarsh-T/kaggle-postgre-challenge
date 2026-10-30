import os, sys, unittest
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "services"))
import parallel_expansion as pe

SRC = "(\n    SELECT *\n    FROM \"public\".\"raw_data\"\n    WHERE \"raw_schema\" IN ('group001')\n)"


def dest(name, stmt, reads=()):
    return {"table_name": f"public.{name}", "schema_name": "public", "pure_table_name": name,
            "qualified_name": f"public.{name}", "statements": [stmt], "read_relations": [["public", r] for r in reads]}


class ParallelExpansionTest(unittest.TestCase):
    def test_chunk_ranges_cover_exactly(self):
        r = pe.chunk_ranges(0, 1003, 16)
        self.assertEqual(r[0][0], 0)
        self.assertEqual(r[-1][1], 1003)
        self.assertTrue(all(a[1] == b[0] for a, b in zip(r, r[1:])))

    def test_chunkable_detection(self):
        self.assertTrue(pe.is_chunkable(f"INSERT INTO t (a) SELECT s.raw_id FROM {SRC} AS s", SRC))
        self.assertFalse(pe.is_chunkable(f"INSERT INTO t (a) SELECT DISTINCT s.raw_hash FROM {SRC} AS s", SRC))
        self.assertFalse(pe.is_chunkable(f"INSERT INTO t (a) SELECT count(*) FROM {SRC} AS s", SRC))
        self.assertFalse(pe.is_chunkable(f"INSERT INTO t SELECT * FROM {SRC} a JOIN {SRC} b USING (raw_id)", SRC))
        # keywords inside string literals do not count
        self.assertTrue(pe.is_chunkable(f"INSERT INTO t (a) SELECT 'distinct' FROM {SRC} AS s", SRC))

    def test_waves_follow_read_relations(self):
        ds = [dest("table3", f"INSERT INTO public.table3 SELECT DISTINCT x FROM {SRC} s"),
              dest("table1", f"INSERT INTO public.table1 SELECT s.raw_id FROM {SRC} s JOIN public.table3 t ON true", ["table3"]),
              dest("table2", f"INSERT INTO public.table2 SELECT m.id FROM {SRC} s JOIN public.table1 m ON true", ["table1"])]
        waves = pe.plan_waves(ds, SRC)
        self.assertEqual([[j["table_name"] for j in w] for w in waves], [["public.table3"], ["public.table1"], ["public.table2"]])
        self.assertEqual([[j["chunked"] for j in w] for w in waves], [[False], [True], [True]])
        self.assertEqual(pe.read_by_others(ds), {"public.table3", "public.table1"})

    def test_script_has_resume_and_timing_contract(self):
        ds = [dict(dest("table1", f"INSERT INTO public.table1 SELECT s.raw_id FROM {SRC} s"), read_by_others=False)]
        script, key = pe.build_parallel_expansion_script(
            database_name="db", sql_username="u", source_qualified="public.raw_data", source_relation_sql=SRC,
            manifest_sql="SELECT 1;", destinations=ds, waves=pe.plan_waves(ds, SRC),
            dml_builder=lambda st, d, i: f"DO $x$ BEGIN {st}; END $x$;", lo=0, hi=100, chunks=4,
            timing_table_sql="CREATE TEMP TABLE pgdm_expansion_timings ();", timing_result_sql="SELECT 2;")
        self.assertEqual(script.count("AND raw_id > "), 4)
        for s in ("LOCK TABLE public.raw_data IN SHARE MODE", "Acquire destination table locks",
                  "Scan source and build dependency manifest", "Commit expansion transaction",
                  "rerun the same Expand to resume", f"DELETE FROM {pe.PROGRESS_TABLE} WHERE run_key = '{key}'"):
            self.assertIn(s, script)
        self.assertEqual(key, pe.run_key("db", SRC, ds))


if __name__ == "__main__":
    unittest.main()
