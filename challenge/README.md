# Parallel, resumable Expand (challenge entry)

Fork of [igorsiecz/kaggle_postgre_challenge](https://github.com/igorsiecz/kaggle_postgre_challenge) (Apache-2.0, upstream
commit `5dfde9d`), made for the Kaggle "Python & PostgreSQL Data Pipeline Optimization Challenge". The recipes in
`SQL files/`, the generator, the validator, the version registration and the Windows -> SSH -> Linux architecture are
unchanged. Only the way the Expand batch is executed on the server changes.

## Idea
The original Expand runs the six recipes in one psql session, inside one REPEATABLE READ transaction, one statement after
another. Each statement is one serial INSERT ... SELECT over the whole source. The new executor:

1. Sends ONE bash script over the existing SSH connection (`bash -s`). It needs only bash, coreutils, xargs and psql on the
   server; no server-side Python and no extensions.
2. A coordinator session freezes the source (`LOCK raw_data IN SHARE MODE`, READ COMMITTED) and runs the unchanged
   manifest SQL, so all workers see the same source rows.
3. Orders the recipes in waves from their read relations (table3 + table4, then table1, then table2 + table5 + table6).
4. Splits every chunkable recipe (no DISTINCT or aggregate) into raw_id ranges. Each range runs in its own transaction
   in one of K parallel psql workers (`xargs -P K`). The transaction contains the app's own managed DML block (insert,
   row counter, timing row) plus a checkpoint row.
5. Before the load, drops the FK and UNIQUE constraints that no recipe in the batch depends on. Their definitions are
   read from the catalog and saved in the checkpoint table. After the load it rebuilds them and verifies every chunk and
   every constraint. The FK to the source is kept.
6. A deterministic SQL error (bad input value, constraint violation) undoes the whole run and reports the error, so the
   result matches the original's rollback (see Limits).
7. If anything fails (worker killed, server restart, SSH loss) only whole chunks remain. Rerunning the same Expand skips
   them, finishes the rest, rebuilds, verifies and then registers the versions. The original has no resume path.

## Settings (environment variables on the manager)
| variable | default | meaning |
|---|---|---|
| `PGDM_EXPAND_WORKERS` | server `nproc` (max 16) | parallel psql workers; `1` = original serial batch |
| `PGDM_EXPAND_CHUNKS` | 4 x workers | raw_id ranges per chunked recipe |

The executor falls back to the original batch when the source has no integer `raw_id` column.

## Check it without the GUI
`challenge/run_check.sh [ROWS]` (Linux or WSL, PostgreSQL 16, libpq env vars for the connection) runs the unit tests,
generates the official database with `generate_database.py`, then runs the serial baseline and the parallel executor.
It compares id-insensitive digests of all six tables (`challenge/digest.sql`) and the row counters, and finally kills
the workers mid-run and checks that a resumed run gives identical results. Last, it corrupts one source value and
checks that both paths fail and leave the tables, counters and constraints unchanged; step 8 does the same for a
violated deferred constraint. `SKIP_GEN=1` reuses an existing database.

## Measured with the GUI (MEASURED PROCESSING TOTAL)
Run Expand once with `PGDM_EXPAND_WORKERS=1` (original path) and once with the default. In the parallel path the server
part is counted once, as the remote wall-clock time of the script. The per-chunk PostgreSQL timings are still shown, but as
diagnostics outside the total, because summing parallel work would report CPU time rather than elapsed time.

## Limits
- Deterministic SQL errors (SQLSTATE classes 22 data, 23 integrity, 42 syntax/access, P0 raise) are not resumable, so
  the executor undoes the run instead: it deletes every row the run committed (each chunk records its transaction id,
  rows are matched by `xmin`), restores the row counters and the deferred constraints, clears the checkpoint and reports
  the original error. The database ends as before the Expand, as with the original's rollback. This is tested end to end
  with a malformed source value (step 7 of `run_check.sh`) and with a deferred UNIQUE constraint that the data violates
  (step 8). It also works when the tables already held rows from an earlier Expand: only this run's rows are removed.
- While a run is undone, its rows were briefly visible to other sessions. The original never shows partial rows.
- Surrogate ids are assigned per chunk, so ids interleave differently from the serial run. All natural keys and joins
  are identical.
- Faster only when the server has more than one core. With one core, use `PGDM_EXPAND_WORKERS=1`.

## Credits
Original application, generator and recipes: igorsiecz (Apache-2.0, see `LICENSE`). Changed files:
`services/postgres_service.py`. New files: `services/parallel_expansion.py`, `tests/test_parallel_expansion.py`,
`challenge/`. Written with AI assistance (Claude Code) on behalf of the fork owner (Aadarsh-T).
