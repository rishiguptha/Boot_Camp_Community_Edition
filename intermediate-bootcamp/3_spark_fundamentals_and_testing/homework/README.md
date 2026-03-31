# Halo Spark Analysis

Spark job built as part of Zach Wilson's Boot Camp Community Edition — Module 4 (Apache Spark).

The dataset is Halo match data: player performance per match, medal breakdowns, map info. The goal was to practice join strategies and see how they show up in the physical plan, not just write aggregations.

---

## What it does

Joins 5 tables using two different strategies, then answers 4 analytical questions about the data.

**Join strategies:**

- Auto-broadcast disabled via `spark.sql.autoBroadcastJoinThreshold = -1` so Spark can't cheat
- `medals` and `maps` are explicitly broadcast — they're small lookup tables, no reason to shuffle them
- `match_details`, `matches`, and `medals_matches_players` are bucket joined on `match_id` with 16 buckets — the shuffle happens once at write time, zero shuffle at query time

You can verify this in the explain plan: `BroadcastHashJoin` on the small tables, `SortMergeJoin` with no `Exchange` nodes on the big ones.

**Aggregations:**

- Which player averages the most kills per game?
- Which playlist gets played the most?
- Which map gets played the most?
- Which map do players get the most Killing Spree medals on?

**sortWithinPartitions experiment:**

Wrote the same dataset sorted by a low cardinality column vs a high cardinality column and compared output file sizes. The idea: when similar values cluster together, Parquet's compression has more to work with. On a small dataset the difference is minimal, but at scale — sorting by something like `is_team_game` (2 values) vs `match_id` (one UUID per row) would show a significant gap.

---

## How to run it

```bash
# Install dependencies
uv sync

# Run the job
uv run python src/jobs/halo_analysis.py
```

Make sure your CSVs are in the `data/` folder before running. The job is idempotent — run it as many times as you want.

**Directory structure:**

```
spark-homework/
├── src/
│   ├── jobs/
│   │   └── halo_analysis.py
│   └── tests/
│       ├── conftest.py
│       └── test_halo_analysis.py
├── data/
│   ├── match_details.csv
│   ├── matches.csv
│   ├── medals_matches_players.csv
│   ├── medals.csv
│   └── maps.csv
├── output/
│   ├── bucketed_tables/
│   └── sorted_tables/
└── requirements.txt
```

---

## Key learnings

**Bucket joins require writing to disk first.** You can't bucket join in-memory DataFrames — the bucket metadata only exists after writing. The pattern is: write bucketed → read back → join. Learned this the hard way.

**Broadcast vs bucket is about table size.** Broadcast works when one side is small enough to copy to every executor. Bucket join works when both sides are large and you want to pre-pay the shuffle cost at write time. They're not interchangeable.

**External tables fix the idempotency problem.** Using `saveAsTable` without an explicit path creates a managed table — Spark owns the files. `DROP TABLE` removes the metadata but not the files, so rerunning throws an error. Switching to external tables with `.option("path", ...)` makes the job idempotent.

**sortWithinPartitions is a local sort, sort is global.** `sortWithinPartitions` adds no Exchange node — data stays on its partition. `sort` adds an Exchange node because Spark needs to shuffle everything to guarantee global order. For compression purposes, local sort is usually enough.
