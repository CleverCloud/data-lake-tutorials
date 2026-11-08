"""A writer of the Delta table, as a Spark Connect client.

The tutorial runs this script in several Jobs at once. Each one connects to the Spark Connect
server named in SPARK_REMOTE and sends it SQL: the server's driver runs the queries and commits
the writes one after the other. The pod needs neither Java nor Spark, only `pyspark-client`.

    python delta_client.py init     creates the table with three rows, or replaces its rows
    python delta_client.py write    commits rows one by one, as writer JOB_COMPLETION_INDEX + 1
    python delta_client.py check    counts the rows of each writer, and fails if any is missing
"""

import json
import os
import sys

from pyspark.sql import SparkSession


def table(path: str) -> str:
    return f"delta.`{path}`"


def init(spark: SparkSession, path: str) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table(path)} (writer INT, n INT) USING DELTA")
    spark.sql(f"INSERT OVERWRITE {table(path)} VALUES (0, 1), (0, 2), (0, 3)")


def write(spark: SparkSession, path: str, writer: int, commits: int) -> None:
    """Commits one row at a time, so that writers running at once compete for every commit."""
    for n in range(commits):
        spark.sql(f"INSERT INTO {table(path)} VALUES ({writer}, {n})")


def check(spark: SparkSession, path: str, writers: int, commits: int) -> dict:
    rows = spark.sql(f"SELECT writer, count(*) AS n FROM {table(path)} GROUP BY writer").collect()
    found = {row.writer: row.n for row in sorted(rows)}
    expected = {0: 3, **{writer: commits for writer in range(1, writers + 1)}}
    version = spark.sql(f"DESCRIBE HISTORY {table(path)}").agg({"version": "max"}).first()[0]
    return {"rows_per_writer": found, "expected": expected, "version": version, "ok": found == expected}


def main() -> int:
    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    path = f"s3a://{os.environ['LAKE_BUCKET']}/delta/{os.environ.get('TABLE', 'events')}"
    writers, commits = int(os.environ.get("WRITERS", "2")), int(os.environ.get("COMMITS", "10"))
    action = sys.argv[1]
    if action == "init":
        init(spark, path)
    elif action == "write":
        write(spark, path, int(os.environ["JOB_COMPLETION_INDEX"]) + 1, commits)
    elif action == "check":
        summary = check(spark, path, writers, commits)
        print(json.dumps(summary), flush=True)
        return 0 if summary["ok"] else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
