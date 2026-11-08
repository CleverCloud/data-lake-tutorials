"""A Spark batch on an Iceberg table of the lake, through the Lakekeeper catalogue.

The tutorial runs this script in several Jobs, each one its own Spark driver with its executors.
Two of them write to the same table at the same time: Lakekeeper accepts one commit at a time, and
Iceberg retries a commit that comes second on the new version of the table.

    spark-submit iceberg_job.py init     creates the table with three rows, or replaces its rows
    spark-submit iceberg_job.py write    commits rows one by one, as writer JOB_COMPLETION_INDEX + 1
    spark-submit iceberg_job.py check    counts the rows of each writer, and fails if any is missing
    spark-submit iceberg_job.py compute  a CPU batch in many tasks, with no table: the executors follow
"""

import json
import os
import sys
import time

from pyspark.sql import SparkSession

TABLE = "lake.batch.events"


def session(builder: SparkSession.Builder | None = None) -> SparkSession:
    """A session whose `lake` catalogue is Lakekeeper, with Cellar for the files.

    Without LAKEKEEPER_URI, `lake` is a local Hadoop catalogue in WAREHOUSE, which the test uses.
    """
    builder = (builder or SparkSession.builder.appName("iceberg")).config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ).config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    if os.environ.get("LAKEKEEPER_URI"):
        builder = (
            builder.config("spark.sql.catalog.lake.type", "rest")
            .config("spark.sql.catalog.lake.uri", os.environ["LAKEKEEPER_URI"])
            .config("spark.sql.catalog.lake.warehouse", "lake")
            # Iceberg writes the files itself, straight to their place. Its S3 client, like Hadoop's
            # below, reads the Cellar keys from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the
            # environment: the keys never enter Spark's configuration, nor the tasks sent to executors.
            .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lake.s3.endpoint", f"https://{os.environ['CELLAR_HOST']}")
            .config("spark.sql.catalog.lake.s3.path-style-access", "true")
            .config("spark.sql.catalog.lake.client.region", "us-east-1")
            # Other files on Cellar, such as raw Parquet or CSV, are read through Hadoop's S3 connector.
            .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['CELLAR_HOST']}")
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # The table's files are at s3:// paths. remove_orphan_files lists them through Hadoop, so
            # s3:// goes through the same connector as s3a://.
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    else:
        builder = builder.config("spark.sql.catalog.lake.type", "hadoop").config(
            "spark.sql.catalog.lake.warehouse", os.environ["WAREHOUSE"]
        )
    return builder.getOrCreate()


def init(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.batch")
    # A commit that comes second is retried on the new version of the table, up to this many times.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {TABLE} (writer INT, n INT) USING iceberg "
        "TBLPROPERTIES ('commit.retry.num-retries' = '20')"
    )
    spark.sql(f"INSERT OVERWRITE {TABLE} VALUES (0, 1), (0, 2), (0, 3)")


def write(spark: SparkSession, writer: int, commits: int) -> None:
    """Commits one row at a time, so that writers running at once compete for every commit."""
    for n in range(commits):
        spark.sql(f"INSERT INTO {TABLE} VALUES ({writer}, {n})")


def check(spark: SparkSession, writers: int, commits: int) -> dict:
    rows = spark.sql(f"SELECT writer, count(*) AS n FROM {TABLE} GROUP BY writer").collect()
    found = {row.writer: row.n for row in sorted(rows)}
    expected = {0: 3, **{writer: commits for writer in range(1, writers + 1)}}
    snapshots = spark.sql(f"SELECT count(*) FROM {TABLE}.snapshots").first()[0]
    return {"rows_per_writer": found, "expected": expected, "snapshots": snapshots, "ok": found == expected}


def compute(spark: SparkSession, rows: int, partitions: int) -> dict:
    """Hashes `rows` numbers in `partitions` tasks. It reads and writes nothing: only CPU, so that
    dynamic allocation has work to spread over executors."""
    start = time.monotonic()
    total = spark.range(0, rows, 1, partitions).selectExpr("sum(hash(id)) AS h").first().h
    return {"rows": rows, "partitions": partitions, "checksum": total, "seconds": round(time.monotonic() - start)}


def main() -> int:
    spark = session()
    writers, commits = int(os.environ.get("WRITERS", "2")), int(os.environ.get("COMMITS", "10"))
    action, status = sys.argv[1], 0
    if action == "init":
        init(spark)
    elif action == "write":
        write(spark, int(os.environ["JOB_COMPLETION_INDEX"]) + 1, commits)
    elif action == "check":
        summary = check(spark, writers, commits)
        print(json.dumps(summary), flush=True)
        status = 0 if summary["ok"] else 1
    elif action == "compute":
        rows, partitions = int(os.environ.get("COMPUTE_ROWS", "20000000000")), int(os.environ.get("COMPUTE_PARTITIONS", "96"))
        print(json.dumps(compute(spark, rows, partitions)), flush=True)
    spark.stop()
    return status


if __name__ == "__main__":
    sys.exit(main())
