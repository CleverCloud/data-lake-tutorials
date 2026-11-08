"""Runs the batch's example on a local Spark, with a local Iceberg catalogue instead of Lakekeeper.

On the cluster the writers are separate drivers, committing through Lakekeeper. Here they are
threads of one local session, committing to a catalogue in a temporary directory.
"""

import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pytest


def java_available() -> bool:
    java = shutil.which("java", path=os.path.join(os.environ.get("JAVA_HOME", ""), "bin")) or shutil.which("java")
    if not java:
        return False
    version = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
    return any(f'"{major}.' in version for major in ("17", "21"))


needs_spark = pytest.mark.skipif(not java_available(), reason="Spark 4 needs Java 17 or 21 on JAVA_HOME")


@pytest.fixture
def spark(tmp_path, monkeypatch):
    from pyspark.sql import SparkSession

    import iceberg_job

    monkeypatch.delenv("LAKEKEEPER_URI", raising=False)
    monkeypatch.setenv("WAREHOUSE", str(tmp_path / "warehouse"))
    builder = (
        SparkSession.builder.master("local[4]")
        .appName("test")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0")
        # Spark's own warehouse directory goes to a temporary place, not to the repository.
        .config("spark.sql.warehouse.dir", str(tmp_path / "spark-warehouse"))
    )
    session = iceberg_job.session(builder)
    yield session
    session.stop()


def run_example(spark, writers, commits):
    import iceberg_job

    iceberg_job.init(spark)
    with ThreadPoolExecutor(writers) as pool:
        list(pool.map(lambda writer: iceberg_job.write(spark, writer, commits), range(1, writers + 1)))
    return iceberg_job.check(spark, writers, commits)


@needs_spark
def test_writers_at_the_same_time_lose_no_row(spark):
    summary = run_example(spark, writers=3, commits=4)
    assert summary["rows_per_writer"] == {0: 3, 1: 4, 2: 4, 3: 4}
    # One snapshot for the first rows, then one per row written by the writers.
    assert summary["snapshots"] == 13
    assert summary["ok"]


@needs_spark
def test_init_replaces_the_rows_so_the_example_runs_again(spark):
    run_example(spark, writers=2, commits=2)
    summary = run_example(spark, writers=2, commits=2)
    assert summary["rows_per_writer"] == {0: 3, 1: 2, 2: 2}
    assert summary["ok"]


@needs_spark
def test_a_missing_row_fails_the_check(spark):
    import iceberg_job

    iceberg_job.init(spark)
    iceberg_job.write(spark, 1, 3)
    assert not iceberg_job.check(spark, writers=2, commits=3)["ok"]


@needs_spark
def test_compute_hashes_every_row_in_the_given_tasks(spark):
    import iceberg_job

    summary = iceberg_job.compute(spark, rows=1000, partitions=8)
    expected = spark.range(0, 1000).selectExpr("sum(hash(id)) AS h").first().h
    assert summary["checksum"] == expected
    assert (summary["rows"], summary["partitions"]) == (1000, 8)
