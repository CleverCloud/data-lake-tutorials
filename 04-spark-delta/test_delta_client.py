"""Runs the writers' example on a local Spark, against a directory instead of Cellar.

On the cluster the writers are separate pods, sending SQL to one Spark Connect server. Here they
are threads sharing one local Spark session: in both cases, one driver commits every write.
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


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[4]")
        .appName("test")
        # Spark's own warehouse directory goes to a temporary place, not to the repository.
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("spark-warehouse")))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def run_example(spark, path, writers, commits):
    import delta_client

    delta_client.init(spark, path)
    with ThreadPoolExecutor(writers) as pool:
        list(pool.map(lambda writer: delta_client.write(spark, path, writer, commits), range(1, writers + 1)))
    return delta_client.check(spark, path, writers, commits)


@needs_spark
def test_writers_at_the_same_time_lose_no_row(spark, tmp_path):
    summary = run_example(spark, str(tmp_path / "events"), writers=3, commits=4)
    assert summary["rows_per_writer"] == {0: 3, 1: 4, 2: 4, 3: 4}
    # The table's creation, the first rows, then one commit per row written by the writers.
    assert summary["version"] == 13
    assert summary["ok"]


@needs_spark
def test_init_replaces_the_rows_so_the_example_runs_again(spark, tmp_path):
    path = str(tmp_path / "events")
    run_example(spark, path, writers=2, commits=2)
    summary = run_example(spark, path, writers=2, commits=2)
    assert summary["rows_per_writer"] == {0: 3, 1: 2, 2: 2}
    assert summary["ok"]


@needs_spark
def test_a_missing_row_fails_the_check(spark, tmp_path):
    import delta_client

    path = str(tmp_path / "events")
    delta_client.init(spark, path)
    delta_client.write(spark, path, 1, 3)
    assert not delta_client.check(spark, path, writers=2, commits=3)["ok"]
