"""Runs the server on a local lake and queries it as a remote client would."""

import os
import socket
import subprocess
import sys

import duckdb
import pytest

import serve

TOKEN = "a-token-for-the-tests-only"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# The server runs in its own process, as it does on Clever Cloud: in one process, the client and
# the server would wait on each other.
SERVER = """
import signal, sys, duckdb, serve
root = sys.argv[1]
serve.write_secrets(f"{root}/secrets", f"{root}/catalog.duckdb", f"{root}/data/")
con = serve.open_lake(f"{root}/secrets")
con.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
con.execute("CREATE TABLE hello AS SELECT range AS n FROM range(3)")
serve.serve(con, "localhost", int(sys.argv[2]), sys.argv[3], public=False)
serve.lock_down(con, f"{root}/data/")
print("ready", flush=True)
signal.sigwait({signal.SIGTERM})
"""


@pytest.fixture
def server(tmp_path):
    port = free_port()
    process = subprocess.Popen(
        [sys.executable, "-c", SERVER, str(tmp_path), str(port), TOKEN],
        stdout=subprocess.PIPE, text=True, cwd=os.path.dirname(__file__),
    )
    assert process.stdout.readline().strip() == "ready"
    yield port, tmp_path
    process.terminate()
    process.wait(timeout=10)


def test_a_client_with_the_token_reads_and_writes_the_lake(server):
    port, tmp_path = server
    client = duckdb.connect()
    client.execute(f"ATTACH 'quack:localhost:{port}' AS remote (TOKEN '{TOKEN}')")
    client.execute("INSERT INTO remote.hello VALUES (42)")
    assert client.execute("SELECT count(*) FROM remote.hello").fetchone()[0] == 4
    # With inlining off, every write is a Parquet file in the lake's data path.
    assert len(list((tmp_path / "data").rglob("*.parquet"))) == 2


def test_a_client_without_the_token_is_refused(server):
    port, _ = server
    with pytest.raises(duckdb.Error, match="Authentication failed"):
        duckdb.connect().execute(f"ATTACH 'quack:localhost:{port}' AS remote (TOKEN 'not-the-token')")


def test_a_short_token_is_refused():
    with pytest.raises(ValueError):
        serve.serve(duckdb.connect(), "localhost", free_port(), "short", public=False)


def test_the_lake_is_read_from_the_add_ons_variables():
    lake = serve.lake_from_env(
        {
            "POSTGRESQL_ADDON_DB": "db",
            "POSTGRESQL_ADDON_HOST": "pg.example",
            "POSTGRESQL_ADDON_PORT": "5432",
            "POSTGRESQL_ADDON_USER": "u",
            "POSTGRESQL_ADDON_PASSWORD": "p",
            "LAKE_BUCKET": "my-lake",
            "CELLAR_ADDON_HOST": "cellar.example",
            "CELLAR_ADDON_KEY_ID": "id",
            "CELLAR_ADDON_KEY_SECRET": "secret",
        }
    )
    assert lake["metadata_path"] == "postgres:dbname=db host=pg.example port=5432 user=u password=p"
    assert lake["data_path"] == "s3://my-lake/lake/"
    assert lake["s3"] == {"endpoint": "cellar.example", "key_id": "id", "key_secret": "secret"}


def test_quote_doubles_single_quotes():
    assert serve.quote("it's") == "'it''s'"


def test_passwords_are_hidden_in_error_messages():
    message = 'Unable to connect to Postgres at "dbname=d host=h port=1 user=u password=s3cr3t": refused'
    assert serve.redact(message) == 'Unable to connect to Postgres at "dbname=d host=h port=1 user=u password=<hidden>": refused'


def test_the_lake_opens_once_the_catalogue_answers(monkeypatch):
    calls = []

    def open_lake(directory):
        calls.append(directory)
        if len(calls) < 3:
            raise duckdb.IOException("Unable to connect: password=s3cr3t refused")
        return "connection"

    monkeypatch.setattr(serve, "open_lake", open_lake)
    assert serve.open_lake_when_ready("dir", attempts=5, delay=0) == "connection"
    assert len(calls) == 3


def test_a_catalogue_that_never_answers_fails_without_its_password(monkeypatch):
    def open_lake(directory):
        raise duckdb.IOException("Unable to connect: password=s3cr3t refused")

    monkeypatch.setattr(serve, "open_lake", open_lake)
    with pytest.raises(RuntimeError) as failure:
        serve.open_lake_when_ready("dir", attempts=2, delay=0)
    assert "s3cr3t" not in str(failure.value)


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM read_text('/proc/self/environ')",
        "COPY (SELECT 1) TO '/tmp/escape.csv'",
        "INSTALL spatial",
        "SET enable_external_access = true",
    ],
)
def test_a_client_cannot_leave_the_lake(server, statement):
    port, _ = server
    client = duckdb.connect()
    with pytest.raises(duckdb.Error):
        client.execute(f"FROM quack_query('quack:localhost:{port}', {serve.quote(statement)}, token = '{TOKEN}')").fetchall()
