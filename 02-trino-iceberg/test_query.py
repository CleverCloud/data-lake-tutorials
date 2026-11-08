"""Tests the SQL client's parsing and output, without a Trino server."""

import query


def test_statements_split_on_semicolons_at_the_end_of_a_line():
    text = """
    CREATE SCHEMA IF NOT EXISTS lake;
    CREATE TABLE lake.events (writer INTEGER, n INTEGER);

    INSERT INTO lake.events VALUES (0, 1), (0, 2)
    """
    assert query.statements(text) == [
        "CREATE SCHEMA IF NOT EXISTS lake",
        "CREATE TABLE lake.events (writer INTEGER, n INTEGER)",
        "INSERT INTO lake.events VALUES (0, 1), (0, 2)",
    ]


def test_a_semicolon_inside_a_line_does_not_split():
    assert query.statements("SELECT 'a;b' AS x;") == ["SELECT 'a;b' AS x"]


def test_rows_print_tab_separated_with_empty_nulls():
    class Cursor:
        def execute(self, statement):
            self.statement = statement

        def fetchall(self):
            return [(1, "a", None), (2, "b", 3.5)]

    cursor = Cursor()
    assert query.run(cursor, "SELECT 1") == ["1\ta\t", "2\tb\t3.5"]
    assert cursor.statement == "SELECT 1"


def test_the_connection_uses_https_and_a_password(monkeypatch):
    monkeypatch.setenv("TRINO_HOST", "203.0.113.7")
    monkeypatch.setenv("TRINO_USER", "analyst")
    monkeypatch.setenv("TRINO_PASSWORD", "secret")
    monkeypatch.setenv("TRINO_CA", "/tmp/trino.crt")
    connection = query.connect()
    assert (connection.host, connection.port, connection.http_scheme) == ("203.0.113.7", 443, "https")
    assert connection.auth._username == "analyst"
