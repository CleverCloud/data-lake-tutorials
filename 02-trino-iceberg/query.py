"""Runs SQL on Trino through its dedicated load balancer, over HTTPS with a password.

    uv run query.py "SELECT 1"            runs one statement
    uv run query.py < statements.sql      runs each statement of the input, in order

The connection comes from the environment: TRINO_HOST, TRINO_USER, TRINO_PASSWORD, and TRINO_CA,
the certificate Trino serves. Each statement's rows are printed tab-separated.
"""

import os
import sys

import trino


def statements(text: str) -> list[str]:
    """Splits SQL on semicolons that end a line, and drops empty statements."""
    parts, current = [], []
    for line in text.splitlines():
        current.append(line)
        if line.rstrip().endswith(";"):
            parts.append("\n".join(current).rstrip().rstrip(";").strip())
            current = []
    parts.append("\n".join(current).strip())
    return [part for part in parts if part]


def connect() -> trino.dbapi.Connection:
    user = os.environ["TRINO_USER"]
    return trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=443,
        http_scheme="https",
        user=user,
        auth=trino.auth.BasicAuthentication(user, os.environ["TRINO_PASSWORD"]),
        verify=os.environ.get("TRINO_CA", "trino.crt"),
        catalog="iceberg",
    )


def run(cursor, statement: str) -> list[str]:
    cursor.execute(statement)
    return ["\t".join("" if value is None else str(value) for value in row) for row in cursor.fetchall()]


def main() -> int:
    text = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else sys.stdin.read()
    cursor = connect().cursor()
    for statement in statements(text):
        for line in run(cursor, statement):
            print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
