"""DuckDB in server mode: opens the lake and serves it to remote DuckDB clients.

On Clever Cloud, this script is the application. It reads the add-ons' credentials from the
environment, opens the DuckLake lake, and serves it with Quack on the port Clever Cloud gives.
The platform's load balancer terminates TLS in front of it, so the server itself speaks plain
HTTP. Every client authenticates with the token in QUACK_TOKEN.

The lake is the main database of the server, not an attached one: each remote client runs on its
own server-side connection, and the main database is the one every connection sees by default.
"""

import os
import re
import signal
import tempfile
import time

import duckdb


def quote(value: str) -> str:
    """Quotes a value as an SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def lake_from_env(env: dict) -> dict:
    """The lake's location and credentials, from the variables of the linked add-ons."""
    conninfo = " ".join(
        f"{key}={env[var]}"
        for key, var in (
            ("dbname", "POSTGRESQL_ADDON_DB"),
            ("host", "POSTGRESQL_ADDON_HOST"),
            ("port", "POSTGRESQL_ADDON_PORT"),
            ("user", "POSTGRESQL_ADDON_USER"),
            ("password", "POSTGRESQL_ADDON_PASSWORD"),
        )
    )
    return {
        "metadata_path": f"postgres:{conninfo}",
        "data_path": f"s3://{env['LAKE_BUCKET']}/lake/",
        "s3": {
            "endpoint": env["CELLAR_ADDON_HOST"],
            "key_id": env["CELLAR_ADDON_KEY_ID"],
            "key_secret": env["CELLAR_ADDON_KEY_SECRET"],
        },
    }


def write_secrets(secret_directory: str, metadata_path: str, data_path: str, s3: dict | None = None) -> None:
    """Stores the lake's location and the Cellar keys as DuckDB secrets in `secret_directory`."""
    con = duckdb.connect(config={"secret_directory": secret_directory})
    for extension in ("ducklake", "postgres", "httpfs"):
        con.execute(f"INSTALL {extension}")
    if s3:
        # Cellar speaks the S3 API, with its own endpoint and path-style addresses.
        con.execute(
            f"CREATE OR REPLACE PERSISTENT SECRET cellar (TYPE s3, KEY_ID {quote(s3['key_id'])}, "
            f"SECRET {quote(s3['key_secret'])}, ENDPOINT {quote(s3['endpoint'])}, URL_STYLE 'path', "
            "REGION 'us-east-1')"
        )
    con.execute(
        f"CREATE OR REPLACE PERSISTENT SECRET lake (TYPE ducklake, METADATA_PATH {quote(metadata_path)}, "
        f"DATA_PATH {quote(data_path)}, METADATA_SCHEMA 'ducklake')"
    )
    con.close()


def open_lake(secret_directory: str) -> duckdb.DuckDBPyConnection:
    """Opens the lake as the main database, from the secrets `write_secrets` stored."""
    return duckdb.connect("ducklake:lake", config={"secret_directory": secret_directory})


def redact(message: str) -> str:
    """Hides the passwords a connection error repeats from the catalogue's connection string."""
    return re.sub(r"password=[^\s\"']+", "password=<hidden>", message)


def open_lake_when_ready(secret_directory: str, attempts: int = 30, delay: float = 10.0) -> duckdb.DuckDBPyConnection:
    """Opens the lake, waiting for the catalogue: a new PostgreSQL accepts connections a while after
    its creation, and the application may start before it."""
    for attempt in range(1, attempts + 1):
        try:
            return open_lake(secret_directory)
        except duckdb.Error as error:
            if attempt == attempts:
                raise RuntimeError(redact(str(error))) from None
            print(f"the lake is not reachable yet ({attempt}/{attempts}): {redact(str(error))[:200]}", flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def serve(con: duckdb.DuckDBPyConnection, host: str, port: int, token: str, public: bool) -> str:
    """Starts the Quack server on the session. It runs in the background until the process ends."""
    if len(token) < 16:
        raise ValueError("QUACK_TOKEN must be at least 16 characters long")
    options = f"token = {quote(token)}"
    if public:
        # A public address must be allowed explicitly. TLS is the load balancer's job.
        options += ", allow_other_hostname = true, disable_ssl = true"
    return con.execute(f"CALL quack_serve({quote(f'quack:{host}:{port}')}, {options})").fetchone()[1]


def lock_down(con: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """Limits what a client of the server can do to reading and writing the lake.

    A client runs SQL inside the server's process. Without these settings, it could read the
    process's environment (the add-ons' keys and passwords), write files, or load extensions.
    The settings apply to every connection, and nobody can change them afterwards. They come after
    `quack_serve`, which loads its own extension.
    """
    for statement in (
        f"SET GLOBAL allowed_directories = [{quote(data_path)}]",
        "SET GLOBAL enable_external_access = false",
        "SET GLOBAL allow_community_extensions = false",
        "SET GLOBAL autoinstall_known_extensions = false",
        "SET GLOBAL autoload_known_extensions = false",
        "SET GLOBAL lock_configuration = true",
    ):
        con.execute(statement)


def main() -> None:
    secrets = tempfile.mkdtemp(prefix="duckdb-secrets-")
    lake = lake_from_env(os.environ)
    write_secrets(secrets, lake["metadata_path"], lake["data_path"], lake["s3"])
    con = open_lake_when_ready(secrets)
    url = serve(con, "0.0.0.0", int(os.environ.get("PORT", "8080")), os.environ["QUACK_TOKEN"], public=True)
    lock_down(con, lake["data_path"])
    print(f"serving the lake on {url}", flush=True)
    signal.sigwait({signal.SIGINT, signal.SIGTERM})


if __name__ == "__main__":
    main()
