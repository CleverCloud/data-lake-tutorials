# Data lakes on Clever Cloud

You keep analytical data on Clever Cloud and query it. These tutorials build the lake for a
given need. Each one starts from the need, derives the architecture, deploys it, and runs a
minimal example to adapt.

Each tutorial lives in its own directory and stands alone. The example data is a few rows,
enough to show each mechanism.

## Four words first

- **Cellar** is Clever Cloud's object storage. It speaks the S3 API. The lake's files live there.
- **A table format** turns a set of Parquet files into a table, with transactions and a history.
  The tutorials use DuckLake, Iceberg and Delta Lake.
- **A catalogue** records which files make the current version of each table. A commit goes
  through it.
- **Karpenter** adds nodes to a Clever Kubernetes Engine cluster when pods wait for room, and
  removes the nodes they leave empty, within the limits of a NodePool.

## The tutorials

| Tutorial | The need | Architecture |
|---|---|---|
| [1. Analyse a history of a few gigabytes to a few terabytes](01-duckdb-ducklake/) | A few users<br>Occasional analytical queries or dashboards<br>No team to run a cluster | DuckLake tables: Parquet files on Cellar, catalogue in PostgreSQL<br>DuckDB, embedded or in server mode, in a Clever Cloud application |
| [2. Share a lake of several terabytes between many users](02-trino-iceberg/) | Interactive queries too large for one instance<br>Many users at the same time<br>Queries that join the lake with PostgreSQL | Iceberg tables: files on Cellar, Lakekeeper catalogue, its state in PostgreSQL<br>Trino on Clever Kubernetes Engine: a coordinator and workers<br>A dedicated load balancer in front of Trino |
| [3. Recalculate tens of terabytes in batches](03-spark-iceberg/) | Heavy, scheduled processing<br>Spread over several machines | Iceberg tables: files on Cellar, Lakekeeper catalogue, its state in PostgreSQL<br>Spark on Clever Kubernetes Engine: one Job per batch, a driver and its executors |
| [4. Take over or share Delta Lake tables](04-spark-delta/) | Tables already in Delta, from Databricks or Microsoft Fabric<br>Tools that read only Delta<br>Specifications that require Delta | Delta tables: Parquet files and the Delta log on Cellar<br>A Spark Connect server on Clever Kubernetes Engine: the one driver every writer goes through<br>DuckDB or Spark reads the tables |

## What every tutorial guarantees

These points do not depend on the volume. Every tutorial keeps them.

- **Concurrent writes are handled.** Two writes at the same time neither lose nor overwrite data.
  Each commit goes through one point that takes commits in turn: the catalogue, or for Delta
  Lake, the one Spark driver. Two queries that change the same rows at the same time, with
  `UPDATE`, `DELETE` or `MERGE`, end with one of them failing on a conflict: the application runs
  it again.
- **The tables' state is kept safe.** The catalogue lives in a managed PostgreSQL. A Delta table
  keeps its state in its own log, next to its files on Cellar.
- **Formats are open.** The files are Parquet, and their tables follow open formats. DuckDB reads
  the Iceberg tables of tutorial 2 and the Delta tables of tutorial 4.
- **Access is protected.** Every service is reached with its credentials: the Cellar add-on's
  keys, the PostgreSQL databases' passwords, a token or users with a password for the engines. No
  service is exposed without authentication.
- **Long queries are planned for.** The platform's shared load balancer cuts a request without an
  answer after 3 minutes. Each tutorial says how its long queries avoid it.
- **Growth has a ceiling.** On Kubernetes Engine, every layer that grows has a limit: the nodes
  Karpenter adds, the namespace's quota, the number of workers or executors. Whatever the load,
  a cluster does not grow past them.

## Backups and restores

| What | Where it lives | How it is backed up |
|---|---|---|
| A catalogue's state (tutorials 1, 2, 3) | A PostgreSQL add-on | The add-on's backups |
| Lakekeeper's encryption key (tutorials 2, 3) | The Terraform state | With the Terraform state.<br>The catalogue's backup is of no use without the key. |
| The tables' files | The Cellar bucket | A copy to a second bucket.<br>Cellar replicates the data, but it replicates a deletion too. |
| Trino's users (tutorial 2) | `password.db` | With your other secrets |

A catalogue and its files are restored together. A catalogue restored to an earlier day names
files that upkeep may have deleted since: keep the files at least as long as the catalogue's
backups.

[Object Lock](https://www.clever.cloud/developers/doc/deploy/storage/cellar/) prevents the
deletion of files for a chosen period. It is enabled when a bucket is created. Upkeep of the
tables cannot delete locked files.

## Precautions

- **No load test through a shared load balancer.** Use a dedicated load balancer, and tell the
  Clever Cloud support beforehand.
- **One Kubernetes Engine tutorial at a time.** Tutorials 2, 3 and 4 each create a cluster. Two at
  once pass the organisation's default quota of 40 vCPU and 40 GB, which is raised on request.

## Before you start

You need:

- A Clever Cloud account, and an organisation. Its identifier, `orga_…`, is in its Console
  address: `console.clever-cloud.com/organisations/orga_…`.
- Clever Tools, Clever Cloud's command line (https://www.clever.cloud/developers/doc/cli/), logged
  in once with `clever login`. The Terraform provider reads the credentials `clever login` stores.
- For tutorials 2, 3 and 4, Kubernetes Engine turned on for your account, once:
  `clever features enable k8s`.
- `terraform` 1.6 or later.
- `git`: clone this repository rather than downloading an archive. Tutorial 1 deploys an
  application from your clone.

Each tutorial lists the other tools it needs. Its commands run from its own directory, in one
terminal: they read values that earlier commands exported. In a new terminal, export the
`TF_VAR_*` variables, and `KUBECONFIG` for tutorials 2, 3 and 4, again.

Every tutorial ends with a step that removes everything it created. Until then, the services run.
