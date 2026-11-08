# A DuckLake lake on Clever Cloud: Parquet files on Cellar, the catalogue in a managed PostgreSQL,
# and DuckDB in server mode in a Python application.
#
# `terraform apply` creates the add-ons and deploys the server from this repository. DuckDB,
# wherever it runs, also attaches the lake directly with the outputs below (see README.md).

terraform {
  required_version = ">= 1.6"
  required_providers {
    clevercloud = {
      source  = "clevercloud/clevercloud"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "clevercloud" {
  organisation = var.organisation
}

# Clever Cloud organisation identifier (orga_xxx), supplied with:
#   export TF_VAR_organisation=orga_xxxxxxxx
variable "organisation" {
  type        = string
  description = "Clever Cloud organisation (orga_xxx)"
}

variable "region" {
  type        = string
  description = "Region of every service"
  default     = "par"
}

# Bucket names are global to Cellar: choose one nobody else has. Supplied with:
#   export TF_VAR_bucket_name=<a-unique-bucket-name>
variable "bucket_name" {
  type        = string
  description = "Bucket holding the lake's Parquet files"
  # The name also forms the server's address, <bucket>.cleverapps.io: one label of a domain name.
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "Use 3 to 63 lower-case letters, digits and hyphens, starting and ending with a letter or a digit."
  }
}

# The catalogue holds metadata only: which files make which table. It is sized in connections:
# every DuckDB process attached to the lake holds at least one.
variable "postgresql_plan" {
  type        = string
  description = "PostgreSQL plan of the catalogue"
  default     = "xxs_sml"
}

variable "enable_server" {
  type        = bool
  description = "Deploy DuckDB in server mode, for clients that connect remotely"
  default     = true
}

# The server's memory is the memory of its queries. Clients share it.
variable "server_flavor" {
  type        = string
  description = "Instance size of the DuckDB server"
  default     = "S"
}

locals {
  # The server's address. Bucket names are unique across Cellar, so this domain is unique too.
  server_host = "${var.bucket_name}.cleverapps.io"
}

resource "clevercloud_cellar" "lake" {
  name   = "ducklake-cellar"
  region = var.region
}

resource "clevercloud_cellar_bucket" "lake" {
  cellar_id = clevercloud_cellar.lake.id
  id        = var.bucket_name
}

# The DuckLake catalogue, in its `ducklake` schema, and the `reporting` schema for dashboards.
resource "clevercloud_postgresql" "catalog" {
  name   = "ducklake-postgresql"
  plan   = var.postgresql_plan
  region = var.region
}

# The token every client of the server presents.
resource "random_password" "quack_token" {
  length  = 40
  special = false
}

# DuckDB in server mode. One instance: the clients share its memory and its sessions.
resource "clevercloud_python" "server" {
  count              = var.enable_server ? 1 : 0
  name               = "ducklake-duckdb-server"
  region             = var.region
  min_instance_count = 1
  max_instance_count = 1
  smallest_flavor    = var.server_flavor
  biggest_flavor     = var.server_flavor
  python_version     = "3.13"
  app_folder         = "01-duckdb-ducklake"
  redirect_https     = true
  vhosts             = [{ fqdn = local.server_host }]

  # Linking the add-ons gives the application their CELLAR_ADDON_* and POSTGRESQL_ADDON_* variables.
  dependencies = [clevercloud_cellar.lake.id, clevercloud_postgresql.catalog.id]
  environment = {
    LAKE_BUCKET = clevercloud_cellar_bucket.lake.id
    QUACK_TOKEN = random_password.quack_token.result
    # With a uv.lock in the folder, Clever Cloud installs the project with uv and runs this.
    CC_PYTHON_UV_RUN_COMMAND = "uv run --no-sync python serve.py"
  }

  # Deploys the commit checked out in this clone of the repository.
  deployment {
    repository = "file://${abspath("${path.module}/..")}"
  }
}

output "bucket" {
  value = clevercloud_cellar_bucket.lake.id
}

output "cellar_host" {
  value = clevercloud_cellar.lake.host
}

output "cellar_key_id" {
  value     = clevercloud_cellar.lake.key_id
  sensitive = true
}

output "cellar_key_secret" {
  value     = clevercloud_cellar.lake.key_secret
  sensitive = true
}

output "postgresql_uri" {
  description = "Connection string for psql and any PostgreSQL client"
  value       = clevercloud_postgresql.catalog.uri
  sensitive   = true
}

output "postgresql_conninfo" {
  description = "Connection string in the form DuckDB's postgres extension expects"
  value       = "dbname=${clevercloud_postgresql.catalog.database} host=${clevercloud_postgresql.catalog.host} port=${clevercloud_postgresql.catalog.port} user=${clevercloud_postgresql.catalog.user} password=${clevercloud_postgresql.catalog.password}"
  sensitive   = true
}

output "server_host" {
  description = "Address of the DuckDB server, empty when it is not deployed"
  value       = var.enable_server ? local.server_host : ""
}

output "server_app_id" {
  description = "Identifier of the server application, for `clever logs --app`; empty without the server"
  value       = var.enable_server ? clevercloud_python.server[0].id : ""
}

output "quack_token" {
  value     = random_password.quack_token.result
  sensitive = true
}
