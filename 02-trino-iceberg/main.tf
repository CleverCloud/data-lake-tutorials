# An Iceberg lake on Clever Cloud, shared by many users through Trino.
#
# `terraform apply` creates the managed services and the cluster: the lake's bucket on Cellar, the
# catalogue's PostgreSQL, a business PostgreSQL to join with the lake, and the Kubernetes Engine
# cluster where Lakekeeper and Trino run. The Kubernetes manifests are then applied with kubectl
# (see README.md).

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
  description = "Region of the managed services"
  default     = "par"
}

# Bucket names are global to Cellar: choose one nobody else has. Supplied with:
#   export TF_VAR_bucket_name=<a-unique-bucket-name>
variable "bucket_name" {
  type        = string
  description = "Bucket holding the Iceberg tables"
}

# The catalogue's state is small. The plan is sized in connections.
variable "catalog_plan" {
  type        = string
  description = "PostgreSQL plan of the catalogue's state"
  default     = "xxs_sml"
}

# The business database stands for one of your own, to show a query that joins it with the lake.
# Set it to false to join your own database instead (see README.md).
variable "enable_business_database" {
  type        = bool
  description = "Create a PostgreSQL that illustrates a business database"
  default     = true
}

variable "business_plan" {
  type        = string
  description = "PostgreSQL plan of the business database"
  default     = "xxs_sml"
}

# The lake. Lakekeeper, Trino and DuckDB reach it with this add-on's keys.
resource "clevercloud_cellar" "lake" {
  name   = "iceberg-lake"
  region = var.region
}

resource "clevercloud_cellar_bucket" "lake" {
  cellar_id = clevercloud_cellar.lake.id
  id        = var.bucket_name
}

# The catalogue's state: which tables exist, and where their current metadata is.
# The key Lakekeeper encrypts the warehouse's credentials with, in its PostgreSQL. It lives in the
# Terraform state: a cluster created again reads the same catalogue with the same key. Back it up
# with the catalogue's PostgreSQL: one is of no use without the other.
resource "random_password" "lakekeeper_key" {
  length  = 32
  special = false
}

resource "clevercloud_postgresql" "catalog" {
  name   = "iceberg-catalog"
  plan   = var.catalog_plan
  region = var.region
}

# An illustration of a business database, joined with the lake in Trino queries.
resource "clevercloud_postgresql" "business" {
  count  = var.enable_business_database ? 1 : 0
  name   = "iceberg-business"
  plan   = var.business_plan
  region = var.region
}

# Karpenter creates nodes for the pods that wait for room, and removes the nodes they leave empty,
# within the limits of the NodePool in nodepool.yaml.
resource "clevercloud_kubernetes" "trino" {
  name                  = "iceberg-trino"
  node_autoprovisioning = true
}

# One fixed node, for what always runs: Karpenter itself, the cluster's services, Lakekeeper and
# the Trino coordinator. The Trino workers run on the nodes Karpenter adds.
resource "clevercloud_kubernetes_nodegroup" "system" {
  kubernetes_id = clevercloud_kubernetes.trino.id
  name          = "system"
  flavor        = "S"
  size          = 1
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

output "catalog_uri" {
  value     = clevercloud_postgresql.catalog.uri
  sensitive = true
}

# The business database's values, empty when it is not created.
output "business_uri" {
  value     = var.enable_business_database ? clevercloud_postgresql.business[0].uri : ""
  sensitive = true
}

output "business_jdbc_url" {
  description = "The business database, in the form Trino's PostgreSQL connector expects"
  value       = var.enable_business_database ? "jdbc:postgresql://${clevercloud_postgresql.business[0].host}:${clevercloud_postgresql.business[0].port}/${clevercloud_postgresql.business[0].database}" : ""
}

output "business_user" {
  value = var.enable_business_database ? clevercloud_postgresql.business[0].user : ""
}

output "business_password" {
  value     = var.enable_business_database ? clevercloud_postgresql.business[0].password : ""
  sensitive = true
}

output "lakekeeper_encryption_key" {
  value     = random_password.lakekeeper_key.result
  sensitive = true
}

output "kubeconfig" {
  description = "Kubeconfig of the cluster, to write to a file and export as KUBECONFIG"
  value       = clevercloud_kubernetes.trino.kubeconfig
  sensitive   = true
}
