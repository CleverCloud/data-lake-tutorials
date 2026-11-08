# Delta Lake tables on Cellar, written by Spark on a Clever Kubernetes Engine cluster.
#
# `terraform apply` creates the lake, a Cellar add-on and its bucket, and the cluster that writes
# to it. The lake does not depend on the cluster: the cluster can be resized, deleted or created
# again, and the tables stay. The Kubernetes manifests are then applied with kubectl (see README.md).

terraform {
  required_version = ">= 1.6"
  required_providers {
    clevercloud = {
      source  = "clevercloud/clevercloud"
      version = "~> 3.0"
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
  description = "Region of the lake"
  default     = "par"
}

# Bucket names are global to Cellar: choose one nobody else has. Supplied with:
#   export TF_VAR_bucket_name=<a-unique-bucket-name>
variable "bucket_name" {
  type        = string
  description = "Bucket holding the Delta tables"
}

variable "cluster_name" {
  type    = string
  default = "delta-spark"
}


# The lake: the Parquet files and the Delta log of every table.
resource "clevercloud_cellar" "lake" {
  name   = "delta-cellar"
  region = var.region
}

resource "clevercloud_cellar_bucket" "lake" {
  cellar_id = clevercloud_cellar.lake.id
  id        = var.bucket_name
}

# Karpenter creates nodes for the pods that wait for room, and removes the nodes they leave empty,
# within the limits of the NodePool in nodepool.yaml.
resource "clevercloud_kubernetes" "spark" {
  name                  = var.cluster_name
  node_autoprovisioning = true
}

# One fixed node, for what always runs: Karpenter itself, the cluster's services and the Spark Connect server.
# The executors run on the nodes Karpenter adds.
resource "clevercloud_kubernetes_nodegroup" "system" {
  kubernetes_id = clevercloud_kubernetes.spark.id
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

output "cluster_id" {
  value = clevercloud_kubernetes.spark.id
}

output "kubeconfig" {
  description = "Kubeconfig of the cluster, to write to a file and export as KUBECONFIG"
  value       = clevercloud_kubernetes.spark.kubeconfig
  sensitive   = true
}
