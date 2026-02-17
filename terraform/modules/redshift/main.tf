# ============================================================
# Redshift Module — Data Warehouse
# ============================================================

variable "environment" { type = string }
variable "project" { type = string }
variable "node_type" { type = string }
variable "num_nodes" { type = number }
variable "master_username" { type = string }
variable "master_password" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "kms_key_arn" { type = string }
variable "curated_bucket_arn" { type = string }

# Subnet Group
resource "aws_redshift_subnet_group" "main" {
  name       = "${var.project}-redshift-subnet-${var.environment}"
  subnet_ids = var.subnet_ids
}

# Security Group
resource "aws_security_group" "redshift" {
  name_prefix = "${var.project}-redshift-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]  # Internal VPC only
    description = "Redshift port from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# IAM Role for Redshift COPY
resource "aws_iam_role" "redshift" {
  name = "${var.project}-redshift-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "redshift-s3-access"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [var.curated_bucket_arn, "${var.curated_bucket_arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = "kms:Decrypt"
        Resource = var.kms_key_arn
      }
    ]
  })
}

# Redshift Cluster
resource "aws_redshift_cluster" "main" {
  cluster_identifier  = "${var.project}-warehouse-${var.environment}"
  database_name       = "customer360"
  master_username     = var.master_username
  master_password     = var.master_password
  node_type           = var.node_type
  number_of_nodes     = var.num_nodes
  cluster_type        = var.num_nodes > 1 ? "multi-node" : "single-node"

  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  vpc_security_group_ids    = [aws_security_group.redshift.id]

  iam_roles = [aws_iam_role.redshift.arn]

  encrypted  = true
  kms_key_id = var.kms_key_arn

  publicly_accessible = false
  skip_final_snapshot = var.environment != "prod"

  automated_snapshot_retention_period = var.environment == "prod" ? 7 : 1

  tags = {
    Name = "${var.project}-warehouse-${var.environment}"
  }
}

output "cluster_identifier" { value = aws_redshift_cluster.main.cluster_identifier }
output "cluster_endpoint"   { value = aws_redshift_cluster.main.endpoint }
output "database_name"      { value = aws_redshift_cluster.main.database_name }
