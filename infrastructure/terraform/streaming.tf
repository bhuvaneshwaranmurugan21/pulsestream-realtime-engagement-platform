resource "aws_security_group" "msk" {
  name_prefix = "${local.name}-msk-"
  description = "IAM-authenticated MSK Serverless traffic"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Kafka IAM clients"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = var.stream_client_security_group_ids
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_serverless_cluster" "events" {
  cluster_name = "${local.name}-events"

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }
}
