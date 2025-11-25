# ecs.tf - ECS Cluster, Task Definition, and Service (without ALB)

# CloudWatch Log Group for ECS tasks
resource "aws_cloudwatch_log_group" "dashboard" {
  name              = "/ecs/c20-dashboard"
  retention_in_days = 7

  tags = {
    Name = "Dashboard ECS Logs"
  }
}

# Security group for ECS tasks
resource "aws_security_group" "ecs_tasks" {
  name        = "c20-dashboard-ecs-tasks-sg"
  description = "Security group for dashboard ECS tasks"
  vpc_id      = var.vpc_id

  # Allow inbound traffic on Streamlit port from anywhere
  ingress {
    description = "Allow Streamlit access"
    from_port   = var.container_port
    to_port     = var.container_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow all outbound traffic (for RDS, S3, internet access)
  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "Dashboard ECS Tasks Security Group"
  }
}

# ECS Cluster
resource "aws_ecs_cluster" "dashboard" {
  name = var.ecs_cluster_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = "Dashboard ECS Cluster"
  }
}

# ECS Task Definition
resource "aws_ecs_task_definition" "dashboard" {
  family                   = var.ecs_task_family
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.ecs_task_cpu
  memory                   = var.ecs_task_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "dashboard"
      image     = "${aws_ecr_repository.dashboard.repository_url}:latest"
      essential = true

      portMappings = [
        {
          containerPort = var.container_port
          hostPort      = var.container_port
          protocol      = "tcp"
        }
      ]

      environment = [
        {
          name  = "S3_BUCKET_NAME"
          value = var.s3_bucket_name
        },
        {
          name  = "AWS_DEFAULT_REGION"
          value = var.aws_region
        }
      ]

      secrets = [
        {
          name      = "DB_HOST"
          valueFrom = "${var.db_secret_arn}:DB_HOST::"
        },
        {
          name      = "DB_NAME"
          valueFrom = "${var.db_secret_arn}:DB_NAME::"
        },
        {
          name      = "DB_USER"
          valueFrom = "${var.db_secret_arn}:DB_USER::"
        },
        {
          name      = "DB_PASSWORD"
          valueFrom = "${var.db_secret_arn}:DB_PASSWORD::"
        },
        {
          name      = "DB_PORT"
          valueFrom = "${var.db_secret_arn}:DB_PORT::"
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.dashboard.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:${var.container_port}/ || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    }
  ])

  tags = {
    Name = "Dashboard Task Definition"
  }
}

# ECS Service
resource "aws_ecs_service" "dashboard" {
  name            = var.ecs_service_name
  cluster         = aws_ecs_cluster.dashboard.id
  task_definition = aws_ecs_task_definition.dashboard.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.public_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  tags = {
    Name = "Dashboard ECS Service"
  }
}


# Outputs

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.dashboard.name
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.dashboard.arn
}

output "ecs_service_name" {
  description = "Name of the ECS service"
  value       = aws_ecs_service.dashboard.name
}

output "ecs_task_definition_arn" {
  description = "ARN of the ECS task definition"
  value       = aws_ecs_task_definition.dashboard.arn
}

output "dashboard_access_instructions" {
  description = "Instructions to access the dashboard"
  value       = "Dashboard is running! Find the task's public IP in the ECS console, then access at http://<public-ip>:8501"
}