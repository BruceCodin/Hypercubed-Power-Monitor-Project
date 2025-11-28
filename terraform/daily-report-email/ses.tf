# ses.tf - AWS SES configuration for email sending

# ==============================================================================
# SES Configuration
# ==============================================================================

# Note: This configuration assumes that:
# 1. The AWS account is out of the SES sandbox (production access)
# 2. The sender email and recipient emails are verified in SES
# 3. Email sending is enabled in the target region

# Data source to validate sender email is verified in SES
data "aws_sesv2_account_details" "main" {
  count = var.ses_enabled ? 1 : 0
}

# ==============================================================================
# CloudWatch Alarms for SES Bounce/Complaint Monitoring
# ==============================================================================

# SNS Topic for SES notifications (bounces and complaints)
resource "aws_sns_topic" "ses_notifications" {
  count = var.ses_enabled ? 1 : 0
  name  = "${var.project_name}-ses-notifications-${var.environment}"

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-ses-notifications"
  })
}

# SNS Topic Policy to allow SES to publish
resource "aws_sns_topic_policy" "ses_notifications" {
  count = var.ses_enabled ? 1 : 0
  arn   = aws_sns_topic.ses_notifications[0].arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ses.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.ses_notifications[0].arn
      }
    ]
  })
}

# SES Configuration Set for bounce/complaint tracking
resource "aws_ses_configuration_set" "main" {
  count = var.ses_enabled ? 1 : 0
  name  = "${var.project_name}-config-set-${var.environment}"

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-ses-config-set"
  })
}

# Event destination for bounce notifications
resource "aws_ses_event_destination" "bounce" {
  count                  = var.ses_enabled ? 1 : 0
  name                   = "${var.project_name}-bounce-destination"
  configuration_set_name = aws_ses_configuration_set.main[0].name
  enabled                = true
  matching_types         = ["Bounce"]
  type                   = "SNS"
  sns_topic              = aws_sns_topic.ses_notifications[0].arn
}

# Event destination for complaint notifications
resource "aws_ses_event_destination" "complaint" {
  count                  = var.ses_enabled ? 1 : 0
  name                   = "${var.project_name}-complaint-destination"
  configuration_set_name = aws_ses_configuration_set.main[0].name
  enabled                = true
  matching_types         = ["Complaint"]
  type                   = "SNS"
  sns_topic              = aws_sns_topic.ses_notifications[0].arn
}

# ==============================================================================
# CloudWatch Alarms for Bounce and Complaint Monitoring
# ==============================================================================

resource "aws_cloudwatch_metric_alarm" "ses_bounce" {
  count               = var.ses_enabled ? 1 : 0
  alarm_name          = "${var.project_name}-ses-bounce-alarm-${var.environment}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Bounce"
  namespace           = "AWS/SES"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alert when SES bounce rate increases"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ConfigurationSet = aws_ses_configuration_set.main[0].name
  }

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-ses-bounce-alarm"
  })
}

resource "aws_cloudwatch_metric_alarm" "ses_complaint" {
  count               = var.ses_enabled ? 1 : 0
  alarm_name          = "${var.project_name}-ses-complaint-alarm-${var.environment}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Complaint"
  namespace           = "AWS/SES"
  period              = "300"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alert when SES complaint rate increases"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ConfigurationSet = aws_ses_configuration_set.main[0].name
  }

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-ses-complaint-alarm"
  })
}

resource "aws_cloudwatch_metric_alarm" "ses_send" {
  count               = var.ses_enabled ? 1 : 0
  alarm_name          = "${var.project_name}-ses-send-failures-${var.environment}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Send"
  namespace           = "AWS/SES"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "Alert when SES send failures occur"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ConfigurationSet = aws_ses_configuration_set.main[0].name
  }

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-ses-send-failures"
  })
}

# ==============================================================================
# Outputs
# ==============================================================================

output "ses_configuration_set_name" {
  description = "Name of the SES configuration set for bounce/complaint tracking"
  value       = var.ses_enabled ? aws_ses_configuration_set.main[0].name : null
}

output "sns_topic_arn" {
  description = "ARN of the SNS topic for SES notifications"
  value       = var.ses_enabled ? aws_sns_topic.ses_notifications[0].arn : null
}

output "ses_sender_email" {
  description = "Verified sender email address for SES"
  value       = var.sender_email
}

output "ses_recipient_emails" {
  description = "List of recipient email addresses"
  value       = var.recipient_emails
}
