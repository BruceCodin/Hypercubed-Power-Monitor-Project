# AWS Step Function for Email Notification Pipeline

# Step Function State Machine
resource "aws_sfn_state_machine" "email_pipeline" {
  name     = "${var.project_name}-email-pipeline-${var.environment}"
  role_arn = aws_iam_role.email_step_function_role.arn

  definition = jsonencode({
    Comment = "Email notification pipeline: Generate daily report and send via SES"
    StartAt = "RunReportLambda"
    States = {
      RunReportLambda = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.daily_report_email.arn
          Payload = {
            "executionContext.$" = "$$.Execution"
          }
        }
        ResultSelector = {
          "htmlEmail.$" = "$.Payload.body"
        }
        Next = "SendEmailViaSES"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "ReportGenerationFailed"
          }
        ]
      }

      SendEmailViaSES = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:ses:sendEmail"
        Parameters = {
          Source = var.sender_email
          Destination = {
            ToAddresses = var.recipient_emails
          }
          Message = {
            Subject = {
              Data = var.email_subject
            }
            Body = {
              Html = {
                "Data.$" = "$.htmlEmail"
                Charset  = "UTF-8"
              }
            }
          }
        }
        Next = "Success"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "EmailSendFailed"
          }
        ]
      }

      Success = {
        Type = "Succeed"
      }

      ReportGenerationFailed = {
        Type  = "Fail"
        Cause = "Lambda failed to generate daily report email"
        Error = "ReportGenerationError"
      }

      EmailSendFailed = {
        Type  = "Fail"
        Cause = "SES failed to send email"
        Error = "EmailSendError"
      }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.email_step_function_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tags = merge(var.common_tags, {
    Name      = "${var.project_name}-email-pipeline"
    Component = "EmailPipeline"
  })
}
