#!/bin/bash

# Configuration
ECR_REPO_URI="129033205317.dkr.ecr.eu-west-2.amazonaws.com/c20-power-monitor-ai-summary-repo"
AWS_REGION="eu-west-2"
IMAGE_TAG="latest"
LAMBDA_FUNCTION_NAME="c20-power-monitor-ai-summary-lambda"

echo "Updating Lambda function..."
aws lambda update-function-code \
    --region $AWS_REGION \
    --function-name $LAMBDA_FUNCTION_NAME \
    --image-uri $ECR_REPO_URI:$IMAGE_TAG


echo "Lambda function updated successfully!"
echo ""
echo "Next steps:"
echo "1. Test Lambda: aws lambda invoke --function-name $LAMBDA_FUNCTION_NAME --region $AWS_REGION response.json"
echo "2. Check logs: aws logs tail /aws/lambda/$LAMBDA_FUNCTION_NAME --region $AWS_REGION --follow"