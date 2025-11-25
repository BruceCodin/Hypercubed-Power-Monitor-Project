#!/bin/bash

# Configuration
ECR_REPO_URI="129033205317.dkr.ecr.eu-west-2.amazonaws.com/c20-power-monitor-ai-summary-repo"
AWS_REGION="eu-west-2"
IMAGE_TAG="latest"

echo "🔐 Logging into ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REPO_URI

if [ $? -ne 0 ]; then
    echo "❌ ECR login failed"
    exit 1
fi

echo "🏗️  Building Docker image..."
docker build -t ai-summary:$IMAGE_TAG .

if [ $? -ne 0 ]; then
    echo "❌ Docker build failed"
    exit 1
fi

echo "🏷️  Tagging image..."
docker tag ai-summary:$IMAGE_TAG $ECR_REPO_URI:$IMAGE_TAG

if [ $? -ne 0 ]; then
    echo "❌ Docker tag failed"
    exit 1
fi

echo "⬆️  Pushing to ECR..."
docker push $ECR_REPO_URI:$IMAGE_TAG

if [ $? -ne 0 ]; then
    echo "❌ Docker push failed"
    exit 1
fi

echo "✅ Deployment complete!"
echo "📦 Image pushed to: $ECR_REPO_URI:$IMAGE_TAG"
echo ""
echo "Next steps:"
echo "1. Update your Lambda function to use this image"
echo "2. Test the Lambda function manually"
echo "3. Check CloudWatch logs for any issues"