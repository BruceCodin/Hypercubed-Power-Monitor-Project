
# ECR Registry URL
ECR_REGISTRY="129033205317.dkr.ecr.eu-west-2.amazonaws.com"

# ECR Repository name
ECR_REPOSITORY="c20-power-monitor-dashboard-repo"

# Full ECR image URI (registry + repository)
ECR_IMAGE_URI="$ECR_REGISTRY/$ECR_REPOSITORY"

# Local image name
LOCAL_IMAGE_NAME="c20-power-monitor-alerts-dashboard"
AWS_REGION="eu-west-2"

# 1. Authenticate Docker to ECR
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY"

# 2. Tag local image with the ECR image URI
docker tag "$LOCAL_IMAGE_NAME:latest" "$ECR_IMAGE_URI:latest"

# 3. Push image to ECR
docker push "$ECR_IMAGE_URI:latest"