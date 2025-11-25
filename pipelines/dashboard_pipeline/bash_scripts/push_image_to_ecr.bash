
DASHBOARD_ECR_URL="129033205317.dkr.ecr.eu-west-2.amazonaws.com/c20-power-monitor-dashboard-repo"
DASHBOARD_ECR_NAME="c20-power-monitor-dashboard-repo"
AWS_REGION="eu-west-2"

# 1. Authenticate Docker to ECR
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin $DASHBOARD_ECR_URL

# 2. Tag local image with the ECR repository URL
docker tag $DASHBOARD_ECR_NAME:latest $DASHBOARD_ECR_URL:latest

# 3. Push image to ECR
docker push $DASHBOARD_ECR_URL:latest