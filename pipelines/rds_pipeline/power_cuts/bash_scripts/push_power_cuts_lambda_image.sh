POWER_CUT_ECR_URL="129033205317.dkr.ecr.eu-west-2.amazonaws.com/c20-power-monitor-powercuts-etl-repo"
POWER_CUT_ECR_NAME="c20-power-monitor-powercuts-etl-repo"
AWS_REGION="eu-west-2"

# 1. Authenticate Docker to ECR
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin $POWER_CUT_ECR_URL

# 2. Tag local image with the ECR repository URL
docker tag $POWER_CUT_ECR_NAME:latest $POWER_CUT_ECR_URL:latest

# 3. Push image to ECR
docker push $POWER_CUT_ECR_URL:latest