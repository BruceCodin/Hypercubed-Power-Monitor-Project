S3_ECR_REPO_NAME="c20-power-monitor-s3-etl-repo"

docker build --platform linux/amd64 --provenance=false --no-cache -t ${S3_ECR_REPO_NAME}:latest ../.
