FUNCTION_NAME="c20-power-monitor-s3-etl-lambda"
IMAGE_URI="129033205317.dkr.ecr.eu-west-2.amazonaws.com/c20-power-monitor-s3-etl-repo"

aws lambda update-function-code --function-name $FUNCTION_NAME --image-uri $IMAGE_URI:latest