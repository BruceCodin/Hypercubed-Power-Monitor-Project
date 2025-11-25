FROM amazon/aws-lambda-python:3.13

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY extract_from_rds.py .
COPY load_to_s3.py .

# Main Lambda Handler
COPY rds_to_s3_lambda.py .

CMD ["rds_to_s3_lambda.lambda_handler"]