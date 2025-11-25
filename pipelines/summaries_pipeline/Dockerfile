FROM amazon/aws-lambda-python:3.13

COPY requirements.txt .
RUN pip install -r requirements.txt

# Main Lambda Handler
COPY summaries_lambda_handler.py .

CMD ["summaries_lambda_handler.lambda_handler"]