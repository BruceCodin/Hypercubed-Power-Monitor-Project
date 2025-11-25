FROM amazon/aws-lambda-python:3.13

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY alerts_lambda.py .

CMD ["alerts_lambda.lambda_handler"]