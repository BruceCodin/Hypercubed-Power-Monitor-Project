FROM public.ecr.aws/lambda/python:3.11

# Copy requirements file
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy function code
COPY generate_ai_summary.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["generate_ai_summary.lambda_handler"]