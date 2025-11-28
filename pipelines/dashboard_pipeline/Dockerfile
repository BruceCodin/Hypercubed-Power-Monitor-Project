FROM python:3.13-slim

WORKDIR /app

# Install requirements
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files
COPY app/ .

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
