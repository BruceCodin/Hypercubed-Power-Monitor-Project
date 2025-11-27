FROM python:3.13-slim

WORKDIR /app

# Install requirements
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files
COPY app/app.py .
COPY app/heatmap.py .
COPY app/heatmap_helper.py .
COPY app/dashboard_power_generation.py .
COPY app/dashboard_power_generation_helpers.py .
COPY app/summaries_page.py .
COPY app/load_summaries.py .
COPY app/etl_customer.py .
COPY app/page_form.py .

# Copy config folder
COPY app/.streamlit/ .streamlit/

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
