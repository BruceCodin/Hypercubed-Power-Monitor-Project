FROM amazon/aws-lambda-python:3.13

COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all pipeline directories
# .dockerignore ensures unnecessary files are not copied

COPY national_grid_pipeline/ ./national_grid_pipeline/
COPY nie_networks_pipeline/ ./nie_networks_pipeline/
COPY northern_powergrid_pipeline/ ./northern_powergrid_pipeline/
COPY sp_energy_pipeline/ ./sp_energy_pipeline/
COPY sp_northwest_pipeline/ ./sp_northwest_pipeline/
COPY ssen_pipeline/ ./ssen_pipeline/
COPY uk_power_networks_pipeline/ ./uk_power_networks_pipeline/

# Main Lambda Handler
COPY lambda_handler_power_cuts.py .

CMD ["lambda_handler_power_cuts.lambda_handler"]