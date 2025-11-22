FROM amazon/aws-lambda-python:3.13

COPY requirements.txt .
RUN pip install -r requirements.txt

# National Grid Pipeline
COPY extract_national_grid.py ../national_grid_pipeline/
COPY transform_national_grid.py ../national_grid_pipeline/

# NIE Networks Pipeline
COPY extract_nie.py ../nie_networks_pipeline/
COPY transform_nie.py ../nie_networks_pipeline/

# Northern Powergrid Pipeline
COPY extract_northern_powergrid.py ../northern_powergrid_pipeline/
COPY transform_northern_powergrid.py ../northern_powergrid_pipeline/

# SP Energy Networks Pipeline
COPY extract_sp_en.py ../sp_energy_pipeline/
COPY transform_sp_en.py ../sp_energy_pipeline/

# SP Northwest Pipeline
COPY extract_sp_northwest.py ../sp_northwest_pipeline/
COPY transform_sp_northwest.py ../sp_northwest_pipeline/

# SSEN Pipeline
COPY extract_ssen.py ../ssen_pipeline/
COPY transform_ssen.py ../ssen_pipeline/

# UK Power Networks Pipeline
COPY extract_uk_pow.py ../uk_power_networks_pipeline/
COPY transform_uk_pow.py ../uk_power_networks_pipeline/

# Main Lambda Handler
COPY lambda_handler_power_cuts.py .

CMD ["lambda_handler_power_cuts.lambda_handler"]