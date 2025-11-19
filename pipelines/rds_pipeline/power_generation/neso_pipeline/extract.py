import pandas as pd
import requests

HISTORICAL_DEMAND_RESOURCE_ID = "b2bde559-3455-4021-b179-dfe60c0337b0"
DEMAND_UPDATE_RESOURCE_ID = "177f6fa4-ae49-4182-81ea-0c6b35f26ca6"

def fetch_neso_demand_data(resource_id: str) -> dict:
    """
    Fetch historical demand data from NESO API using SQL query
    Endpoint: https://api.neso.energy/api/3/action/datastore_search_sql
    """
    url = "https://api.neso.energy/api/3/action/datastore_search_sql"

    sql_query = f"""
    SELECT *
    FROM  "{resource_id}"
    ORDER BY "_id"
    """
    params = {"sql": sql_query}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data["result"]["records"])
        return df

    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None

def parse_neso_demand_data(data: pd.DataFrame) -> pd.DataFrame:
    '''Get columns ND, TSD, SETTLEMENT_DATE, SETTLEMENT_PERIOD from NESO demand data'''
    return data[["ND", "TSD", "SETTLEMENT_DATE", "SETTLEMENT_PERIOD"]] 


