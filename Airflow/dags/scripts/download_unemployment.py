import requests
import time
import os
import pandas as pd
from airflow.sdk import Variable

abbreviations = [
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA",
    "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
    "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI",
    "WV", "WY"]


def download_unemployment():
    main_df = pd.DataFrame()

    api_key = Variable.get("fred_key")
    
    for i in abbreviations:

        if i == 'AK':
            ak_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=AKUR&api_key={api_key}&file_type=json"
            response = requests.get(ak_url)
            time.sleep(1)
            response.raise_for_status()
            data = response.json()

            clean_data = []

            for obs in data['observations']:
                cols= {k:v for k, v in obs.items() if k not in ['realtime_start', 'realtime_end']}
                clean_data.append(cols)

                df = pd.DataFrame(clean_data)

                df = df.rename(columns={'value': 'AKUR'})

            main_df = df
            
            

        elif i == 'NJ':
            nj_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=NJURN&api_key={api_key}&file_type=json"
            response = requests.get(nj_url)
            time.sleep(1)
            response.raise_for_status()
            data = response.json()

            clean_data = []

            for obs in data['observations']:
                cols= {k:v for k, v in obs.items() if k not in ['realtime_start', 'realtime_end']}
                clean_data.append(cols)

                nj_df = pd.DataFrame(clean_data)

                nj_df = nj_df.rename(columns={'value': 'NJUR'})

            main_df = main_df.merge(nj_df, on="date", how='inner')

        else:
            base_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={i}UR&api_key={api_key}&file_type=json"
            response = requests.get(base_url)
            time.sleep(1)
            response.raise_for_status()
            data = response.json()

            clean_data = []

            for obs in data['observations']:
                cols= {k:v for k, v in obs.items() if k not in ['realtime_start', 'realtime_end']}
                clean_data.append(cols)

                df = pd.DataFrame(clean_data)

                df = df.rename(columns={'value': f'{i}UR'})

            main_df = main_df.merge(df, on="date", how='inner')


    script_dir = os.path.dirname(os.path.abspath(__file__))
    download_folder = os.path.join(script_dir, 'data', 'gov')
    output_path = os.path.join(download_folder, "unemployment.parquet")
    os.makedirs(download_folder, exist_ok=True)
    main_df.to_parquet(output_path, index=False)

