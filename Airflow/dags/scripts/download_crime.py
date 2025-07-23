import requests
import os
import pandas as pd

def download_crime_data() -> pd.DataFrame.to_parquet:

    ### I provided this GitHub URL to download the data because the Data.gov API has a rate limit of 1000 calls per hour and the amount of calls needed to get the data is around 3000.
    ### The data is from the FBI's Crime Data Explorer, which is a public dataset, there is not a direct link for it either
    url = "https://raw.githubusercontent.com/nbran3/TransactionPipeline/refs/heads/main/estimated_crimes_1979_2023.csv"
    filename = 'crimes_data.csv'

    response = requests.get(url)

    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {url} to {filename}")
    else:
        print(f"Failed to download. Status code: {response.status_code}")

    df = pd.read_csv(r'./crimes_data.csv')
    print("Transforming data...")

    df = df[['year','state_abbr', 'population','violent_crime','property_crime','larceny']]

    df = df[(df['year'] >= 2010) & (df['year'] <= 2019)]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    download_folder = os.path.join(script_dir, 'data', 'gov')
    output_path = os.path.join(download_folder, "crime.parquet")
    os.makedirs(download_folder, exist_ok=True)
    df.to_parquet(output_path, index=False)
    
    print(f"Transformed data saved to {output_path}")
    os.remove(filename)


