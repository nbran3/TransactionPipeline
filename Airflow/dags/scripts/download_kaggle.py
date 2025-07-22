import kaggle
import shutil
import pandas as pd
import os


def download_kaggle_files() -> None:

    path = './Airflow/data/kaggle'
    kaggle.api.authenticate()

    kaggle.api.dataset_download_files(r"computingvictor/transactions-fraud-datasets", path=path, unzip=True)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    download_folder = os.path.join(script_dir, 'data', 'kaggle')

    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    for item in os.listdir(path):
        s = os.path.join(path, item)
        d = os.path.join(download_folder, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            shutil.copy2(s, d)

    print(f"Files have been moved to {download_folder}")

    for file_name in os.listdir(download_folder):
        if file_name.endswith('.csv'):
            csv_file_path = os.path.join(download_folder, file_name)
            parquet_file_name = file_name.replace('.csv', '.parquet')
            parquet_file_path = os.path.join(download_folder, parquet_file_name)

            print(f"Transforming {file_name} to Parquet...")
            try:

                df = pd.read_csv(csv_file_path)

                df.to_parquet(parquet_file_path, engine='pyarrow', index=False) 

                print(f"Transformed {file_name} into {parquet_file_name} in {download_folder}")

                os.remove(csv_file_path)
                print(f"Removed original CSV: {csv_file_path}")

            except Exception as e:
                print(f"Error processing {file_name}: {e}")

    print("Parquet transformation process complete.")

