import boto3
import os

bucket_name = 'transactionsnbran'
local_folder = './data/kaggle/'
s3 = boto3.client('s3')

def upload_kaggle_files() -> None:
    for filename in os.listdir(local_folder):
        local_path = os.path.join(local_folder, filename)

        if filename.endswith('.csv'):
            s3_key = f"data/csv/{filename}"
        elif filename.endswith('.json'):
            s3_key = f"data/json/{filename}"
        else:
            print(f"Skipped (unsupported format): {filename}")
            continue

        try:
            s3.upload_file(local_path, bucket_name, s3_key)
            print(f"{filename} uploaded to s3://{bucket_name}/{s3_key}")
            os.remove(filename)
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")