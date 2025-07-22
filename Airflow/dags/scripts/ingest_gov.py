from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from airflow.sdk import Variable

def upload_gov_files() -> None:
    bucket_name = Variable.get("bucket_name")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_folder = os.path.join(script_dir, 'data', 'gov')
    s3 = S3Hook(aws_conn_id='aws_default')

    for filename in os.listdir(local_folder):
        local_path = os.path.join(local_folder, filename)

        if filename.endswith('.parquet'):
            s3_key = f"data/parquet/{filename}"
        elif filename.endswith('.json'):
            s3_key = f"data/json/{filename}"
        else:
            print(f"Skipped (unsupported format): {filename}")
            continue

        try:
            s3.load_file(
                filename=local_path,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"{filename} uploaded to s3://{bucket_name}/{s3_key}")
            os.remove(local_path) 
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")

