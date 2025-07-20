import kagglehub
import shutil
import os

def download_kaggle_files() -> None:
    path = kagglehub.dataset_download("computingvictor/transactions-fraud-datasets")

    download_folder= './data/kaggle/'

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