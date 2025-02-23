import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time
import gzip
import shutil

#Change this to your bucket name
BUCKET_NAME = "terraform_bucket_450522"  

#If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "./my_key/bucket.json"  
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)


BASE_URL = {
    'yellow': "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_",
    'green': "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_",
    'fhv': "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_"
}

DOWNLOAD_DIR = "../data"

CHUNK_SIZE = 8 * 1024 * 1024  
# mkdir "data" and save the data in data fold
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(params):
    taxi_type, year, month = params
    url = f"{BASE_URL[taxi_type]}{year}-{month}.csv.gz"
    gz_file_path = os.path.join(DOWNLOAD_DIR, f"{taxi_type}_tripdata_{year}-{month}.csv.gz")
    file_path = os.path.join(DOWNLOAD_DIR, f"{taxi_type}_tripdata_{year}-{month}.csv")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, gz_file_path)
        print(f"Downloaded: {gz_file_path}")
        
        # Unzip the file
        print(f"Unzipping file...")
        with gzip.open(gz_file_path, 'rb') as gz_file:
            with open(file_path, 'wb') as csv_file:
                shutil.copyfileobj(gz_file, csv_file)
        
        print(f"Unzipped file saved to: {file_path}")
        
        # Optionally, remove the gz file to save space
        os.remove(gz_file_path)
        print(f"Removed compressed file: {gz_file_path}")
        return file_path
    
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    if not file_path:
        return
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Generate all year-month combinations from 2019-01 to 2020-12
    download_params = []
    for taxi_type in ['yellow', 'green']:
        for year in range(2019, 2021):
            for month in range(1, 13):
                download_params.append((taxi_type, f"{year}", f"{month:02d}"))
    
    for month in range(1, 13):
        download_params.append(('fhv', "2019", f"{month:02d}"))
        
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_path = list(executor.map(download_file, download_params))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_path))  # Remove None values

    print("All files processed and verified.")