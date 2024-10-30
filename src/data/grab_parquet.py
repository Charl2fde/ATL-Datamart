from minio import Minio
import urllib.request
import pandas as pd
import sys
import os
import requests

def main():
    grab_data()

try:
    import pyarrow.parquet as pq
    engine = 'pyarrow'
except ImportError:
    try:
        import fastparquet
        engine = 'fastparquet'
    except ImportError:
        raise ImportError("Unable to find a usable engine; please install 'pyarrow' or 'fastparquet' to use parquet support.")    

def grab_data() -> None:
    """
    Grab the data from New York Yellow Taxi

    This method downloads parquet files of the New York Yellow Taxi from January 2024 to December 2024.
    Files are saved into "../../data/raw" folder.
    This method then combines all parquet files into a single parquet file named "yellow_tripdata_2024.parquet".
    """
    # Base URL for the NYC Yellow Taxi Trip Records on Google Cloud Storage
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    
    # Create the target directory if it does not exist
    data_dir = "data/raw"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # Download files from January 2024 to December 2024
    parquet_files = []
    for month in range(1, 13):
        # Format the month to be two digits (e.g., 01, 02, ..., 12)
        month_str = f"{month:02d}"
        
        # Construct the URL for the current year and month
        file_url = f"{base_url}2024-{month_str}.parquet"
        
        # Define the local file path where the file will be saved
        local_file_path = os.path.join(data_dir, f"yellow_tripdata_2024-{month_str}.parquet")
        
        # Download the file if it does not already exist
        if not os.path.exists(local_file_path):
            print(f"Downloading {file_url}...")
            response = requests.get(file_url, stream=True)
            if response.status_code == 200:
                with open(local_file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
                print(f"Saved to {local_file_path}")
                parquet_files.append(local_file_path)  # Only add if download was successful
            else:
                print(f"Failed to download {file_url}: Status code {response.status_code}")
        else:
            print(f"File {local_file_path} already exists, skipping download.")
            parquet_files.append(local_file_path)  # Add existing file to the list

    # Combine all the downloaded parquet files into a single parquet file
    valid_parquet_files = [file for file in parquet_files if os.path.exists(file)]
    if valid_parquet_files:
        combined_df = pd.concat([pd.read_parquet(file, engine=engine) for file in valid_parquet_files], ignore_index=True)
        combined_file_path = os.path.join(data_dir, "yellow_tripdata_2024.parquet")
        combined_df.to_parquet(combined_file_path, index=False, engine=engine)
        print(f"Combined parquet file saved to {combined_file_path}")
    else:
        print("No valid parquet files were downloaded.")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
