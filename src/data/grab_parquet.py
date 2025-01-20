from minio import Minio
import pandas as pd
import os
import requests

try:
    import pyarrow.parquet as pq
    engine = 'pyarrow'
except ImportError:
    try:
        import fastparquet
        engine = 'fastparquet'
    except ImportError:
        raise ImportError("Unable to find a usable engine; please install 'pyarrow' or 'fastparquet' to use parquet support.")

def upload_to_minio(file_path: str, client: Minio, bucket_name: str) -> None:
    """
    Upload a file to MinIO.

    Args:
        file_path (str): Path of the file to upload.
        client (Minio): MinIO client instance.
        bucket_name (str): Name of the MinIO bucket.
    """
    object_name = os.path.basename(file_path)  # Nom du fichier dans le bucket
    try:
        client.fput_object(bucket_name, object_name, file_path)
        print(f"Uploaded {file_path} to bucket {bucket_name} as {object_name}.")
    except Exception as e:
        print(f"Failed to upload {file_path} to MinIO: {e}")

def grab_data() -> None:
    """
    Download and upload data to MinIO.

    This method downloads parquet files of the New York Yellow Taxi from January 2023 to August 2023.
    Files are saved into "data/raw" folder and uploaded to MinIO.
    """
    # Base URL for the NYC Yellow Taxi Trip Records
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    data_dir = "data/raw"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # Configuration MinIO
    client = Minio(
        "127.0.0.1:9000",  # Adresse de MinIO
        access_key="minio",  # Remplace si tu utilises d'autres identifiants
        secret_key="minio123",
        secure=False  # Désactiver HTTPS pour le local
    )
    bucket_name = "parquet-bucket"

    # Crée le bucket s'il n'existe pas
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")

    # Télécharger les fichiers de janvier à août 2023
    parquet_files = []
    for year, months in [(2023, range(1, 9))]:  # Janvier à Août 2023
        for month in months:
            month_str = f"{month:02d}"
            file_url = f"{base_url}{year}-{month_str}.parquet"
            local_file_path = os.path.join(data_dir, f"yellow_tripdata_{year}-{month_str}.parquet")
            
            if not os.path.exists(local_file_path):
                try:
                    print(f"Downloading {file_url}...")
                    response = requests.get(file_url, stream=True)
                    response.raise_for_status()
                    with open(local_file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=1024):
                            f.write(chunk)
                    print(f"Saved to {local_file_path}")

                    # Upload vers MinIO
                    upload_to_minio(local_file_path, client, bucket_name)
                    parquet_files.append(local_file_path)
                except requests.exceptions.RequestException as e:
                    print(f"Failed to download {file_url}: {e}")
                    continue

    # Combiner les fichiers téléchargés en un seul fichier Parquet
    if parquet_files:
        combined_df = pd.concat([pd.read_parquet(file, engine=engine) for file in parquet_files], ignore_index=True)
        combined_file_path = os.path.join(data_dir, "yellow_tripdata_2023.parquet")
        combined_df.to_parquet(combined_file_path, index=False, engine=engine)
        print(f"Combined parquet file saved to {combined_file_path}")
        # Upload du fichier combiné vers MinIO
        upload_to_minio(combined_file_path, client, bucket_name)
    else:
        print("No valid parquet files were downloaded.")

if __name__ == '__main__':
    grab_data()
