import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect
from minio import Minio
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

# Configuration des journaux
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Configuration MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

# Configuration PostgreSQL
DB_CONFIG = {
    "dbms_engine": "postgresql",
    "dbms_username": "postgres",
    "dbms_password": "admin",
    "dbms_ip": "localhost",
    "dbms_port": "15432",
    "dbms_database": "nyc_warehouse",
    "dbms_table": "nyc_raw"
}
DB_CONFIG["database_url"] = (
    f"{DB_CONFIG['dbms_engine']}://{DB_CONFIG['dbms_username']}:{DB_CONFIG['dbms_password']}@"
    f"{DB_CONFIG['dbms_ip']}:{DB_CONFIG['dbms_port']}/{DB_CONFIG['dbms_database']}"
)

# Liste des colonnes attendues dans PostgreSQL
EXPECTED_COLUMNS = [
    "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "rate_code", "store_and_fwd_flag", "pickup_location_id",
    "dropoff_location_id", "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee"
]

def add_missing_columns(engine, table_name: str, expected_columns: list):
    """Ajoute les colonnes manquantes à la table PostgreSQL."""
    logging.info(f"Vérification des colonnes manquantes dans la table '{table_name}'...")
    inspector = inspect(engine)
    existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
    missing_columns = set(expected_columns) - set(existing_columns)

    if missing_columns:
        logging.warning(f"Colonnes manquantes détectées : {missing_columns}")
        with engine.connect() as conn:
            for col in missing_columns:
                col_type = "DOUBLE PRECISION" if col not in ["vendor_id", "pickup_location_id", "dropoff_location_id"] else "INTEGER"
                sql = f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type};"
                logging.info(f"Ajout de la colonne : {col} ({col_type})")
                conn.execute(sql)
    else:
        logging.info("Toutes les colonnes nécessaires sont déjà présentes.")

def fetch_parquet_files_from_minio(bucket_name: str) -> list:
    logging.info(f"Récupération de la liste des fichiers Parquet dans le bucket '{bucket_name}'...")
    objects = minio_client.list_objects(bucket_name)
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
    logging.info(f"Fichiers trouvés : {parquet_files}")
    return parquet_files

def download_parquet_from_minio(bucket_name: str, object_name: str, local_temp_dir: str) -> str:
    logging.info(f"Téléchargement du fichier '{object_name}' depuis le bucket '{bucket_name}'...")
    local_file_path = os.path.join(local_temp_dir, object_name)
    minio_client.fget_object(bucket_name, object_name, local_file_path)
    logging.info(f"Fichier téléchargé : {local_file_path}")
    return local_file_path

def write_data_postgres_fast(dataframe: pd.DataFrame, temp_csv: str) -> bool:
    logging.info("Chargement rapide des données dans PostgreSQL...")
    try:
        dataframe.to_csv(temp_csv, index=False, header=False)

        engine = create_engine(DB_CONFIG["database_url"])
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            with open(temp_csv, "r") as f:
                cursor.copy_expert(f"COPY {DB_CONFIG['dbms_table']} FROM STDIN WITH CSV", f)
            conn.commit()
            logging.info("Données insérées rapidement dans PostgreSQL.")
        finally:
            conn.close()
        return True
    except Exception as e:
        logging.error(f"Erreur lors de l'insertion rapide : {e}")
        return False
    finally:
        if os.path.exists(temp_csv):
            os.remove(temp_csv)

def align_columns(dataframe: pd.DataFrame, expected_columns: list) -> pd.DataFrame:
    missing_cols = set(expected_columns) - set(dataframe.columns)
    if missing_cols:
        logging.warning(f"Colonnes manquantes ajoutées au DataFrame : {missing_cols}")
        for col in missing_cols:
            dataframe[col] = None  # Colonnes manquantes remplies par des valeurs nulles
    return dataframe[expected_columns]

def convert_columns_to_integer(dataframe: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convertit les colonnes spécifiées en entier si possible."""
    for col in columns:
        if col in dataframe.columns:
            logging.info(f"Conversion de la colonne '{col}' en entier.")
            dataframe[col] = dataframe[col].fillna(0).astype(int)
    return dataframe

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    logging.info("Nettoyage des noms de colonnes...")
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def process_parquet_file(bucket_name: str, parquet_file: str, local_temp_dir: str) -> None:
    try:
        local_file_path = download_parquet_from_minio(bucket_name, parquet_file, local_temp_dir)
        parquet_df = pd.read_parquet(local_file_path, engine='pyarrow')
        logging.info(f"Fichier chargé dans un DataFrame : {parquet_file}")

        parquet_df = clean_column_name(parquet_df)

        parquet_df = convert_columns_to_integer(parquet_df, [
            "vendor_id", "passenger_count", "rate_code", "pickup_location_id", "dropoff_location_id", "payment_type"
        ])

        parquet_df = align_columns(parquet_df, EXPECTED_COLUMNS)

        temp_csv = os.path.join(local_temp_dir, f"temp_data_{uuid4().hex}.csv")

        if not write_data_postgres_fast(parquet_df, temp_csv):
            logging.error(f"Échec de l'insertion pour le fichier : {parquet_file}")

        os.remove(local_file_path)
        logging.info(f"Fichier temporaire supprimé : {local_file_path}")
    except Exception as e:
        logging.error(f"Erreur lors du traitement du fichier {parquet_file}: {e}")

def main() -> None:
    logging.info("Démarrage du script...")
    bucket_name = "parquet-bucket"
    local_temp_dir = "/tmp"

    logging.info(f"Vérification de l'existence du bucket '{bucket_name}'...")
    if not minio_client.bucket_exists(bucket_name):
        logging.error(f"Le bucket '{bucket_name}' n'existe pas.")
        return

    if not os.path.exists(local_temp_dir):
        os.makedirs(local_temp_dir)
        logging.info(f"Répertoire temporaire créé : {local_temp_dir}")

    engine = create_engine(DB_CONFIG["database_url"])
    add_missing_columns(engine, DB_CONFIG["dbms_table"], EXPECTED_COLUMNS)

    parquet_files = fetch_parquet_files_from_minio(bucket_name)
    if not parquet_files:
        logging.info("Aucun fichier Parquet trouvé dans le bucket.")
        return

    with ThreadPoolExecutor(max_workers=4) as executor:
        for parquet_file in parquet_files:
            executor.submit(process_parquet_file, bucket_name, parquet_file, local_temp_dir)

    logging.info("Tous les fichiers ont été traités avec succès !")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.error(f"Erreur fatale : {e}")
        sys.exit(1)