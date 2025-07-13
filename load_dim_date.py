import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from urllib.parse import quote_plus

print("🚀 Lecture du fichier parquet...")
# Lire le fichier parquet
parquet_path = '/home/walid1234/data_engineering/datasets/ytaxi_2024_01_clean.parquet'
table = pq.read_table(parquet_path)
df = table.to_pandas()

# Encodage du mot de passe
password = quote_plus("walid1234@")

print("📅 Extraction des dates distinctes...")
# Extraire date seule
df['date_only'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date
unique_dates = pd.Series(df['date_only'].unique())

# Créer le DataFrame dim_date
dim_date_df = pd.DataFrame({
    'date_id': pd.to_datetime(unique_dates),
    'year': pd.to_datetime(unique_dates).dt.year,
    'month': pd.to_datetime(unique_dates).dt.month,
    'day': pd.to_datetime(unique_dates).dt.day,
    'day_of_week': pd.to_datetime(unique_dates).dt.weekday + 1
})

print("🛠 Connexion à PostgreSQL...")
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@127.0.0.1:5432/de_warehouse")

print("⬆ Chargement dans dim_date...")
dim_date_df.to_sql('dim_date', engine, if_exists='append', index=False)

print("✅ dim_date chargée avec succès !")

