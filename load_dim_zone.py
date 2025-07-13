import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

print("🚀 Lecture du fichier taxi_zone_lookup.csv...")
zone_df = pd.read_csv('/home/walid1234/data_engineering/datasets/taxi_zone_lookup.csv')

# Encodage du mot de passe
password = quote_plus("walid1234@")

# Renommer colonnes
zone_df.rename(columns={'LocationID': 'zone_id', 'Zone': 'zone_name'}, inplace=True)
zone_df = zone_df[['zone_id', 'zone_name']]

print("🛠 Connexion à PostgreSQL...")
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@127.0.0.1:5432/de_warehouse")

print("⬆ Chargement dans dim_zone...")
zone_df.to_sql('dim_zone', engine, if_exists='append', index=False)

print("✅ dim_zone chargée avec succès !")

