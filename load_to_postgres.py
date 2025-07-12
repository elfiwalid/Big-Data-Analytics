import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Lire le fichier nettoyé Parquet
df = pd.read_parquet("datasets/ytaxi_2024_01_clean.parquet")

# Encodage du mot de passe
password = quote_plus("walid1234@")

# Connexion à PostgreSQL
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@127.0.0.1:5432/de_warehouse")

# Créer table et insérer données
df.to_sql("fact_taxi", engine, if_exists="replace", index=False)

print("✅ Données chargées dans PostgreSQL avec succès !")

