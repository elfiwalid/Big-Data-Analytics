import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from urllib.parse import quote_plus

# Encodage du mot de passe
password = quote_plus("walid1234@")

print("🛠 Connexion à PostgreSQL...")
engine = create_engine(f"postgresql+psycopg2://postgres:{password}@127.0.0.1:5432/de_warehouse")

# 📥 Charger les données
query = 'SELECT trip_distance, pickup_hour, fare_amount FROM fact_taxi'
try:
    df = pd.read_sql(query, engine)
except Exception as e:
    print(f"❌ Erreur de lecture SQL : {e}")
    exit()

print(f"✅ {len(df)} lignes chargées")

# ⚠️ Vérifier colonnes disponibles
if 'trip_duration_min' in df.columns:
    feature_cols = ['trip_distance', 'trip_duration_min', 'pickup_hour']
else:
    feature_cols = ['trip_distance', 'pickup_hour']
    print("⚠️ Colonne 'trip_duration_min' absente, entraînement sans cette feature.")

# 💧 Nettoyage des données
df.dropna(subset=feature_cols + ['fare_amount'], inplace=True)
if len(df) == 0:
    print("❌ Pas de données après nettoyage, abandon.")
    exit()

# 🏗 Préparer X et y
X = df[feature_cols]
y = df['fare_amount']

# ✂ Split train/test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 📈 Modèle
model = LinearRegression()
model.fit(X_train, y_train)

# 📊 Évaluation
y_pred = model.predict(X_test)
score = r2_score(y_test, y_pred)
print(f"✅ Modèle entraîné, R² = {score:.2f}")

# 🔮 Prédictions complètes
df['predicted_fare'] = model.predict(X)

# 💾 Sauvegarde des résultats
output_table = 'fact_taxi_predictions'
cols_to_save = [col for col in ['trip_distance', 'trip_duration_min', 'pickup_hour', 'fare_amount', 'predicted_fare']
                if col in df.columns]

try:
    df[cols_to_save].to_sql(output_table, engine, if_exists='replace', index=False)
    print(f"✅ Résultats enregistrés dans la table '{output_table}'")
except Exception as e:
    print(f"❌ Erreur lors de l'enregistrement : {e}")

