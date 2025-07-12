import psycopg2

try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="postgres",
        password="walid1234@",
        dbname="de_warehouse"
    )
    print("✅ Connexion réussie à PostgreSQL !")
    conn.close()
except Exception as e:
    print(f"❌ Erreur de connexion : {e}")

