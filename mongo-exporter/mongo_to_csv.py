import os
import csv
from pymongo import MongoClient

mongo_host = os.getenv("MONGO_HOST", "localhost")
mongo_db = os.getenv("MONGO_DB", "mibase")
mongo_collection = os.getenv("MONGO_COLLECTION", "alertas")
mongo_user = os.getenv("MONGO_USER", "admin")
mongo_pass = os.getenv("MONGO_PASS", "admin123")

mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/"
client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]

data = list(collection.find({}, {"_id": 0}))

# Ruta CSV para luego consumirlo con el hadoop
csv_path = "/data/datos.csv"

if data:
    all_keys = set()
    for doc in data:
        all_keys.update(doc.keys())
    all_keys = list(all_keys)

    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_keys)
        writer.writeheader()
        for row in data:
            # acá se llenan las filas q no tienen todas las key -> no todas las alertas tienen los mismo attributos
            full_row = {key: row.get(key, "") for key in all_keys}
            writer.writerow(full_row)
    
    print(f"CSV generado exitosamente en {csv_path}")
else:
    print("No se encontraron datos.")
