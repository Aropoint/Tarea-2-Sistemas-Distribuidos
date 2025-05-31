import os
import csv
from pymongo import MongoClient
import json
from datetime import datetime

# Configuración MongoDB (igual)
mongo_host = os.getenv("MONGO_HOST", "mongo")
mongo_db = os.getenv("MONGO_DB", "waze_alertas")
mongo_collection = os.getenv("MONGO_COLLECTION", "alertas")
mongo_user = os.getenv("MONGO_USER", "admin")
mongo_pass = os.getenv("MONGO_PASS", "admin123")

mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/?authSource=admin"
client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]

# Obtener datos
data = list(collection.find({}, {"_id": 0}))

# Ruta CSV
csv_path = "/data/datos_clean.csv"  # Nombre específico para Pig

if data:
    # Normalización mejorada para Pig
    def normalize_field(value):
        if value is None:
            return ""
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, (dict, list)):
            # JSON compacto sin saltos de línea
            return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
        if isinstance(value, datetime):
            return value.isoformat()
        # Escape de comas y comillas
        return str(value).replace('"', '""').replace('\n', ' ').replace('\r', '')
    
    # Campos en orden específico (ajusta según tu esquema Pig)
    priority_fields = [
        'uuid', 'type', 'city', 'street', 'speed', 'reliability',
        'confidence', 'country', 'reportRating', 'pubMillis'
    ]
    
    # Todos los campos (prioritarios primero)
    all_keys = set()
    for doc in data:
        all_keys.update(doc.keys())
    
    # Ordenamos: campos prioritarios primero, luego el resto
    fieldnames = priority_fields + [k for k in sorted(all_keys) if k not in priority_fields]

    # Escribir CSV optimizado para Pig
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        # Header
        writer.writerow(fieldnames)
        
        for row in data:
            try:
                # Construir fila con valores normalizados
                csv_row = []
                for field in fieldnames:
                    value = row.get(field)
                    
                    # Manejo especial para campos JSON/embebidos
                    if field in ['location', 'wazeData'] and isinstance(value, (dict, list)):
                        value = json.dumps(value, separators=(',', ':'))
                    
                    csv_row.append(normalize_field(value))
                
                writer.writerow(csv_row)
            except Exception as e:
                print(f"Error procesando fila: {e}")
                continue
    
    print(f"✅ CSV compatible con Pig generado en {csv_path}")
    print(f"📊 Total registros: {len(data)}")
    print(f"📝 Campos: {len(fieldnames)}")
    print("⚙️ Configuración CSV:")
    print(f"   - Delimitador: ','")
    print(f"   - Quotechar: '\"'")
    print(f"   - Campos JSON convertidos a strings compactos")
else:
    print("⚠️ No se encontraron datos en MongoDB")