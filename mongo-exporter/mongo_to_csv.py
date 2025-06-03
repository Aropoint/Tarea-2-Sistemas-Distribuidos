import os
import csv
from pymongo import MongoClient
import json
from datetime import datetime
import time
import logging
import sys

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Configuración MongoDB
mongo_host = os.getenv("MONGO_HOST", "mongo")
mongo_db = os.getenv("MONGO_DB", "waze_db")
mongo_collection = os.getenv("MONGO_COLLECTION", "alertas")
mongo_user = os.getenv("MONGO_USER", "admin")
mongo_pass = os.getenv("MONGO_PASS", "admin123")

# Parámetros de reintento
MAX_RETRIES = 10
RETRY_DELAY = 30

def connect_to_mongo():
    """Establece conexión con MongoDB con manejo de errores"""
    try:
        mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/?authSource=admin&serverSelectionTimeoutMS=5000"
        client = MongoClient(mongo_uri, connectTimeoutMS=20000, socketTimeoutMS=None)
        client.admin.command('ping')
        logger.info("✅ Conexión a MongoDB establecida correctamente")
        return client
    except Exception as e:
        logger.error(f"⚠️ Error conectando a MongoDB: {str(e)}")
        return None

def normalize_field(value):
    """Normaliza campos para CSV y JSON"""
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value).replace('"', '""').replace('\n', ' ').replace('\r', '')

def get_common_fields(data):
    """Obtiene solo los campos que están presentes en todos los documentos y no están vacíos, excepto 'city' que siempre se incluye"""
    common_fields = set()
    if data:
        common_fields = set(data[0].keys())
        for doc in data[1:]:
            common_fields.intersection_update(doc.keys())
    final_fields = []
    for field in common_fields:
        if field == 'location' or field == 'wazeData':
            continue
        if field == 'city':
            final_fields.append(field)
            continue
        all_valid = True
        for doc in data:
            value = doc.get(field)
            if value is None or value == "" or (isinstance(value, (dict, list)) and not value):
                all_valid = False
                break
        if all_valid:
            final_fields.append(field)
    if 'city' not in final_fields:
        final_fields.append('city')
    return sorted(final_fields)

def export_to_csv():
    """Exporta datos de MongoDB a CSV con reintentos"""
    retry_count = 0
    while retry_count < MAX_RETRIES:
        client = None
        try:
            logger.info(f"🔍 Intento {retry_count + 1}/{MAX_RETRIES}")
            client = connect_to_mongo()
            if client is None:
                raise ConnectionError("No se pudo conectar a MongoDB")
            db = client[mongo_db]
            collection = db[mongo_collection]
            count = collection.count_documents({})
            logger.info(f"📊 Documentos encontrados en MongoDB: {count}")
            if count == 0:
                logger.warning(f"⚠️ No hay datos en MongoDB. Reintentando en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)
                retry_count += 1
                continue
            data = list(collection.find({}, {"_id": 0, "location": 0, "wazeData": 0}))
            fieldnames = get_common_fields(data)
            logger.info(f"📋 Campos seleccionados: {', '.join(fieldnames)}")
            csv_path = "/data/datos_clean.csv"
            with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in data:
                    try:
                        csv_row = []
                        for field in fieldnames:
                            value = row.get(field)
                            if field == 'city' and (value is None or value == ""):
                                value = "Santiago"
                            normalized_value = normalize_field(value)
                            csv_row.append(normalized_value)
                        writer.writerow(csv_row)
                    except Exception as e:
                        logger.error(f"⚠️ Error procesando fila: {e}")
                        continue
            logger.info(f"✅ CSV generado exitosamente en {csv_path}")
            logger.info(f"📊 Registros exportados: {len(data)}")
            if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
                logger.info("✔️ Verificación: Archivo CSV creado correctamente")
                return True
            else:
                raise Exception("El archivo CSV no se creó correctamente")
        except Exception as e:
            logger.error(f"⚠️ Error durante la exportación: {str(e)}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                logger.info(f"🔄 Reintentando en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"❌ Error: No se pudieron obtener datos después de {MAX_RETRIES} intentos")
        finally:
            if client:
                client.close()
    return False

def export_to_json():
    """Exporta datos de MongoDB a JSON con reintentos"""
    retry_count = 0
    while retry_count < MAX_RETRIES:
        client = None
        try:
            logger.info(f"🔍 Intento {retry_count + 1}/{MAX_RETRIES}")
            client = connect_to_mongo()
            if client is None:
                raise ConnectionError("No se pudo conectar a MongoDB")
            db = client[mongo_db]
            collection = db[mongo_collection]
            count = collection.count_documents({})
            logger.info(f"📊 Documentos encontrados en MongoDB: {count}")
            if count == 0:
                logger.warning(f"⚠️ No hay datos en MongoDB. Reintentando en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)
                retry_count += 1
                continue
            data = list(collection.find({}, {"_id": 0, "location": 0, "wazeData": 0}))
            for row in data:
                for key in row:
                    value = row[key]
                    if key == 'city' and (value is None or value == ""):
                        row[key] = "Santiago"
                    else:
                        row[key] = normalize_field(value)
            json_path = "/data/datos_clean.json"
            with open(json_path, "w", encoding="utf-8") as jsonfile:
                json.dump(data, jsonfile, ensure_ascii=False, indent=2)
            logger.info(f"✅ JSON generado exitosamente en {json_path}")
            logger.info(f"📊 Registros exportados: {len(data)}")
            if os.path.exists(json_path) and os.path.getsize(json_path) > 0:
                logger.info("✔️ Verificación: Archivo JSON creado correctamente")
                return True
            else:
                raise Exception("El archivo JSON no se creó correctamente")
        except Exception as e:
            logger.error(f"⚠️ Error durante la exportación: {str(e)}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                logger.info(f"🔄 Reintentando en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"❌ Error: No se pudieron obtener datos después de {MAX_RETRIES} intentos")
        finally:
            if client:
                client.close()
    return False

if __name__ == "__main__":
    logger.info("🚀 Iniciando script de exportación MongoDB a JSON")
    if export_to_json():
        logger.info("🎉 Exportación completada con éxito")
        time.sleep(300)
    else:
        logger.error("💥 Fallo crítico en la exportación")
        time.sleep(600)