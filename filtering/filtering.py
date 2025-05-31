import time
import pymongo
import pandas as pd
import numpy as np
from geopy.distance import geodesic

# Conexión a MongoDB
client = pymongo.MongoClient("mongodb://admin:admin123@mongo:27017/")

# Esperar hasta que exista la base de datos y tenga datos
while True:
    dbs = client.list_database_names()
    if "waze_db" in dbs:
        db = client["waze_db"]
        if "alertas" in db.list_collection_names():
            collection = db["alertas"]
            if collection.count_documents({}) > 0:
                break
    print("⏳ Esperando a que 'waze_db.alertas' exista y tenga datos...")
    time.sleep(5)

# Cargar datos desde MongoDB
data = pd.DataFrame(list(collection.find()))

# 🔍 Eliminar registros con datos críticos faltantes
data.dropna(subset=["type", "city", "location", "pubMillis"], inplace=True)

# 🧹 Normalizar tipo de incidente y comuna
data["type"] = data["type"].str.lower().str.strip()
data["city"] = data["city"].str.title().str.strip()

# 🕓 Convertir pubMillis a datetime
data["datetime"] = pd.to_datetime(data["pubMillis"], unit="ms")

# 📍 Extraer lat/lon de `location`
data["lat"] = data["location"].apply(lambda loc: loc["y"])
data["lon"] = data["location"].apply(lambda loc: loc["x"])

# Eliminar duplicados por coordenada y hora exacta
data.drop_duplicates(subset=["type", "city"], inplace=True)

# ✂️ Seleccionar solo columnas relevantes
data_final = data[["uuid", "type", "subtype", "city", "street", "datetime", "lat", "lon"]]

# Limpiar espacios en blanco en todos los campos de texto
for col in ["uuid", "type", "subtype", "city", "street"]:
    data[col] = data[col].astype(str).str.strip()

# Eliminar filas donde algún campo relevante esté vacío después de limpiar
data.replace("", np.nan, inplace=True)
data.dropna(subset=["uuid", "type", "subtype", "city", "street", "datetime", "lat", "lon"], inplace=True)

def agrupar_incidentes(df, max_dist_m=100, max_min=10):
    df = df.sort_values("datetime")
    usados = set()
    grupos = []
    for idx, row in df.iterrows():
        if idx in usados:
            continue
        grupo = [idx]
        usados.add(idx)
        for jdx, other in df.loc[df.index > idx].iterrows():
            if other["type"] != row["type"] or other["city"] != row["city"]:
                continue
            # Diferencia temporal en minutos
            delta_min = abs((other["datetime"] - row["datetime"]).total_seconds() / 60)
            if delta_min > max_min:
                break  # Como está ordenado, no hace falta seguir
            # Distancia geográfica
            dist = geodesic((row["lat"], row["lon"]), (other["lat"], other["lon"])).meters
            if dist <= max_dist_m:
                grupo.append(jdx)
                usados.add(jdx)
        grupos.append(grupo)
    # Fusionar: tomar el primero de cada grupo como representante
    return df.loc[[g[0] for g in grupos]].reset_index(drop=True)

# Agrupar incidentes similares por cercanía geográfica y temporal
data_final = agrupar_incidentes(data[["uuid", "type", "subtype", "city", "street", "datetime", "lat", "lon"]])

# 💾 Guardar resultado limpio
data_final.to_csv("../pig_processing/alertas_limpias.csv", index=False)
print(f"✅ Filtrado y limpieza completado: {len(data_final)} registros procesados.")
