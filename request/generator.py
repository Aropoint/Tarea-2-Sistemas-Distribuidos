import random
import time
import logging
import os
import requests
from datetime import datetime
from collections import defaultdict
import numpy as np
import json

if not os.path.exists('request'):
    os.makedirs('request')

logging.basicConfig(
    filename='request/request_wazer_logs.txt',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

hit_count = 0
miss_count = 0
total_requests = 0
id_access_count = defaultdict(int)

# Estadísticas por ID -> con esto despúes generamos lo gráficos 
id_stats = defaultdict(lambda: {
    "requests": 0,
    "hits": 0,
    "misses": 0,
    "total_time": 0.0
})

# URLs del backend
BACKEND_URL = "http://redis-cache:5000/alerts"
ALL_IDS_URL = "http://redis-cache:5000/alerts/ids"

# Condicional para cambiar la distribución
CONDITIONAL_DISTRIBUTION = 0  # 1 para uniforme, 0 para exponencial

# Control de la escala de la distribución exponencial
scale_min = 0.2  # Escala mínima
scale_max = 5.0  # Escala máxima
scale_increment = 0.3  # Incremento de la escala por ciclo
current_scale = scale_min  # Escala inicial

# se hace el fetch al endpoint de ids y se eligen aleatoriamente 1000 ids
def fetch_random_ids(sample_size=100):
    try:
        response = requests.get(ALL_IDS_URL)
        if response.status_code == 200:
            all_ids = response.json().get("ids", [])
            return random.sample(all_ids, sample_size)
        else:
            logging.error(f"❌ Error fetching IDs: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"💥 Exception fetching IDs: {e}")
        return []

# aca se genera el plan de requests, se eligen aleatoriamente x ids frecuentes y x no frecuentes
def generate_requests_plan(selected_ids, min_repeats=1, max_repeats=3, target_length=60, num_frequent_ids=10):
    requests_plan = []

    frequent_ids = random.sample(selected_ids, num_frequent_ids)
    non_frequent_ids = list(set(selected_ids) - set(frequent_ids))

    while len(requests_plan) < target_length:
        for _id in frequent_ids:
            repeats = random.randint(2, max_repeats)
            requests_plan.extend([_id] * repeats)
            if len(requests_plan) >= target_length:
                break

        for _id in random.sample(non_frequent_ids, min(len(non_frequent_ids), 10)):
            repeats = random.randint(1, 2)
            requests_plan.extend([_id] * repeats)
            if len(requests_plan) >= target_length:
                break

    random.shuffle(requests_plan)
    return requests_plan[:target_length]

def main():
    global hit_count, miss_count, total_requests, current_scale

    selected_ids = fetch_random_ids(1000)
    if not selected_ids:
        print("No se pudieron obtener los IDs.")
        return
    print(f"✅ IDs seleccionados: {selected_ids}")

    cycle_count = 0  # Contador de ciclos

    while True:
        requests_plan = generate_requests_plan(selected_ids, min_repeats=1, max_repeats=3, target_length=60)
        # si el condicional es 1, se elige la distribución uniforme, si no, se elige la exponencial
        if CONDITIONAL_DISTRIBUTION == 1:
            for _id in requests_plan:
                id_access_count[_id] += 1
                total_requests += 1
                params = {"id": _id}

                start_time = time.time()
                try:
                    response = requests.get(BACKEND_URL, params=params)
                    elapsed_time = time.time() - start_time

                    id_stats[_id]["requests"] += 1
                    id_stats[_id]["total_time"] += elapsed_time

                    if response.status_code == 200:
                        result = response.json()
                        source = result.get("source", "unknown")
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        if source == "cache":
                            hit_count += 1
                            id_stats[_id]["hits"] += 1
                            log = f"[CACHE HIT] id={_id} at {current_time} | Time: {elapsed_time:.3f}s"
                        else:
                            miss_count += 1
                            id_stats[_id]["misses"] += 1
                            log = f"[CACHE MISS] id={_id} at {current_time} | Time: {elapsed_time:.3f}s"

                        print(log)
                        logging.info(log)

                        if total_requests % 10 == 0:
                            hit_rate = (hit_count / total_requests) * 100
                            miss_rate = (miss_count / total_requests) * 100
                            stats = f"Total Requests: {total_requests} | Hits: {hit_count} | Misses: {miss_count} | Hit Rate: {hit_rate:.2f}% | Miss Rate: {miss_rate:.2f}%"
                            print(stats)
                            logging.info(stats)
                    else:
                        logging.warning(f"❌ Error {response.status_code} for id={_id}")
                except Exception as e:
                    logging.error(f"💥 Exception during request: {e}")

                wait_time = random.uniform(0.1, 1)
                time.sleep(wait_time)
        else:
            for _id in requests_plan:
                id_access_count[_id] += 1
                total_requests += 1
                params = {"id": _id}

                start_time = time.time()
                try:
                    response = requests.get(BACKEND_URL, params=params)
                    elapsed_time = time.time() - start_time

                    id_stats[_id]["requests"] += 1
                    id_stats[_id]["total_time"] += elapsed_time

                    if response.status_code == 200:
                        result = response.json()
                        source = result.get("source", "unknown")
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        if source == "cache":
                            hit_count += 1
                            id_stats[_id]["hits"] += 1
                            log = f"[CACHE HIT] id={_id} at {current_time} | Time: {elapsed_time:.3f}s"
                        else:
                            miss_count += 1
                            id_stats[_id]["misses"] += 1
                            log = f"[CACHE MISS] id={_id} at {current_time} | Time: {elapsed_time:.3f}s"

                        print(log)
                        logging.info(log)

                        if total_requests % 10 == 0:
                            hit_rate = (hit_count / total_requests) * 100
                            miss_rate = (miss_count / total_requests) * 100
                            stats = f"Total Requests: {total_requests} | Hits: {hit_count} | Misses: {miss_count} | Hit Rate: {hit_rate:.2f}% | Miss Rate: {miss_rate:.2f}%"
                            print(stats)
                            logging.info(stats)
                    else:
                        logging.warning(f"❌ Error {response.status_code} for id={_id}")
                except Exception as e:
                    logging.error(f"💥 Exception during request: {e}")

                wait_time = np.random.exponential(scale=current_scale)
                time.sleep(wait_time)

        cycle_count += 1
        if cycle_count % 10 == 0:  
            current_scale = min(current_scale + scale_increment, scale_max)

        with open("request/id_stats.json", "w") as f:
            json.dump(id_stats, f, indent=4)

        print("\n🔄 Generando nuevo ciclo de peticiones...\n")
        logging.info("🔄 Generando nuevo ciclo de peticiones...")
        time.sleep(2)

if __name__ == "__main__":
    main()
