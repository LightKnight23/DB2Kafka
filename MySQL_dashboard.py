import datetime
import random
import json
import time
import pymysql

def generate_continuous_data(start_date, interval_seconds):
    current_date = start_date
    temperature = 25.0  # Temperatura inicial
    data_id = 1  # Inicializar el ID en 1
    while True:
        # Generar temperatura aleatoria entre 20 y 40 grados Celsius
        temperature = random.uniform(max(20, temperature - 2), min(40.01, temperature + 2))
        # Generar humedad aleatoria entre 30% y 70%
        humidity = random.uniform(30, 70)
        data = {
            "id": data_id,  # Agregar el ID al diccionario generado
            "fecha_hora": current_date.strftime('%Y-%m-%d %H:%M:%S'),
            "temperatura": round(temperature, 2),
            "humedad": round(humidity, 2)
        }
        yield data
        data_id += 1  # Incrementar el ID
        current_date += datetime.timedelta(seconds=interval_seconds)
        time.sleep(interval_seconds)  # Esperar el tiempo especificado antes de la próxima generación de datos

# Configuración inicial
start_date = datetime.datetime(2024, 4, 25, 14, 30, 0)  # Fecha de inicio
interval_seconds = 0.7  # Intervalo de tiempo entre registros (en segundos)

# Conexión a la base de datos MySQL
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="221200",
    database="sensor"  # Cambia el nombre de la base de datos si es diferente
)
cursor = conn.cursor()

# Generar datos continuos en un bucle infinito
data_generator = generate_continuous_data(start_date, interval_seconds)
for data in data_generator:
    # Insertar datos en la tabla datos
    insert_query = "INSERT INTO datos (id, fecha, temperatura, humedad) VALUES (%s, %s, %s, %s)"
    cursor.execute(insert_query, (data["id"], data["fecha_hora"], data["temperatura"], data["humedad"]))
    conn.commit()
    print("Datos insertados:", data)