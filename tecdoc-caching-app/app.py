import os
import json
import sqlite3
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from apify_client import ApifyClient
from apify_client.errors import ApifyClientError

# 1. Cargar Variables de Entorno
load_dotenv()

app = Flask(__name__)

# 2. Configuración Global
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
TECDOC_ACTOR_ID = os.getenv("TECDOC_ACTOR_ID", "making-data-meaningful/tecdoc")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "cache.db")

# Inicializar el cliente de Apify
if APIFY_TOKEN:
    apify_client = ApifyClient(APIFY_TOKEN)
else:
    print("FATAL: APIFY_TOKEN no configurado.")
    # Si la clave es esencial, podrías terminar la aplicación aquí.

# --- Funciones de Base de Datos SQLite ---

def get_db_connection():
    """Establece y devuelve una conexión a la base de datos SQLite."""
    try:
        # La conexión es thread-safe en Flask si se usa el mismo hilo de request.
        conn = sqlite3.connect(SQLITE_DB_PATH)
        # Permite acceder a las columnas por nombre
        conn.row_factory = sqlite3.Row 
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos SQLite: {e}")
        return None

def initialize_db():
    """Crea la tabla 'vehicle_cache' si no existe."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        # Usamos TEXT para el 'data' y guardamos la representación JSON como string.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_cache (
                cache_key TEXT PRIMARY KEY, 
                data TEXT NOT NULL,
                retrieved_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print(f"✅ DB inicializada en: {SQLITE_DB_PATH}")
    except Exception as e:
        print(f"Error al inicializar la DB SQLite: {e}")
    finally:
        if conn:
            conn.close()

# Inicializar la base de datos al inicio de la aplicación
initialize_db()

# --- Funciones de Lógica de Caching ---

def create_cache_key(make, model, year):
    """Genera una clave estandarizada para el cacheo."""
    return f"{make.strip()}_{model.strip()}_{year}".upper()

def check_cache(cache_key):
    """Busca los datos del vehículo en la base de datos local."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute("SELECT data FROM vehicle_cache WHERE cache_key = ?", (cache_key,))
        result = cur.fetchone()
        
        if result:
            print(f"✅ Cache Hit para: {cache_key}")
            # Deserializar el string JSON a un objeto Python
            return json.loads(result['data']) 
        else:
            print(f"❌ Cache Miss para: {cache_key}")
            return None
    finally:
        if conn:
            conn.close()

def save_to_cache(cache_key, data):
    """Guarda o actualiza los datos en la base de datos."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        # Serializar el objeto Python a string JSON
        data_json = json.dumps(data) 
        
        # Insertar o actualizar. SQLite usa REPLACE INTO para esta lógica.
        cur.execute(
            """
            INSERT OR REPLACE INTO vehicle_cache (cache_key, data, retrieved_at) 
            VALUES (?, ?, CURRENT_TIMESTAMP);
            """,
            (cache_key, data_json)
        )
        conn.commit()
        print(f"💾 Datos guardados en caché para: {cache_key}")
    except Exception as e:
        print(f"Error al guardar en caché: {e}")
    finally:
        if conn:
            conn.close()

def call_apify_api(make, model, year):
    """Llama al Actor de Apify para obtener los datos del vehículo."""
    print("⏳ Llamando a la API de Apify...")

    run_input = {
        "make": make,
        "model": model,
        "year": int(year),
    }

    try:
        run = apify_client.actor(TECDOC_ACTOR_ID).call(run_input=run_input, timeout_secs=600)

        if run and run.get('defaultDatasetId'):
            dataset_items = apify_client.dataset(run['defaultDatasetId']).list_items().items
            
            if dataset_items:
                print(f"🎉 API Success: {len(dataset_items)} ítems encontrados.")
                return dataset_items
            else:
                print("API Success: Dataset vacío.")
                return []
        
        return []

    except ApifyClientError as e:
        print(f"Error específico de Apify Client: {e}")
        return None
    except Exception as e:
        print(f"Error general al llamar a Apify: {e}")
        return None

# --- Endpoint de la API Web (Flask) ---

@app.route('/vehicle-data', methods=['GET'])
def get_vehicle_data():
    """
    Endpoint: /vehicle-data?make=AUDI&model=A4&year=2020
    Realiza la búsqueda con la lógica de Caching.
    """
    
    make = request.args.get('make')
    model = request.args.get('model')
    year_str = request.args.get('year')

    if not all([make, model, year_str]):
        return jsonify({"error": "Faltan parámetros. Se requiere 'make', 'model' y 'year'."}), 400
    
    try:
        # Validación simple del año
        year = int(year_str) 
    except ValueError:
        return jsonify({"error": "El parámetro 'year' debe ser un número entero válido."}), 400

    cache_key = create_cache_key(make, model, year_str)

    # 1. Verificar Cache
    cached_data = check_cache(cache_key)
    
    if cached_data:
        # Retornar datos cacheados
        return jsonify({
            "source": "cache",
            "message": "Datos recuperados de la base de datos local (Cache Hit).",
            "query": {"make": make, "model": model, "year": year_str},
            "data": cached_data
        })
    else:
        # 2. Llamar a la API de Apify
        api_data = call_apify_api(make, model, year_str)

        if api_data is None:
            return jsonify({"source": "apify_api", "message": "Error al comunicarse con la API de Apify. Revisa logs."}), 503
        
        if api_data:
            # 3. Éxito de la API: Guardar en caché y retornar
            save_to_cache(cache_key, api_data)
            return jsonify({
                "source": "apify_api",
                "message": "Datos recuperados y guardados en caché (Cache Miss).",
                "query": {"make": make, "model": model, "year": year_str},
                "data": api_data
            })
        else:
            # 4. No hay datos
            return jsonify({
                "source": "apify_api", 
                "message": "Consulta exitosa en la API, pero no se encontraron datos de vehículo para la combinación especificada."
            }), 404

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
