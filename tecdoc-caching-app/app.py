import os
import json
import psycopg2
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from apify_client import ApifyClient
from apify_client.errors import ApifyClientError

# 1. Cargar Variables de Entorno
# Lee el archivo .env para obtener credenciales (DB_HOST, APIFY_TOKEN, etc.)
load_dotenv()

app = Flask(__name__)

# 2. Configuración Global
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
# Usamos el ID del Actor que busca en el catálogo de TecDoc
TECDOC_ACTOR_ID = os.getenv("TECDOC_ACTOR_ID", "making-data-meaningful/tecdoc")

# Credenciales de la Base de Datos
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Inicializar el cliente de Apify
if APIFY_TOKEN:
    apify_client = ApifyClient(APIFY_TOKEN)
else:
    # Esto es crítico: la app no puede funcionar sin el token.
    print("FATAL: APIFY_TOKEN no configurado.")
    # Considera elevar un error o forzar el cierre si la clave es esencial.

# --- Funciones de Base de Datos ---

def get_db_connection():
    """Intenta establecer y devolver una conexión a la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: Asegúrate de que las variables de .env sean correctas. {e}")
        return None

def initialize_db():
    """Crea la tabla 'vehicle_cache' si no existe, usando JSONB para flexibilidad."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_cache (
                # Clave única para el cacheo (ej: AUDI_A4_2020)
                cache_key VARCHAR(255) PRIMARY KEY, 
                # Columna para guardar la respuesta completa de la API en formato JSON
                data JSONB NOT NULL,
                retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    except Exception as e:
        print(f"Error al inicializar la DB. ¿Las credenciales son correctas? {e}")
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
        cur.execute("SELECT data FROM vehicle_cache WHERE cache_key = %s;", (cache_key,))
        result = cur.fetchone()
        
        if result:
            print(f"✅ Cache Hit: Datos encontrados para {cache_key}")
            return result[0] # Retorna el JSONB
        else:
            print(f"❌ Cache Miss: No hay datos para {cache_key}")
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
        # Usamos ON CONFLICT para actualizar si la clave ya existe
        cur.execute(
            """
            INSERT INTO vehicle_cache (cache_key, data) 
            VALUES (%s, %s)
            ON CONFLICT (cache_key) DO UPDATE 
            SET data = EXCLUDED.data, retrieved_at = CURRENT_TIMESTAMP;
            """,
            (cache_key, json.dumps(data))
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

    # El input debe coincidir con los parámetros que espera el Actor de TecDoc
    run_input = {
        "make": make,
        "model": model,
        "year": int(year),
        # Puedes añadir otros parámetros aquí (e.g., region, language)
    }

    try:
        # Ejecutar el Actor. La llamada es bloqueante hasta que el Actor finalice.
        run = apify_client.actor(TECDOC_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=600 # Define un tiempo límite alto para Actors largos
        )

        # 1. Verificar si el run fue exitoso y tiene un dataset
        if run and run.get('defaultDatasetId'):
            # 2. Obtener los resultados del dataset generado
            dataset_items = apify_client.dataset(run['defaultDatasetId']).list_items().items
            
            if dataset_items:
                print(f"🎉 API Success: {len(dataset_items)} ítems encontrados.")
                return dataset_items
            else:
                print("API Success: Dataset vacío (no se encontraron datos para la consulta).")
                return []
        
        return [] # Retorna lista vacía si no hay dataset o run falló silenciosamente

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
    
    # 1. Obtener y validar parámetros de la URL
    make = request.args.get('make')
    model = request.args.get('model')
    year_str = request.args.get('year') # Leemos como string para validar

    if not all([make, model, year_str]):
        return jsonify({"error": "Faltan parámetros. Se requiere 'make', 'model' y 'year'."}), 400
    
    try:
        # Intentamos convertir el año a entero para consistencia con la API
        year = int(year_str)
    except ValueError:
        return jsonify({"error": "El parámetro 'year' debe ser un número entero válido."}), 400

    cache_key = create_cache_key(make, model, year_str)

    # 2. Lógica de Caching: Intentar obtener datos de la caché
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
        # 3. Cache Miss: Llamar a la API de Apify
        api_data = call_apify_api(make, model, year_str)

        if api_data is None:
             # Retornar error si la llamada falló (por token, red, etc.)
            return jsonify({"source": "apify_api", "message": "Error al comunicarse con la API de Apify. Revisa logs."}), 503
        
        if api_data:
            # 4. Éxito de la API: Guardar en caché y retornar
            save_to_cache(cache_key, api_data)
            return jsonify({
                "source": "apify_api",
                "message": "Datos recuperados y guardados en caché (Cache Miss).",
                "query": {"make": make, "model": model, "year": year_str},
                "data": api_data
            })
        else:
            # 5. La API fue contactada, pero no encontró datos para la consulta
            return jsonify({
                "source": "apify_api", 
                "message": "Consulta exitosa en la API, pero no se encontraron datos de vehículo para la combinación especificada."
            }), 404

# Fin del script: Esto es necesario para ejecutar Flask directamente si no usas 'flask run'
if __name__ == '__main__':
    # En un entorno Docker, la línea CMD en el Dockerfile debe ser usada.
    # Esto es más para fines de prueba local.
    app.run(debug=True, host='0.0.0.0', port=5000)
