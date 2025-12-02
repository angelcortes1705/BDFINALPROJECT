import os

from pymongo import MongoClient
from cassandra.cluster import Cluster
import pydgraph
from Mongo.mongo_setup import get_mongo_client

# Mongo
def get_mongo_db():
    """
    Regresa el objeto de base de datos de MongoDB para el proyecto.

    Usa la variable de entorno MONGO_URI si existe,
    en otro caso se conecta a localhost.
    """
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(mongo_uri)
    db = client["finance"]  # mismo nombre que en mongo_setup
    return db

# Cassandra
# En connect.py

def get_cassandra_session():
    # 1. Obtener host (con fallback a localhost para evitar el error anterior)
    cassandra_host = os.getenv("CASSANDRA_HOST", "localhost")
    # Nombre de tu base de datos (Keyspace)
    keyspace = "finance" 

    cluster = Cluster([cassandra_host])
    session = cluster.connect() # Conecta al clúster sin seleccionar keyspace aún

    # 2. Crear el Keyspace si no existe (Autorreparación)
    # Esto es vital por si borraste el contenedor y está vacío
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
    except Exception as e:
        print(f"Advertencia al crear keyspace: {e}")

    # 3. USAR el Keyspace (Esto soluciona tu error actual)
    session.set_keyspace(keyspace)

    print(f"Conectado a Cassandra en {cassandra_host} usando keyspace '{keyspace}'")

    return cluster, session

# Dgraph
def get_dgraph_client():
    DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')
    dgraph_addr = os.getenv("DGRAPH_ADDR", DGRAPH_URI)
    stub = pydgraph.DgraphClientStub(dgraph_addr)
    client = pydgraph.DgraphClient(stub)

    print(f"Cliente de Dgraph preparado para {dgraph_addr}")

    return client, stub

# Prueba de conexiones
def main():
    mongo_client = None
    cassandra_cluster = None
    cassandra_session = None
    dgraph_client = None
    dgraph_stub = None

    print("=== Probando conexiones a las bases de datos ===")

    # Mongo
    try:
        mongo_client = get_mongo_client()
    except Exception as e:
        print(f"No se pudo conectar a MongoDB: {e}")

    # Cassandra
    try:
        cassandra_cluster, cassandra_session = get_cassandra_session()
    except Exception as e:
        print(f"No se pudo conectar a Cassandra: {e}")

    # Dgraph
    try:
        dgraph_client, dgraph_stub = get_dgraph_client()
    except Exception as e:
        print(f"No se pudo conectar a Dgraph: {e}")

    print("=== Finalizó la prueba de conexiones ===")

    # Cierre de conexiones
    if mongo_client:
        mongo_client.close()

    if cassandra_session:
        cassandra_session.shutdown()
    if cassandra_cluster:
        cassandra_cluster.shutdown()

    if dgraph_stub:
        dgraph_stub.close()


if __name__ == "__main__":
    main()