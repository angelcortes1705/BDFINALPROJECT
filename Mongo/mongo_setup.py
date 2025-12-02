"""
mongo_setup.py

Script para crear las colecciones de MongoDB 

Colecciones:
- customers
- accounts
- transactions (metadata)
"""

from pymongo import MongoClient, ASCENDING, TEXT

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "fraud_detection"


def get_mongo_client(uri: str = MONGO_URI) -> MongoClient:
    """Crea y devuelve un cliente de MongoDB."""
    return MongoClient(uri)


def create_collections(db):
    """
    Asegura que las colecciones existan.
    En MongoDB se crean automáticamente al insertar,
    pero aquí se fuerzan para documentar el modelo.
    """

    existing = db.list_collection_names()

    if "customers" not in existing:
        db.create_collection("customers")

    if "accounts" not in existing:
        db.create_collection("accounts")

    if "transactions" not in existing:
        db.create_collection("transactions")


def create_indexes(db):
    """
    Crea los índices necesarios en cada colección según los
    requerimientos del proyecto.
    """

    # ================================================================
    # CUSTOMERS
    # ================================================================
    customers = db.customers

    # Email único – identificación del cliente
    customers.create_index(
        [("email", ASCENDING)],
        unique=True,
        name="idx_customers_email_unique"
    )

    # País -> segmentación de clientes
    customers.create_index(
        [("country", ASCENDING)],
        name="idx_customers_country"
    )

    # Índice para buscar clientes por nombre
    customers.create_index(
        [("name", TEXT)],
        name="idx_customers_name_text"
    )


    # ================================================================
    # ACCOUNTS
    # ================================================================
    accounts = db.accounts

    # Número de cuenta único
    accounts.create_index(
        [("account_number", ASCENDING)],
        unique=True,
        name="idx_accounts_account_number_unique"
    )

    # Relación cliente → cuentas del cliente
    accounts.create_index(
        [("customer_id", ASCENDING)],
        name="idx_accounts_customer_id"
    )

    # Tipo de cuenta (débito, crédito, etc.)
    accounts.create_index(
        [("account_type", ASCENDING)],
        name="idx_accounts_type"
    )


    # ================================================================
    # TRANSACTIONS (metadatos)
    # ================================================================
    transactions = db.transactions

    # Acceso rápido a transacciones por cuenta
    transactions.create_index(
        [("account_number", ASCENDING)],
        name="idx_transactions_account"
    )

    # Filtros por rango de fechas (metadata)
    transactions.create_index(
        [("timestamp", ASCENDING)],
        name="idx_transactions_timestamp"
    )

    # Búsquedas por tipo de comercio o categoria
    transactions.create_index(
        [("merchant", ASCENDING)],
        name="idx_transactions_merchant"
    )

    # Índice de texto en descripción (opcional)
    transactions.create_index(
        [("description", TEXT)],
        name="idx_transactions_description_text"
    )


def main():
    client = get_mongo_client()
    db = client[DB_NAME]

    print(f"Conectado a MongoDB, base de datos: {DB_NAME}")

    create_collections(db)
    print("Colecciones creadas / verificadas.")

    create_indexes(db)
    print("Índices creados / verificados.")

    client.close()
    print("Conexión cerrada.")


if __name__ == "__main__":
    main()
