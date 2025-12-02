import json
import uuid
import os
from datetime import datetime, timedelta, date

# Conexión
from connect import get_cassandra_session

# Tus queries
from Cassandra.cassandra_queries import (
    insert_transaction,
    get_recent_transactions,
    get_transactions_in_range,
    get_daily_totals,
    insert_alert,
    get_alerts_by_account,
    get_latest_alerts
)

# ============================
#  CARGA DE PARÁMETROS DE PRUEBA
# ============================

def load_test_params():
    """
    Carga parámetros desde JSON si existe.
    Si no, devuelve valores default válidos.
    """

    json_file = 'data/datos_cassandra_finanzas.json'

    if not os.path.exists(json_file):
        print(f"No se encontró {json_file}. Usando valores default.\n")
        return {
            "account_number": "ACC12345",
            "amount": 1200.50,
            "currency": "USD",
            "merchant": "Amazon",
            "status": "APPROVED",
            "range_start": datetime(2025, 11, 1),
            "range_end": datetime(2025, 11, 10),
            "test_day": date(2025, 11, 9)
        }

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        params = {
            "account_number": data.get("account_number", "ACC12345"),
            "amount": data.get("amount", 500.00),
            "currency": data.get("currency", "USD"),
            "merchant": data.get("merchant", "StoreX"),
            "status": data.get("status", "APPROVED"),
            "range_start": datetime.fromisoformat(data.get("range_start")),
            "range_end": datetime.fromisoformat(data.get("range_end")),
            "test_day": date.fromisoformat(data.get("test_day")),
        }

        return params

    except Exception as e:
        print(f"⚠️ Error leyendo JSON: {e}. Usando valores default.\n")
        return {
            "account_number": "ACC12345",
            "amount": 750.00,
            "currency": "MXN",
            "merchant": "Oxxo",
            "status": "APPROVED",
            "range_start": datetime(2025, 11, 1),
            "range_end": datetime(2025, 11, 10),
            "test_day": date(2025, 11, 9)
        }


# ============================
#  EJECUCIÓN DE PRUEBAS
# ============================

def run_tests():
    print("=======================================")
    print("    PRUEBAS DE CASSANDRA - FINANZAS")
    print("=======================================\n")

    cluster, session = get_cassandra_session()

    if not session:
        print("❌ No se pudo conectar a Cassandra.")
        return

    # Cargar parámetros
    p = load_test_params()

    print(f"Cuenta de prueba: {p['account_number']}")
    print(f"Monto ejemplo: {p['amount']} {p['currency']}")
    print(f"Rango fechas: {p['range_start']}  →  {p['range_end']}")
    print(f"Día total diario: {p['test_day']}\n")

    # -------------------------------------
    # REQ 1 – Insertar transacción
    # -------------------------------------
    print("--- [Req 1] Insertar transacción ---")
    res = insert_transaction(
        session,
        p["account_number"],
        p["amount"],
        p["currency"],
        p["merchant"],
        p["status"]
    )
    print("Resultado:", res, "\n")

    # -------------------------------------
    # REQ 2 – Transacciones recientes
    # -------------------------------------
    print("--- [Req 2] Últimas transacciones ---")
    res = get_recent_transactions(session, p["account_number"])
    print(f"Registros encontrados: {len(res)}")
    if res:
        print("Ejemplo:", res[0])
    print("")

    # -------------------------------------
    # REQ 3 – Transacciones en rango
    # -------------------------------------
    print("--- [Req 3] Transacciones en rango de fechas ---")
    res = get_transactions_in_range(
        session,
        p["account_number"],
        p["range_start"],
        p["range_end"]
    )
    print(f"Registros: {len(res)}")
    print("")

    # -------------------------------------
    # REQ 4 – Totales diarios
    # -------------------------------------
    print("--- [Req 4] Totales diarios de la cuenta ---")
    res = get_daily_totals(session, p["account_number"], p["test_day"])
    print("Resultado:", res, "\n")

    # -------------------------------------
    # REQ 5 – Insertar alerta
    # -------------------------------------
    print("--- [Req 5] Insertar alerta ---")
    example_tx = uuid.uuid4()
    res = insert_alert(
        session,
        p["account_number"],
        example_tx,
        "Transacción sospechosa de prueba"
    )
    print("Resultado:", res, "\n")

    # -------------------------------------
    # REQ 6 – Alerts by account
    # -------------------------------------
    print("--- [Req 6] Alertas de la cuenta ---")
    res = get_alerts_by_account(session, p["account_number"])
    print(f"Alertas encontradas: {len(res)}")
    if res:
        print("Ejemplo:", res[0])
    print("")

    # -------------------------------------
    # REQ 7 – Últimas alertas globales
    # -------------------------------------
    print("--- [Req 7] Últimas alertas globales ---")
    res = get_latest_alerts(session, limit=10)
    print(f"Total encontradas: {len(res)}")
    if res:
        print("Ejemplo:", res[0])
    print("")

    # Cerrar
    session.shutdown()
    cluster.shutdown()

    print("=======================================")
    print("        PRUEBAS COMPLETADAS")
    print("=======================================\n")


if __name__ == "__main__":
    run_tests()
