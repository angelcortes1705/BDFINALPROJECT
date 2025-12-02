import uuid
from datetime import datetime, date



#  1. Insertar una transacción en transaction_history
def insert_transaction(session, account_number, amount, currency, merchant, status, raw_payload=""):
    """
    Inserta una transacción completa en Cassandra.
    """
    query = """
        INSERT INTO transaction_history (
            account_number, timestamp, transaction_id,
            amount, currency, merchant, status, raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    tx_id = uuid.uuid4()
    now = datetime.utcnow()

    try:
        session.execute(query, (
            account_number,
            now,
            tx_id,
            amount,
            currency,
            merchant,
            status,
            raw_payload
        ))
        return {"transaction_id": tx_id, "timestamp": now}
    except Exception as e:
        print(f"Error insertando transacción: {e}")
        return None


#  2. Obtener transacciones recientes por cuenta
def get_recent_transactions(session, account_number, limit=20):
    """
    Devuelve las N transacciones más recientes.
    """
    query = """
        SELECT timestamp, transaction_id, amount, currency, merchant, status 
        FROM transaction_history 
        WHERE account_number = %s 
        LIMIT %s
    """
    try:
        rows = session.execute(query, (account_number, limit))
        results = []
        for row in rows:
            results.append({
                "timestamp": row.timestamp,
                "transaction_id": row.transaction_id,
                "amount": row.amount,
                "currency": row.currency,
                "merchant": row.merchant,
                "status": row.status
            })
        return results
    except Exception as e:
        print(f"Error en get_recent_transactions: {e}")
        return []



#  3. Obtener transacciones en un rango de fechas

def get_transactions_in_range(session, account_number, start_ts, end_ts):
    """
    Recupera transacciones de una cuenta entre start_ts y end_ts.
    """
    query = """
        SELECT timestamp, transaction_id, amount, currency, merchant, status 
        FROM transaction_history
        WHERE account_number = %s
        AND timestamp >= %s AND timestamp <= %s
    """
    try:
        rows = session.execute(query, (account_number, start_ts, end_ts))
        results = []
        for row in rows:
            results.append({
                "timestamp": row.timestamp,
                "transaction_id": row.transaction_id,
                "amount": row.amount,
                "currency": row.currency,
                "merchant": row.merchant,
                "status": row.status
            })
        return results
    except Exception as e:
        print(f"Error en get_transactions_in_range: {e}")
        return []



#  4. Obtener totales diarios de una cuenta

def get_daily_totals(session, account_number, day_date):
    """
    Recupera total del día de una cuenta (monto y número de transacciones).
    """
    query = """
        SELECT total_amount, transaction_count
        FROM daily_totals
        WHERE account_number = %s AND date = %s
    """
    try:
        row = session.execute(query, (account_number, day_date)).one()
        if row:
            return {
                "account_number": account_number,
                "date": day_date,
                "total_amount": row.total_amount,
                "transaction_count": row.transaction_count
            }
        return None
    except Exception as e:
        print(f"Error en get_daily_totals: {e}")
        return None



#  5. Insertar alerta de fraude

def insert_alert(session, account_number, transaction_id, reason):
    """
    Inserta una alerta en la tabla alerts.
    """
    query = """
        INSERT INTO alerts (
            alert_id, timestamp, account_number, transaction_id, reason
        ) VALUES (%s, %s, %s, %s, %s)
    """

    alert_id = uuid.uuid4()
    now = datetime.utcnow()

    try:
        session.execute(query, (
            alert_id,
            now,
            account_number,
            transaction_id,
            reason
        ))
        return {"alert_id": alert_id, "timestamp": now}
    except Exception as e:
        print(f"Error insertando alerta: {e}")
        return None



#  6. Obtener alertas por cuenta

def get_alerts_by_account(session, account_number):
    """
    Recupera todas las alertas asociadas a una cuenta.
    """
    query = """
        SELECT alert_id, timestamp, transaction_id, reason
        FROM alerts
        WHERE account_number = %s
    """

    try:
        rows = session.execute(query, (account_number,))
        results = []
        for row in rows:
            results.append({
                "alert_id": row.alert_id,
                "timestamp": row.timestamp,
                "transaction_id": row.transaction_id,
                "reason": row.reason
            })
        return results
    except Exception as e:
        print(f"Error en get_alerts_by_account: {e}")
        return []



#  7. Obtener últimas N alertas globales

def get_latest_alerts(session, limit=20):
    """
    Devuelve las últimas alertas generales registradas.
    (No hay ORDER en Cassandra, así que se filtra por timestamp después.)
    """
    query = "SELECT alert_id, timestamp, account_number, transaction_id, reason FROM alerts LIMIT 200"

    try:
        rows = list(session.execute(query))
        rows.sort(key=lambda r: r.timestamp, reverse=True)
        rows = rows[:limit]

        results = []
        for row in rows:
            results.append({
                "alert_id": row.alert_id,
                "timestamp": row.timestamp,
                "account_number": row.account_number,
                "transaction_id": row.transaction_id,
                "reason": row.reason
            })
        return results
    except Exception as e:
        print(f"Error en get_latest_alerts: {e}")
        return []