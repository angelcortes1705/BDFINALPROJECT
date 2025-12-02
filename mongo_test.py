from datetime import datetime, timedelta
from bson import ObjectId

from connect import get_mongo_db
from Mongo import mongo_queries as q


# ================================================================
# Conexión
# ================================================================
db = get_mongo_db()
print("Conectado a MongoDB\n")


# ================================================================
# REQ 1: Crear y consultar usuarios
# ================================================================
print("=== PRUEBA REQ 1: Crear usuario y obtener por email ===")
u = q.create_user(
    db,
    name="Test User",
    email="test@example.com",
    password_hash="HASH123",
    role="admin"
)
print("Usuario creado:", u.inserted_id)

user = q.find_user_by_email(db, "test@example.com")
print("Usuario encontrado:", user, "\n")


# ================================================================
# REQ 2: Log de accesos y consulta
# ================================================================
print("=== PRUEBA REQ 2: Log de accesos ===")

q.log_user_access(db, str(user["_id"]))
q.log_user_access(db, str(user["_id"]))

access_log = q.get_user_access_log(db, str(user["_id"]))
print("Accesos registrados:", len(access_log))
print(access_log, "\n")


# ================================================================
# REQ 3: CRUD de clientes
# ================================================================
print("=== PRUEBA REQ 3: CRUD clientes ===")

client_id = q.create_client(db, {
    "name": "Cliente Prueba",
    "email": "cliente@correo.com",
    "curp": "ABCD123456XXXXXX",
    "country": "Mexico"
}).inserted_id

print("Cliente creado:", client_id)

print("Cliente encontrado:", q.get_client(db, str(client_id)))

q.update_client(db, str(client_id), {"country": "USA"})
print("Cliente actualizado:", q.get_client(db, str(client_id)))

print("Buscar por email/curp:",
      q.search_client_by_identifier(db, "cliente@correo.com"))

# No eliminamos para que quede como dato base para la siguiente prueba
print("")


# ================================================================
# REQ 4: CRUD de cuentas + relación cliente → cuentas
# ================================================================
print("=== PRUEBA REQ 4: CRUD cuentas ===")

acc_id = q.create_account(
    db,
    client_id=str(client_id),
    account_type="debit",
    balance=1000.0
).inserted_id

print("Cuenta creada:", acc_id)

print("Cuenta consultada:", q.get_account(db, str(acc_id)))

q.update_account(db, str(acc_id), {"balance": 1500})
print("Cuenta después de update:", q.get_account(db, str(acc_id)), "\n")


# ================================================================
# REQ 5: Operaciones en cuenta (deposit/withdraw)
# ================================================================
print("=== PRUEBA REQ 5: Operaciones ===")

op1 = q.create_operation(db, str(acc_id), "deposit", 300)
op2 = q.create_operation(db, str(acc_id), "withdraw", 150)

print("Operaciones insertadas:", op1.inserted_id, op2.inserted_id)

ops = q.get_account_operations(db, str(acc_id))
print("Operaciones de la cuenta:")
for o in ops:
    print(o)
print("")


# ================================================================
# REQ 6: Metadata de transacciones
# ================================================================
print("=== PRUEBA REQ 6: Insertar y obtener transacción ===")

tx_id = q.insert_transaction_metadata(db, {
    "account_id": str(acc_id),
    "amount": 250.75,
    "merchant": "Amazon",
    "status": "APPROVED",
    "timestamp": datetime.utcnow(),
    "description": "Compra en Amazon ejemplo"
}).inserted_id

print("Transacción creada:", tx_id)

print("Transacción obtenida:", q.get_transaction(db, str(tx_id)), "\n")


# ================================================================
# REQ 7: Buscar transacciones por campos
# ================================================================
print("=== PRUEBA REQ 7: search_transactions ===")

print(q.search_transactions(
    db,
    account_id=str(acc_id),
    merchant="Amazon",
    status="APPROVED"
), "\n")


# ================================================================
# REQ 8: Totales diarios
# ================================================================
print("=== PRUEBA REQ 8: get_daily_totals ===")

today = datetime.utcnow()
print(q.get_daily_totals(db, str(acc_id), today), "\n")


# ================================================================
# REQ 9: Totales mensuales por cuenta
# ================================================================
print("=== PRUEBA REQ 9: monthly_totals_per_account ===")

print(q.monthly_totals_per_account(
    db,
    account_id=str(acc_id),
    year=today.year,
    month=today.month
), "\n")


# ================================================================
# REQ 10: search_transactions_by_fields (monto, merchant, fecha)
# ================================================================
print("=== PRUEBA REQ 10: search_transactions_by_fields ===")

print(q.search_transactions_by_fields(
    db,
    min_amount=100,
    merchant="Amazon",
    date=today
))
print("\n")

print("=== PRUEBAS COMPLETADAS ===")
