"""
Mongo/mongo_queries.py
Consultas de MongoDB adaptadas para el sistema bancario antifraude.
"""

from typing import Dict, Any, List, Optional
from bson import ObjectId
from datetime import datetime


# ============================================================
# =============== AUTHENTICATION / USERS ======================
# ============================================================

def create_user(db, name: str, email: str, password_hash: str, role: str):
    return db.users.insert_one({
        "name": name,
        "email": email,
        "password_hash": password_hash,
        "role": role,
        "created_at": datetime.utcnow()
    })


def find_user_by_email(db, email: str):
    return db.users.find_one({"email": email})


def log_user_access(db, user_id: str):
    return db.user_access.insert_one({
        "user_id": ObjectId(user_id),
        "timestamp": datetime.utcnow()
    })


def get_user_access_log(db, user_id: str):
    return list(db.user_access.find({"user_id": ObjectId(user_id)}))


def update_user_password(db, user_id: str, new_pass_hash: str):
    return db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"password_hash": new_pass_hash}}
    )


# ============================================================
# ===================== CLIENTS CRUD =========================
# ============================================================

def create_client(db, client_data: Dict[str, Any]):
    client_data["created_at"] = datetime.utcnow()
    return db.clients.insert_one(client_data)


def get_client(db, client_id: str):
    return db.clients.find_one({"_id": ObjectId(client_id)})


def update_client(db, client_id: str, updated_data: Dict[str, Any]):
    return db.clients.update_one(
        {"_id": ObjectId(client_id)},
        {"$set": updated_data}
    )


def delete_client(db, client_id: str):
    return db.clients.delete_one({"_id": ObjectId(client_id)})


def search_client_by_identifier(db, value: str):
    return list(db.clients.find({
        "$or": [
            {"email": value},
            {"curp": value}
        ]
    }))


# ============================================================
# ==================== ACCOUNTS CRUD =========================
# ============================================================

def create_account(db, client_id: str, account_type: str, balance: float):
    return db.accounts.insert_one({
        "client_id": ObjectId(client_id),
        "account_type": account_type,
        "balance": balance,
        "created_at": datetime.utcnow()
    })


def get_account(db, account_id: str):
    return db.accounts.find_one({"_id": ObjectId(account_id)})


def update_account(db, account_id: str, new_data: Dict[str, Any]):
    return db.accounts.update_one(
        {"_id": ObjectId(account_id)},
        {"$set": new_data}
    )


def delete_account(db, account_id: str):
    return db.accounts.delete_one({"_id": ObjectId(account_id)})


# ============================================================
# ================= OPERATIONS ON ACCOUNTS ===================
# ============================================================

def create_operation(db, account_id: str, op_type: str, amount: float):
    return db.operations.insert_one({
        "account_id": ObjectId(account_id),
        "type": op_type,
        "amount": amount,
        "timestamp": datetime.utcnow()
    })


def get_account_operations(db, account_id: str):
    return list(db.operations.find({
        "account_id": ObjectId(account_id)
    }).sort("timestamp", -1))


def update_operation(db, op_id: str, data: Dict[str, Any]):
    return db.operations.update_one(
        {"_id": ObjectId(op_id)},
        {"$set": data}
    )


def delete_operation(db, op_id: str):
    return db.operations.delete_one({"_id": ObjectId(op_id)})


# ============================================================
# =============== TRANSACTION METADATA QUERIES ===============
# ============================================================

def insert_transaction_metadata(db, tx_data: Dict[str, Any]):
    tx_data["ingested_at"] = datetime.utcnow()
    return db.transactions.insert_one(tx_data)


def get_transaction(db, tx_id: str):
    return db.transactions.find_one({"_id": ObjectId(tx_id)})


def search_transactions(db,
                        account_id: Optional[str] = None,
                        merchant: Optional[str] = None,
                        status: Optional[str] = None):
    query = {}
    if account_id:
        query["account_id"] = account_id
    if merchant:
        query["merchant"] = merchant
    if status:
        query["status"] = status

    return list(db.transactions.find(query))


def get_daily_totals(db, account_id: str, date: datetime):
    start = datetime(date.year, date.month, date.day)
    end = start.replace(day=start.day + 1)

    pipeline = [
        {"$match": {
            "account_id": account_id,
            "timestamp": {"$gte": start, "$lt": end}
        }},
        {"$group": {
            "_id": None,
            "total_amount": {"$sum": "$amount"},
            "count": {"$sum": 1}
        }}
    ]

    result = list(db.transactions.aggregate(pipeline))
    return result[0] if result else {"total_amount": 0, "count": 0}


# ============================================================
# ======================== REPORTING ==========================
# ============================================================

def monthly_totals_per_account(db, account_id: str, year: int, month: int):
    start = datetime(year, month, 1)
    end = datetime(year + (month == 12), (month % 12) + 1, 1)

    pipeline = [
        {"$match": {
            "account_id": account_id,
            "timestamp": {"$gte": start, "$lt": end}
        }},
        {"$group": {
            "_id": None,
            "total_amount": {"$sum": "$amount"},
            "num_transactions": {"$sum": 1}
        }}
    ]

    result = list(db.transactions.aggregate(pipeline))
    return result[0] if result else {
        "total_amount": 0,
        "num_transactions": 0
    }


def search_transactions_by_fields(db,
                                  min_amount: Optional[float] = None,
                                  merchant: Optional[str] = None,
                                  date: Optional[datetime] = None):

    query = {}
    if min_amount:
        query["amount"] = {"$gte": min_amount}
    if merchant:
        query["merchant"] = merchant
    if date:
        start = datetime(date.year, date.month, date.day)
        end = start.replace(day=start.day + 1)
        query["timestamp"] = {"$gte": start, "$lt": end}

    return list(db.transactions.find(query))
