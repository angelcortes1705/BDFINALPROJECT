import pydgraph


# ============================================================
# RF1 – Obtener el grafo completo de un cliente
# Cliente -> Cuentas -> Transacciones
# ============================================================

def get_client_graph(client, curp):
    query = f"""
    {{
        client(func: eq(curp, "{curp}")) {{
            uid
            client_name
            client_email

            owns_account {{
                uid
                account_number
                account_type
                balance

                has_transaction {{
                    uid
                    amount
                    currency
                    merchant
                    timestamp
                    status
                }}
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF2 – Obtener transacciones sospechosas por merchant
# ============================================================

def get_suspicious_transactions_by_merchant(client, merchant):
    query = f"""
    {{
        suspicious(func: eq(merchant, "{merchant}")) @filter(eq(status, "fraudulent")) {{
            uid
            amount
            currency
            timestamp
            merchant

            ~has_transaction {{
                account_number

                ~owns_account {{
                    client_name
                    curp
                }}
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF3 – Consultar las cuentas de un cliente
# ============================================================

def get_accounts_by_client(client, curp):
    query = f"""
    {{
        client(func: eq(curp, "{curp}")) {{
            client_name
            accounts: owns_account {{
                account_number
                account_type
                balance
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF4 – Consultar transacciones de una cuenta
# ============================================================

def get_transactions_by_account(client, account_number):
    query = f"""
    {{
        account(func: eq(account_number, "{account_number}")) {{
            account_number
            has_transaction {{
                uid
                amount
                currency
                timestamp
                merchant
                status
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF5 – Ver casos de investigación creados por un analista
# ============================================================

def get_cases_by_user(client, username):
    query = f"""
    {{
        cases(func: eq(username, "{username}")) {{
            uid
            created_cases: ~created_by {{
                uid
                case_title
                case_status
                created_at
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF6 – Obtener detalles de un caso + transacciones asociadas
# ============================================================

def get_investigation_case(client, case_uid):
    query = f"""
    {{
        case(func: uid({case_uid})) {{
            uid
            case_title
            case_status
            created_at

            includes_transaction {{
                uid
                amount
                timestamp
                merchant
                status
            }}

            has_evidence {{
                uid
                note
                evidence_type
                evidence_url
            }}

            created_by {{
                username
                email
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF7 – Buscar transacciones fraudulentas por rango de fecha
# ============================================================

def get_fraudulent_by_date_range(client, start, end):
    query = f"""
    {{
        fraudulent(func: type(Transaction)) 
        @filter(eq(status, "fraudulent") AND ge(timestamp, "{start}") AND le(timestamp, "{end}")) {{
            uid
            amount
            merchant
            timestamp
            currency

            ~has_transaction {{
                account_number
                ~owns_account {{
                    client_name
                    curp
                }}
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF8 – Obtener evidencia de un caso
# ============================================================

def get_evidence_for_case(client, case_uid):
    query = f"""
    {{
        case(func: uid({case_uid})) {{
            has_evidence {{
                uid
                note
                evidence_type
                evidence_url
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF9 – Obtener clientes conectados por cuentas compartidas
# (posible fraude por identidad compartida)
# ============================================================

def get_clients_sharing_accounts(client, curp):
    query = f"""
    {{
        var(func: eq(curp, "{curp}")) {{
            accs as owns_account
        }}

        related(func: uid(accs)) {{
            ~owns_account @filter(NOT eq(curp, "{curp}")) {{
                uid
                client_name
                curp
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json


# ============================================================
# RF10 – Obtener el grafo de un merchant
# merchant → transacciones → cuentas → clientes
# ============================================================

def get_merchant_graph(client, merchant):
    query = f"""
    {{
        merchant(func: eq(merchant, "{merchant}")) @filter(type(Transaction)) {{
            uid
            amount
            timestamp
            status

            ~has_transaction {{
                account_number

                ~owns_account {{
                    client_name
                    curp
                }}
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return res.json
