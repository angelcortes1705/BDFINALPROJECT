import pydgraph

def create_schema(client):
    schema = """
    
    # ===========================
    #  BASIC ATTRIBUTES
    # ===========================

    client_name: string @index(term) .
    client_email: string @index(hash) .
    curp: string @index(hash) .

    account_number: string @index(hash) .
    account_type: string @index(exact) .
    balance: float .

    amount: float .
    currency: string .
    merchant: string @index(term) .
    timestamp: datetime @index(hour) .
    status: string @index(exact) . # normal / flagged / fraudulent

    case_status: string @index(exact) . # open / in_progress / closed
    case_title: string @index(term) .
    created_at: datetime .
    note: string .
    evidence_type: string .
    evidence_url: string .

    # ===========================
    #  RELATIONSHIPS (UID)
    # ===========================

    owns_account: uid @reverse .
    has_transaction: uid @reverse .
    includes_transaction: uid @reverse .
    created_by: uid .
    has_evidence: uid @reverse .

    # ===========================
    #  TYPES
    # ===========================

    type Client {
        client_name
        client_email
        curp
        owns_account
    }

    type Account {
        account_number
        account_type
        balance
        has_transaction
    }

    type Transaction {
        amount
        currency
        merchant
        timestamp
        status
    }

    type InvestigationCase {
        case_title
        case_status
        created_at
        created_by
        includes_transaction
        has_evidence
    }

    type Evidence {
        note
        evidence_type
        evidence_url
    }

    type User {
        username
        email
    }
    """

    client.alter(pydgraph.Operation(schema=schema))
