def create_tables(session):
    """
    Crea las tablas en Cassandra
    Se asume que el KEYSPACE ya está creado y seleccionado.
    """

    print("Creando tablas en Cassandra...")

    queries = [

        # 1. HISTORIAL DE TRANSACCIONES (principal para detección de anomalías)
        """
        CREATE TABLE IF NOT EXISTS transaction_history (
            account_number text,
            timestamp timestamp,
            transaction_id uuid,
            amount decimal,
            currency text,
            merchant text,
            status text,
            raw_payload text,
            PRIMARY KEY ((account_number), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """,

        # 2. TOTALES POR DÍA (reporting y cálculos agregados)
        """
        CREATE TABLE IF NOT EXISTS daily_totals (
            account_number text,
            date date,
            total_amount decimal,
            transaction_count int,
            PRIMARY KEY ((account_number), date)
        );
        """,

        # 3. ALERTAS DE FRAUDE (excesos, ráfagas, etc.)
        """
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id uuid,
            timestamp timestamp,
            account_number text,
            transaction_id uuid,
            reason text,
            PRIMARY KEY (alert_id)
        );
        """
    ]

    for q in queries:
        try:
            session.execute(q)
        except Exception as e:
            print(f"Error creando tabla: {e}")

    print("Tablas de Cassandra creadas/verificadas exitosamente.")
