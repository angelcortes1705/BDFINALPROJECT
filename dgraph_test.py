import os
import pydgraph

import Dgraph.dgraph_setup as dgraph_setup
import Dgraph.dgraph_queries as dgraph_queries

DGRAPH_URI = os.getenv("DGRAPH_URI", "localhost:9080")


# ===============================
# Menú
# ===============================
def print_menu():
    options = {
        1: "Crear esquema",
        2: "Cargar datos iniciales",
        3: "RF1 – Grafo completo de un cliente",
        4: "RF2 – Transacciones fraudulentas por merchant",
        5: "RF3 – Cuentas de un cliente",
        6: "RF4 – Transacciones de una cuenta",
        7: "RF5 – Casos creados por un analista",
        8: "RF6 – Detalles de un caso",
        9: "RF7 – Transacciones fraudulentas por rango de fecha",
        10: "RF8 – Evidencia de un caso",
        11: "RF9 – Clientes que comparten cuentas",
        12: "RF10 – Grafo de un merchant",
        0: "Salir",
    }

    print("\n=========== MENÚ DGRAPH ===========")
    for k, v in options.items():
        print(k, "--", v)
    print("===================================\n")


# ===============================
# Conexión
# ===============================
def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def create_client(stub):
    return pydgraph.DgraphClient(stub)

def close_client_stub(stub):
    stub.close()


# ===============================
# Main loop
# ===============================
def main():
    stub = create_client_stub()
    client = create_client(stub)

    while True:
        print_menu()

        try:
            option = int(input("Seleccione una opción: "))
        except ValueError:
            print("Opción inválida.\n")
            continue

        # ============================
        #   OPCIONES
        # ============================
        if option == 1:
            print("Creando esquema...")
            dgraph_setup.create_schema(client)
            print("Esquema creado.\n")

        elif option == 2:
            print("Cargando datos iniciales tienes una linea comentada y no va a hacer nada...")
            #data_loader.load_data(client)
            print("Datos cargados.\n")

        elif option == 3:
            curp = input("Ingrese el CURP del cliente: ")
            result = dgraph_queries.get_client_graph(client, curp)
            print(result)

        elif option == 4:
            merchant = input("Ingrese el merchant: ")
            result = dgraph_queries.get_suspicious_transactions_by_merchant(client, merchant)
            print(result)

        elif option == 5:
            curp = input("Ingrese el CURP del cliente: ")
            result = dgraph_queries.get_accounts_by_client(client, curp)
            print(result)

        elif option == 6:
            acc = input("Ingrese el número de cuenta: ")
            result = dgraph_queries.get_transactions_by_account(client, acc)
            print(result)

        elif option == 7:
            username = input("Ingrese el username del analista: ")
            result = dgraph_queries.get_cases_by_user(client, username)
            print(result)

        elif option == 8:
            case_uid = input("Ingrese el UID del caso: ")
            result = dgraph_queries.get_investigation_case(client, case_uid)
            print(result)

        elif option == 9:
            start = input("Fecha inicio (YYYY-MM-DD): ")
            end = input("Fecha fin (YYYY-MM-DD): ")
            start += "T00:00:00Z"
            end += "T23:59:59Z"
            result = dgraph_queries.get_fraudulent_by_date_range(client, start, end)
            print(result)

        elif option == 10:
            case_uid = input("Ingrese el UID del caso: ")
            result = dgraph_queries.get_evidence_for_case(client, case_uid)
            print(result)

        elif option == 11:
            curp = input("Ingrese el CURP del cliente: ")
            result = dgraph_queries.get_clients_sharing_accounts(client, curp)
            print(result)

        elif option == 12:
            merchant = input("Ingrese el merchant: ")
            result = dgraph_queries.get_merchant_graph(client, merchant)
            print(result)

        elif option == 0:
            close_client_stub(stub)
            print("Conexión cerrada.")
            return

        else:
            print("Opción no válida.\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error:", e)
