import pandas as pd
from pyxirr import xirr
import dateparser as dp

data = "data_actividad.xlsx"

movements = pd.read_excel(data, sheet_name="movements")
balances = pd.read_excel(data, sheet_name="balances")


# 1. Función limpieza balances
def clean_balance_data(data):
    cols = ['contract','value_pos_mdo','balance_date']
    df = data[cols].copy()

    # Parse balance_date into datetime
    df['balance_date_2'] = df["balance_date"].apply(lambda x: dp.parse(x))

    # Dictionary to store grouped dfs
    contract_dfs = {}
    for contract, group in df.groupby("contract"):
        grouped = group.groupby("balance_date_2", as_index=False)["value_pos_mdo"].sum()
        contract_dfs[contract] = grouped.reset_index(drop=True)

        # Create a global variable for each contract
        globals()[f"balance_{contract}"] = contract_dfs[contract]

    # Collect names of created variables
    created_vars = [f"balance_{contract}" for contract in contract_dfs.keys()]

    return contract_dfs, created_vars


# 2. Función limpieza movimientos
def clean_movements_data(df_mov):
    """
    Clasifica los movimientos como Depósitos o Retiros.
    Devuelve: contrato, description, movement_import, operation_date
    """
    df = df_mov.copy()

    # Diccionario de clasificación
    clasificacion = {
        "DEPOSITO DE EFECTIVO": "DEPOSITO DE EFECTIVO",
        "DEPOSITO DE EFECTIVO POR TRANSFERENCIA": "DEPOSITO DE EFECTIVO",
        "Compra en Reporto": "DEPOSITO DE EFECTIVO",
        "Compra Soc. de Inv.- Cliente": "DEPOSITO DE EFECTIVO",
        "RETIRO DE EFECTIVO": "RETIRO DE EFECTIVO",
        "Venta Normal": "RETIRO DE EFECTIVO",
        "Venta Soc. de Inv.- Cliente": "RETIRO DE EFECTIVO",
        "Vencimiento de Reporto": "RETIRO DE EFECTIVO",
        "Amortización (cliente)": "RETIRO DE EFECTIVO",
    }

    # Clasificar
    df["description_clean"] = df["description"].map(clasificacion)
    df = df[df["description_clean"].notna()]

    # Convertir fechas
    df["operation_date"] = df["operation_date"].apply(
        lambda x: dp.parse(str(x)) if pd.notnull(x) else pd.NaT
    )

    # Selección de columnas finales
    df_clean = df[
        ["contract", "description_clean", "movement_import", "operation_date"]
    ].rename(columns={"description_clean": "description"})

    return df_clean


# 3. Función para añadir valor inicial y final desde balances
def add_initial_final(mov_df, bal_df):
    """
    Añade filas de VALOR INICIAL y VALOR FINAL a la tabla de movimientos.
    """
    contratos = bal_df["contract"].unique()
    extra_rows = []

    for c in contratos:
        # Valor inicial
        fecha_ini = bal_df[bal_df["contract"] == c]["balance_date"].min()
        valor_ini = bal_df.loc[
            (bal_df["contract"] == c) & (bal_df["balance_date"] == fecha_ini),
            "portfolio_value",
        ].values[0]

        extra_rows.append([c, "VALOR INICIAL", valor_ini, fecha_ini])

        # Valor final
        fecha_fin = bal_df[bal_df["contract"] == c]["balance_date"].max()
        valor_fin = bal_df.loc[
            (bal_df["contract"] == c) & (bal_df["balance_date"] == fecha_fin),
            "portfolio_value",
        ].values[0]

        extra_rows.append([c, "VALOR FINAL", valor_fin, fecha_fin])

    df_extra = pd.DataFrame(
        extra_rows,
        columns=["contract", "description", "movement_import", "operation_date"],
    )

    # Unir movimientos + inicial/final
    df_final = pd.concat([mov_df, df_extra], ignore_index=True).sort_values(
        by=["contract", "operation_date"]
    )

    return df_final


# 4. Función para calcular MWRR
def MWRR(mov_df, contract):
    """
    Calcula el Money Weighted Rate of Return (MWRR) de un contrato.
    Recibe: tabla de movimientos con VALOR INICIAL y VALOR FINAL.
    """

    # Filtrar movimientos del contrato
    mov = mov_df[mov_df["contract"] == contract].copy()

    # Construir listas de fechas y flujos
    fechas = list(mov["operation_date"])
    flujos = []

    for desc, imp in zip(mov["description"], mov["movement_import"]):
        if desc in ["VALOR INICIAL", "DEPOSITO DE EFECTIVO"]:
            flujos.append(-imp)  # aportaciones = negativo
        elif desc in ["RETIRO DE EFECTIVO", "VALOR FINAL"]:
            flujos.append(imp)   # retiros y valor final = positivo
        else:
            flujos.append(imp)   # fallback

    # Calcular rendimiento con XIRR
    return xirr(fechas, flujos)


# Resultados
if __name__ == "__main__":
    bal_clean = clean_balance_data(balances)
    mov_clean = clean_movements_data(movements)
    mov_with_bounds = add_initial_final(mov_clean, bal_clean)

    contracts = ["20486403", "12861603", "AHA84901"]
    for c in contracts:
        rendimiento = MWRR(mov_with_bounds, c)
        print(f"Rendimiento cliente {c}: {rendimiento*100:.2f}%")

