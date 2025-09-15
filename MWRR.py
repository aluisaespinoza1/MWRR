import pandas as pd
import numpy as np
from datetime import datetime
import pyxirr

def parse_spanish_date(date_str):
    """
    Convierte fechas en español a datetime
    """
    if pd.isna(date_str):
        return pd.NaT
    
    spanish_months = {
        'enero': 'January', 'febrero': 'February', 'marzo': 'March',
        'abril': 'April', 'mayo': 'May', 'junio': 'June',
        'julio': 'July', 'agosto': 'August', 'septiembre': 'September',
        'octubre': 'October', 'noviembre': 'November', 'diciembre': 'December'
    }
    
    try:
        if isinstance(date_str, pd.Timestamp):
            return date_str
        
        date_str = str(date_str).strip()
        
        if '/' in date_str or '-' in date_str:
            return pd.to_datetime(date_str, dayfirst=True, errors='coerce')
        
        date_lower = date_str.lower()
        for esp, eng in spanish_months.items():
            if esp in date_lower:
                date_str = date_str.replace(esp, eng).replace(' de ', ' ')
                break
        
        return pd.to_datetime(date_str, errors='coerce')
        
    except:
        return pd.NaT

# Cargar datos del archivo Excel
print("Cargando datos del archivo Excel...")
excel_file = "data_actividad.xlsx"

try:
    data_movements = pd.read_excel(excel_file, sheet_name='movements')
    data_balances = pd.read_excel(excel_file, sheet_name='balances')
    
    print(f"Datos cargados - Movements: {len(data_movements)} registros")
    print(f"Datos cargados - Balances: {len(data_balances)} registros")
    
except FileNotFoundError:
    print("Error: No se encontró el archivo 'data_actividad.xlsx'")
    print("Asegúrate de que el archivo esté en el directorio actual.")
except Exception as e:
    print(f"Error cargando el archivo: {e}")

def clean_balance_data(df_balance):
    """
    Limpia y procesa los datos de balance para obtener el valor del portafolio por fecha y contrato
    """
    df_clean = df_balance.copy()
    
    # Convertir fechas
    df_clean['balance_date'] = df_clean['balance_date'].apply(parse_spanish_date)
    
    # Eliminar filas con fechas inválidas
    df_clean = df_clean.dropna(subset=['balance_date'])
    
    # Agrupar por contrato y fecha, sumando value_pos_mdo
    df_grouped = df_clean.groupby(['contract', 'balance_date'])['value_pos_mdo'].sum().reset_index()
    
    # Renombrar columnas
    df_grouped.rename(columns={
        'balance_date': 'Date',
        'value_pos_mdo': 'Portfolio_Value',
        'contract': 'Contract'
    }, inplace=True)
    
    # Ordenar por contrato y fecha
    df_grouped = df_grouped.sort_values(['Contract', 'Date']).reset_index(drop=True)
    
    return df_grouped

def clean_movements_data(df_movements):
    """
    Limpia y filtra los datos de movimientos para obtener solo depósitos y retiros
    """
    df_clean = df_movements.copy()
    
    # Filtrar depósitos y retiros
    deposits_withdrawals = df_clean[
        df_clean['description'].str.contains('Depósito|Retiro', case=False, na=False)
    ].copy()
    
    if len(deposits_withdrawals) == 0:
        return pd.DataFrame(columns=['Contract', 'Description', 'Movement_Import', 'Operation_Date'])
    
    # Convertir fechas
    deposits_withdrawals['operation_date'] = deposits_withdrawals['operation_date'].apply(parse_spanish_date)
    
    # Eliminar filas con fechas inválidas
    deposits_withdrawals = deposits_withdrawals.dropna(subset=['operation_date'])
    
    # Seleccionar columnas necesarias
    df_filtered = deposits_withdrawals[['contract', 'description', 'movement_import', 'operation_date']].copy()
    
    # Renombrar columnas
    df_filtered.rename(columns={
        'contract': 'Contract',
        'description': 'Description',
        'movement_import': 'Movement_Import',
        'operation_date': 'Operation_Date'
    }, inplace=True)
    
    # Ordenar por contrato y fecha
    df_filtered = df_filtered.sort_values(['Contract', 'Operation_Date']).reset_index(drop=True)
    
    return df_filtered

def MWRR(balance_data, movements_data, contract):
    """
    Calcula el Money Weighted Return Rate (MWRR) para un contrato específico
    
    Parameters:
    - balance_data: DataFrame con datos de balance limpio
    - movements_data: DataFrame con datos de movimientos limpio
    - contract: string con el número de contrato
    
    Returns:
    - float: MWRR anualizado
    """
    
    # Filtrar datos por contrato
    balance_contract = balance_data[balance_data['Contract'] == contract].copy()
    movements_contract = movements_data[movements_data['Contract'] == contract].copy()
    
    if balance_contract.empty:
        print(f"No hay datos de balance para el contrato {contract}")
        return None
        
    # Listas para fechas y flujos de efectivo
    cash_flow_dates = []
    cash_flows = []
    
    # Agregar movimientos
    for _, row in movements_contract.iterrows():
        cash_flow_dates.append(row['Operation_Date'])
        
        if 'Depósito' in row['Description']:
            cash_flows.append(row['Movement_Import'])
        else:  # Retiro
            cash_flows.append(-row['Movement_Import'])
    
    # Agregar valor inicial del portafolio
    if not balance_contract.empty:
        first_date = balance_contract['Date'].min()
        first_value = balance_contract[balance_contract['Date'] == first_date]['Portfolio_Value'].iloc[0]
        
        if first_date not in cash_flow_dates:
            cash_flow_dates.append(first_date)
            cash_flows.append(-first_value)
    
    # Agregar valor final del portafolio
    last_date = balance_contract['Date'].max()
    last_value = balance_contract[balance_contract['Date'] == last_date]['Portfolio_Value'].iloc[0]
    
    cash_flow_dates.append(last_date)
    cash_flows.append(last_value)
    
    # Verificar datos suficientes
    if len(cash_flows) < 2:
        print(f"Datos insuficientes para calcular MWRR del contrato {contract}")
        return None
    
    try:
        # Calcular MWRR usando pyxirr
        mwrr = pyxirr.xirr(cash_flow_dates, cash_flows)
        return mwrr
    except Exception as e:
        print(f"Error calculando MWRR para contrato {contract}: {e}")
        return None

def process_portfolio_analysis():
    """
    Procesa los datos y calcula los rendimientos de las tres carteras
    """
    
    print("\nProcesando datos...")
    
    # Limpiar los datos
    clean_balance = clean_balance_data(data_balances)
    clean_movements = clean_movements_data(data_movements)
    
    print(f"Balances procesados: {len(clean_balance)} filas")
    print(f"Movimientos procesados: {len(clean_movements)} filas")
    
    # Obtener contratos únicos
    contratos = clean_balance['Contract'].unique()
    print(f"Contratos encontrados: {list(contratos)}")
    
    # Calcular MWRR para cada contrato
    results = {}
    
    print("\nCalculando rendimientos MWRR...")
    print("-" * 50)
    
    for contract in contratos:
        mwrr = MWRR(clean_balance, clean_movements, contract)
        results[contract] = mwrr
        
        if mwrr is not None:
            print(f"Contrato {contract}: {mwrr*100:.2f}% anual")
        else:
            print(f"Contrato {contract}: Error en el cálculo")
    
    return results, clean_balance, clean_movements

# Ejecutar el análisis
if __name__ == "__main__":
    
    if 'data_movements' in locals() and 'data_balances' in locals():
        
        # Procesar y calcular rendimientos
        results, balance_data, movements_data = process_portfolio_analysis()
        
    