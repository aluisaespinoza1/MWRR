import pandas as pd
import numpy as np
from datetime import datetime
import pyxirr
import dateutil.parser as dp

def parse_date_safe(date_str):
    """
    Función auxiliar para parsear fechas en español usando el mismo patrón que dp.parse
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
        
        # Si es formato numérico, usar dateutil directamente
        if '/' in date_str or '-' in date_str:
            return dp.parse(date_str, dayfirst=True)
        
        # Si contiene texto en español, traducir
        date_lower = date_str.lower()
        for esp, eng in spanish_months.items():
            if esp in date_lower:
                date_str = date_str.replace(esp, eng).replace(' de ', ' ')
                break
        
        return dp.parse(date_str)
        
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

def clean_balance_data(data):
    cols = ['contract', 'value_pos_mdo', 'balance_date']
    df = data[cols]
    
    df['balance_date_2'] = df["balance_date"].apply(lambda x: parse_date_safe(x))
    
    clean_balance_data = (df.groupby(["contract", "balance_date_2"], as_index=False)["value_pos_mdo"]
                         .sum())
    
    return clean_balance_data

def clean_movements_data(data):
    # Definir patrones para flujos de efectivo
    depositos_patterns = [
        'DEPOSITO DE EFECTIVO',
        'DEPOSITO DE EFECTIVO POR TRANSFERENCIA', 
        'Compra en Reporto',
        'Compra Soc. de Inv.- Cliente'
    ]
    
    retiros_patterns = [
        'RETIRO DE EFECTIVO',
        'Venta Normal',
        'Venta Soc. de Inv.- Cliente',
        'Vencimiento de Reporto',
        'Amortización (cliente)',
        'Amortizacion (cliente)'
    ]
    
    all_patterns = depositos_patterns + retiros_patterns
    pattern = '|'.join(all_patterns)
    
    # Filtrar movimientos de efectivo
    cash_flows = data[data['description'].str.contains(pattern, case=False, na=False, regex=True)]
    
    cols = ['contract', 'description', 'movement_import', 'operation_date']
    df = cash_flows[cols]
    
    df['operation_date_2'] = df["operation_date"].apply(lambda x: parse_date_safe(x))
    
    # Clasificar tipo de flujo
    def classify_flow(description):
        desc_lower = description.lower()
        for pattern in depositos_patterns:
            if pattern.lower() in desc_lower:
                return 'Deposito'
        for pattern in retiros_patterns:
            if pattern.lower().replace('\\\\', '') in desc_lower:
                return 'Retiro'
        return 'Otro'
    
    df['flow_type'] = df['description'].apply(classify_flow)
    
    clean_movements_data = df.sort_values(['contract', 'operation_date_2']).reset_index(drop=True)
    
    return clean_movements_data

def MWRR(balance_data, movements_data, contract):
    """
    Calcula el Money Weighted Return Rate (MWRR) para un contrato específico
    
    Flujos de efectivo:
    - Positivos: Valor inicial, depósitos (dinero que entra al portafolio)
    - Negativos: Valor final, retiros (dinero que sale del portafolio)
    """
    
    # Filtrar datos por contrato
    balance_contract = balance_data[balance_data['contract'] == contract]
    movements_contract = movements_data[movements_data['contract'] == contract]
    
    if balance_contract.empty:
        print(f"No hay datos de balance para el contrato {contract}")
        return None
        
    # Listas para fechas y flujos de efectivo
    cash_flow_dates = []
    cash_flows = []
    
    # Agregar valor inicial del portafolio como flujo positivo
    if not balance_contract.empty:
        first_date = balance_contract['balance_date_2'].min()
        first_value = balance_contract[balance_contract['balance_date_2'] == first_date]['value_pos_mdo'].iloc[0]
        
        cash_flow_dates.append(first_date)
        cash_flows.append(first_value)
    
    # Agregar movimientos de efectivo
    for _, row in movements_contract.iterrows():
        cash_flow_dates.append(row['operation_date_2'])
        
        if row['flow_type'] == 'Deposito':
            cash_flows.append(row['movement_import'])
        elif row['flow_type'] == 'Retiro':
            cash_flows.append(-row['movement_import'])
    
    # Agregar valor final del portafolio como flujo negativo
    last_date = balance_contract['balance_date_2'].max()
    last_value = balance_contract[balance_contract['balance_date_2'] == last_date]['value_pos_mdo'].iloc[0]
    
    cash_flow_dates.append(last_date)
    cash_flows.append(-last_value)
    
    # Verificar datos suficientes
    if len(cash_flows) < 2:
        print(f"Datos insuficientes para calcular MWRR del contrato {contract}")
        return None
    
    # Mostrar información de debug
    print(f"\nContrato {contract} - Flujos de efectivo:")
    for i, (date, flow) in enumerate(zip(cash_flow_dates, cash_flows)):
        flow_type = "Entrada" if flow > 0 else "Salida"
        print(f"  {date.strftime('%Y-%m-%d')}: ${flow:,.2f} ({flow_type})")
    
    try:
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
    contratos = clean_balance['contract'].unique()
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
        
        # Mostrar resumen final
        print("RESUMEN FINAL - RENDIMIENTOS MWRR")
    
        total_contratos = len(results)
        exitosos = sum(1 for v in results.values() if v is not None)
        
        for contract, mwrr in results.items():
            if mwrr is not None:
                print(f"Contrato {contract}: {mwrr*100:.4f}% anual")
            else:
                print(f"Contrato {contract}: No se pudo calcular")
        
        print(f"\nProcesados: {exitosos}/{total_contratos} contratos")
        print("Análisis completado")
                
    else:
        print("Error: No se pudieron cargar los datos del archivo Excel.")