import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib

# CONFIGURACIÓN DE CONEXIÓN
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;" 
    "DATABASE=Vuelos_DW;"
    "UID=sa;"
    "PWD=AdminVuelos2026!;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# EXTRACCIÓN Y TRANSFORMACIÓN
print("Iniciando Extracción y Transformación...")

df_vuelos = pd.read_csv('C:\\Users\\edgar\\Downloads\\SA_FASE2\\SEMINARIOXD\\SS21S2026_202112012\\Practica1\\ETL\\dataset_vuelos_crudo(1).csv')

# Tratar espacios vacíos
df_vuelos = df_vuelos.replace(r'^\s*$', np.nan, regex=True)

# Aeropuertos a mayúsculas
df_vuelos['origin_airport'] = df_vuelos['origin_airport'].str.strip().str.upper()
df_vuelos['destination_airport'] = df_vuelos['destination_airport'].str.strip().str.upper()

# Precios a Float
df_vuelos['ticket_price'] = df_vuelos['ticket_price'].astype(str).str.replace(',', '.').astype(float)
df_vuelos['ticket_price_usd_est'] = df_vuelos['ticket_price_usd_est'].astype(str).str.replace(',', '.').astype(float)

# Fechas
columnas_fecha = ['departure_datetime', 'arrival_datetime', 'booking_datetime']
for col in columnas_fecha:
    df_vuelos[col] = pd.to_datetime(df_vuelos[col], errors='coerce', format='mixed')
    df_vuelos[col] = df_vuelos[col].dt.strftime('%Y-%m-%d %H:%M:%S')

# Géneros y Textos
df_vuelos['passenger_gender'] = df_vuelos['passenger_gender'].astype(str).str.strip().str.upper()
df_vuelos['passenger_gender'] = df_vuelos['passenger_gender'].replace({'MASCULINO': 'M', 'FEMENINO': 'F'})
df_vuelos['airline_name'] = df_vuelos['airline_name'].astype(str).str.title()

# Nulos y valores por defecto
df_vuelos['passenger_nationality'] = df_vuelos['passenger_nationality'].fillna('Unknown')
df_vuelos['sales_channel'] = df_vuelos['sales_channel'].fillna('Unknown')
df_vuelos['passenger_age'] = df_vuelos['passenger_age'].fillna(df_vuelos['passenger_age'].median())
df_vuelos['duration_min'] = df_vuelos['duration_min'].fillna(0)
df_vuelos['delay_min'] = df_vuelos['delay_min'].fillna(0)
df_vuelos['seat'] = df_vuelos['seat'].fillna('Unknown')

# Generar Excel de revisión
ruta_salida_excel = 'C:\\Users\\edgar\\Downloads\\SA_FASE2\\SEMINARIOXD\\SS21S2026_202112012\\Practica1\\ETL\\dataset_vuelos_transformado.xlsx'
df_vuelos.to_excel(ruta_salida_excel, index=False, na_rep='NaN')
print(f"¡Excel generado en: {ruta_salida_excel}!")

# CARGA DE DIMENSIONES
try:
    print("\n--- Iniciando Carga de Dimensiones ---")
    
    # Aeropuertos
    aeropuertos = pd.concat([df_vuelos['origin_airport'], df_vuelos['destination_airport']]).dropna().unique()
    pd.DataFrame({'airport_code': aeropuertos}).to_sql('Dim_Aeropuerto', con=engine, if_exists='append', index=False)

    # Aerolinea
    df_dim_aerolinea = df_vuelos[['airline_code', 'airline_name']].dropna().drop_duplicates()
    df_dim_aerolinea.rename(columns={'airline_name': 'Nombre_Aerolinea'}, inplace=True)
    df_dim_aerolinea.to_sql('Dim_Aerolinea', con=engine, if_exists='append', index=False)

    # Pasajero
    df_dim_pasajero = df_vuelos[['passenger_id', 'passenger_gender', 'passenger_age', 'passenger_nationality']].dropna().drop_duplicates()
    df_dim_pasajero.rename(columns={'passenger_gender': 'Genero', 'passenger_age': 'Edad', 'passenger_nationality': 'Nacionalidad'}, inplace=True)
    df_dim_pasajero.to_sql('Dim_Pasajero', con=engine, if_exists='append', index=False)

    # Canal
    df_dim_canal = df_vuelos[['sales_channel', 'payment_method']].dropna().drop_duplicates()
    df_dim_canal.rename(columns={'sales_channel': 'Canal_Venta', 'payment_method': 'Metodo_Pago'}, inplace=True)
    df_dim_canal.to_sql('Dim_Canal', con=engine, if_exists='append', index=False)

    # Cabina
    df_dim_cabina = df_vuelos[['cabin_class', 'aircraft_type', 'seat']].dropna().drop_duplicates()
    df_dim_cabina.rename(columns={'cabin_class': 'Clase', 'aircraft_type': 'Aeronave', 'seat': 'Asiento'}, inplace=True)
    df_dim_cabina.to_sql('Dim_Cabina', con=engine, if_exists='append', index=False)

    # Tiempo
    fechas = pd.concat([df_vuelos['departure_datetime'], df_vuelos['arrival_datetime'], df_vuelos['booking_datetime']]).dropna().unique()
    df_tiempo_temp = pd.DataFrame({'fecha_completa': pd.to_datetime(fechas)})
    pd.DataFrame({
        'Fecha': df_tiempo_temp['fecha_completa'].dt.date,
        'Anio': df_tiempo_temp['fecha_completa'].dt.year,
        'Trimestre': df_tiempo_temp['fecha_completa'].dt.quarter,
        'Mes': df_tiempo_temp['fecha_completa'].dt.month,
        'Dia': df_tiempo_temp['fecha_completa'].dt.day,
        'Hora': df_tiempo_temp['fecha_completa'].dt.strftime('%H:%M:%S')
    }).drop_duplicates().to_sql('Dim_Tiempo', con=engine, if_exists='append', index=False)

    print("Dimensiones cargadas")

except Exception as e:
    print(f"Error en la carga de dimensiones: {e}")

# CARGA DE TABLA DE HECHOS (FACT_VUELOS)
try:
    print("\n--- Iniciando Carga de Hechos ---")
    dim_aeropuerto = pd.read_sql("SELECT id_aeropuerto, airport_code FROM Dim_Aeropuerto", engine)
    dim_aerolinea = pd.read_sql("SELECT id_aerolinea, airline_code FROM Dim_Aerolinea", engine)
    dim_pasajero = pd.read_sql("SELECT id_pasajero, passenger_id FROM Dim_Pasajero", engine)
    dim_canal = pd.read_sql("SELECT id_canal, Canal_Venta, Metodo_Pago FROM Dim_Canal", engine)
    dim_cabina = pd.read_sql("SELECT id_cabina, Clase, Aeronave, Asiento FROM Dim_Cabina", engine)
    dim_tiempo = pd.read_sql("SELECT id_tiempo, CONCAT(CAST(Fecha AS VARCHAR), ' ', Hora) as fecha_completa FROM Dim_Tiempo", engine)

    fact_df = df_vuelos.copy()

    # Cruces
    fact_df = fact_df.merge(dim_aerolinea, on='airline_code', how='left')
    fact_df = fact_df.merge(dim_pasajero, on='passenger_id', how='left')
    fact_df = fact_df.merge(dim_aeropuerto.rename(columns={'id_aeropuerto': 'id_aeropuerto_origen', 'airport_code': 'origin_airport'}), on='origin_airport', how='left')
    fact_df = fact_df.merge(dim_aeropuerto.rename(columns={'id_aeropuerto': 'id_aeropuerto_destino', 'airport_code': 'destination_airport'}), on='destination_airport', how='left')
    fact_df = fact_df.merge(dim_canal.rename(columns={'Canal_Venta': 'sales_channel', 'Metodo_Pago': 'payment_method'}), on=['sales_channel', 'payment_method'], how='left')
    fact_df = fact_df.merge(dim_cabina.rename(columns={'Clase': 'cabin_class', 'Aeronave': 'aircraft_type', 'Asiento': 'seat'}), on=['cabin_class', 'aircraft_type', 'seat'], how='left')
    fact_df = fact_df.merge(dim_tiempo.rename(columns={'id_tiempo': 'id_fecha_salida', 'fecha_completa': 'departure_datetime'}), on='departure_datetime', how='left')
    fact_df = fact_df.merge(dim_tiempo.rename(columns={'id_tiempo': 'id_fecha_llegada', 'fecha_completa': 'arrival_datetime'}), on='arrival_datetime', how='left')
    fact_df = fact_df.merge(dim_tiempo.rename(columns={'id_tiempo': 'id_fecha_reserva', 'fecha_completa': 'booking_datetime'}), on='booking_datetime', how='left')

    columnas_finales = [
        'id_aerolinea', 'id_aeropuerto_origen', 'id_aeropuerto_destino', 
        'id_pasajero', 'id_cabina', 'id_canal', 
        'id_fecha_salida', 'id_fecha_llegada', 'id_fecha_reserva',
        'flight_number', 'status', 'ticket_price_usd_est', 
        'duration_min', 'delay_min', 'bags_total', 'bags_checked'
    ]
    
    fact_df_final = fact_df[columnas_finales]

    print("Insertando datos en Fact_Vuelos...")
    fact_df_final.to_sql('Fact_Vuelos', con=engine, if_exists='append', index=False)

    print("datos insertados") 
    print("proceso ETL finalizado")

except Exception as e:
    print(f"Error en la carga de la tabla de hechos: {e}")