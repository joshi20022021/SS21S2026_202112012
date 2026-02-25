import pandas as pd

df_vuelos= pd.read_csv('C:\\Users\\edgar\\Downloads\\SA_FASE2\\SEMINARIOXD\\SS21S2026_202112012\\Practica1\\ETL\\dataset_vuelos_crudo(1).csv')

print("transformacion")

# convertir a mayúsculas los nombres de los aeropuertos
df_vuelos['origin_airport'] = df_vuelos['origin_airport'].str.upper()
df_vuelos['destination_airport'] = df_vuelos['destination_airport'].str.upper()

# reemplazo de comas por puntos en el precio y realizar la conversión a float
df_vuelos['ticket_price'] = df_vuelos['ticket_price'].str.replace(',', '.').astype(float)

# Estandarizar el formato de fecha a YYYY-MM-DD
columnas_fecha = ['departure_datetime', 'arrival_datetime', 'booking_datetime']
for col in columnas_fecha:
    df_vuelos[col] = pd.to_datetime(df_vuelos[col], errors='coerce', format='mixed')                  

# validar pasajeros sin nacionalidad y canal de venta
df_vuelos['passenger_nacionality'] = df_vuelos['passenger_nationality'].fillna('Unknown')
df_vuelos['sales_channel'] = df_vuelos['sales_channel'].fillna('Unknown')

# mediana de la edad
mediana_edad = df_vuelos['passenger_age'].median()
df_vuelos['passenger_age'] = df_vuelos['passenger_age'].fillna(mediana_edad)

# duracion y asientos
df_vuelos['duration_min'] = df_vuelos['duration_min'].fillna(0)
df_vuelos['delay_min'] = df_vuelos['delay_min'].fillna(0)
df_vuelos['seat'] = df_vuelos['seat'].fillna('Unknown')

print(df_vuelos.info())
print(df_vuelos.head())