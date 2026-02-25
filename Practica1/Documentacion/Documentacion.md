# Universidad de San Carlos de Guatemala
# Facultad de Ingenieria
# Escuela de Ciencias y Sistemas

## Nombre: Edgar Josías Cán Ajquejay
## Carnet: 202112012

# Documentacion - Practica 1

## Diseño del Modelo Multidimensional (Esquema en Estrella)

Para la implementación del Data Warehouse de la presente práctica, se utilizó la metodología de modelado dimensional propuesta por Ralph Kimball. El objetivo principal fue transformar los datos transaccionales (planos) en un **Esquema en Estrella** altamente optimizado para consultas analíticas y generación de indicadores clave de rendimiento (KPIs).

El proceso de diseño se dividió en las siguientes fases estratégicas:

### 1. Definición de la Tabla de Hechos (`Fact_Vuelos`)
Se identificó que el proceso de negocio central a medir es la **operación y venta de vuelos**. Por lo tanto, se construyó la tabla `Fact_Vuelos` en el centro del esquema, la cual almacena la granularidad transaccional del modelo.
* **Métricas Cuantitativas:** Se depuraron las columnas para almacenar únicamente los datos medibles y sumables que aportan valor al negocio. Entre ellas destacan: el precio estandarizado (`ticket_price_usd_est`), tiempo de vuelo (`duration_min`), retrasos (`delay_min`) y conteo de equipaje (`bags_total`, `bags_checked`).
* **Dimensiones Degeneradas:** Atributos como el número de vuelo (`flight_number`) y el estado operacional (`status`) se mantuvieron directamente en la tabla de hechos al no requerir tablas de atributos adicionales, optimizando el espacio de almacenamiento.

### 2. Identificación y Creación de Dimensiones
Para proveer contexto a las métricas (el *quién, cómo y dónde*), se abstrajeron los datos descriptivos hacia tablas de dimensiones satelitales. 
* Se generaron las dimensiones: `Dim_Pasajero`, `Dim_Aerolinea`, `Dim_Cabina` y `Dim_Canal`.
* **Implementación de Llaves Sustitutas (Surrogate Keys):** En lugar de utilizar las cadenas de texto (strings) originales como identificadores, se generaron llaves primarias numéricas auto-incrementales (`INT IDENTITY`) para cada dimensión. Esto garantiza un cruce de información (JOINs) considerablemente más rápido, reduce el tamaño de almacenamiento de la tabla de hechos e independiza el modelo de los cambios operativos en los sistemas origen.

### 3. Implementación de Dimensiones de "Juego de Roles" (Role-Playing Dimensions)
Uno de los retos técnicos más importantes del diseño fue el manejo de atributos que comparten la misma naturaleza pero distinto contexto dentro de un mismo evento (como el aeropuerto de origen/destino y las múltiples fechas de un vuelo). Para evitar la redundancia de datos, se aplicó el patrón de **Role-Playing Dimensions**:
* **Roles Geográficos:** Se creó una única tabla `Dim_Aeropuerto` con el catálogo maestro de códigos. La tabla de hechos se conecta a esta dimensión a través de dos llaves foráneas distintas: `id_aeropuerto_origen` e `id_aeropuerto_destino`.
* **Roles Temporales:** Se generó una sola dimensión calendario (`Dim_Tiempo`) que descompone las fechas en Año, Trimestre, Mes y Día. Posteriormente, la tabla de hechos hace referencia a esta misma dimensión en tres momentos distintos del ciclo de vida del vuelo: `id_fecha_salida`, `id_fecha_llegada` e `id_fecha_reserva`.

## Documentación del Proceso ETL (Extracción, Transformación y Carga)

El proceso ETL fue desarrollado en **Python**, utilizando la librería **Pandas** para la manipulación de datos en memoria y **SQLAlchemy** junto con `pyodbc` para la orquestación y conexión con el motor de base de datos SQL Server (alojado en un contenedor de Docker). 

El flujo de trabajo se divide en las siguientes tres fases principales:

### Fase 1: Extracción (Extract)
La extracción de los datos se realiza desde una fuente plana. Se lee el archivo origen `dataset_vuelos_crudo(1).csv` utilizando la función `pd.read_csv()` de Pandas, cargando la totalidad de los registros transaccionales en un DataFrame (estructura de datos tabular en memoria) para su posterior procesamiento.

### Fase 2: Transformación (Transform)
Esta es la fase crítica de limpieza y estandarización, diseñada para garantizar la calidad del dato (Data Quality) antes de ingresarlo al Data Warehouse. Se aplicaron las siguientes reglas de negocio:

* **Manejo de Nulos Ocultos:** Se implementó una expresión regular (`r'^\s*$'`) para identificar celdas que visualmente estaban vacías o contenían espacios en blanco, convirtiéndolas en verdaderos valores nulos (`NaN`) para que el sistema pudiera procesarlas.
* **Estandarización de Texto:** Los códigos de los aeropuertos (origen y destino) y los géneros se limpiaron de espacios residuales y se forzaron a letras mayúsculas (`.str.upper()`). Los nombres de las aerolíneas se formatearon a *Title Case* (Letra inicial mayúscula).
* **Conversión de Tipos de Datos (Casting):** Los valores monetarios (`ticket_price` y `ticket_price_usd_est`) que venían con formato europeo (comas en lugar de puntos decimales) fueron limpiados y casteados al tipo numérico `float`.
* **Estandarización Temporal:** Las columnas de fechas presentaban formatos mixtos (ej. sistemas de 12h y 24h). Se utilizó `pd.to_datetime(format='mixed')` para unificar todas las fechas bajo el estándar internacional `YYYY-MM-DD HH:MM:SS`.
* **Imputación de Datos Faltantes:** * Los valores nulos en columnas categóricas (`passenger_nationality`, `sales_channel`, `seat`) se rellenaron con la etiqueta descriptiva `'Unknown'`.
  * Las edades faltantes (`passenger_age`) se sustituyeron por la **mediana** de las edades existentes para no sesgar la distribución estadística.
  * Los minutos de duración y retraso faltantes se imputaron con un valor numérico de `0`.
* **Auditoría:** Como paso final de esta fase, se generó un archivo de auditoría (`dataset_vuelos_transformado.xlsx`) para verificar la calidad de la limpieza antes de la inserción en base de datos.

### Fase 3: Carga (Load)
La carga hacia la base de datos `Vuelos_DW` en SQL Server se ejecutó respetando la integridad referencial del modelo de Esquema en Estrella, dividida en dos etapas secuenciales:

1. **Carga de Dimensiones (Catálogos):**
   Se extrajeron los valores únicos (`drop_duplicates()`) del DataFrame maestro para cada entidad (Aeropuertos, Aerolíneas, Pasajeros, Canales y Cabinas). Se renombraron las columnas para coincidir exactamente con la estructura de la base de datos y se insertaron mediante `.to_sql()`. Destaca la `Dim_Tiempo`, la cual fue descompuesta algorítmicamente en atributos de Año, Trimestre, Mes, Día y Hora directamente desde Python. En este paso, SQL Server generó automáticamente las *Surrogate Keys* (IDs numéricos) para cada registro.

2. **Carga de la Tabla de Hechos (`Fact_Vuelos`):**
   Dado que el Data Warehouse utiliza llaves foráneas numéricas en lugar de textos, se realizó un mapeo de datos:
   * Se consultaron los catálogos recién creados desde SQL Server hacia la memoria de Python (`pd.read_sql`).
   * Se utilizaron cruces lógicos (`pd.merge`) equivalentes a *LEFT JOINs* para reemplazar los textos del CSV original por los IDs numéricos generados por la base de datos.
   * Finalmente, se filtró el DataFrame para contener exclusivamente llaves foráneas y métricas cuantitativas, insertando el lote completo en la tabla `Fact_Vuelos`.
