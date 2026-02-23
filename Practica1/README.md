# Práctica 1 – ETL y Modelo Multidimensional de Vuelos

**Curso:** Seminario de Sistemas 2  
**Carnet:** 202112012  
**Fecha:** 2026-02-22  

---

## 1. Descripción del Problema

El objetivo de esta práctica es diseñar e implementar un proceso **ETL (Extracción, Transformación y Carga)** sobre un dataset de registros de vuelos. Los datos se encuentran en formato CSV y presentan múltiples problemas de calidad. El resultado final es un **Data Warehouse** en SQL Server con un modelo multidimensional que permite ejecutar consultas analíticas de inteligencia de negocios.

---

## 2. Estructura del Proyecto

```
Practica1/
├── dataset_vuelos_crudo(1).csv   # Fuente de datos original (10,000 registros)
├── schema_ddl.sql                # DDL: creación de la BD y tablas dimensionales
├── etl_vuelos.py                 # Script ETL principal (Python)
├── consultas_analiticas.sql      # Consultas SQL analíticas
└── README.md                     # Este documento
```

---

## 3. Problemas de Calidad de Datos Identificados

| Campo | Problema detectado | Corrección aplicada |
|---|---|---|
| `passenger_gender` | Variantes: M, m, Masculino, F, Femenino, X, NoBinario | Estandarizado a M / F / X / DESCONOCIDO |
| `departure_datetime` | Mezcla de formatos: `dd/MM/yyyy HH:mm`, `MM-dd-yyyy hh:mm AM/PM` | Parseo con múltiples formatos |
| `booking_datetime` | Mismo problema que departure | Parseo con múltiples formatos |
| `destination_airport` | Códigos en minúscula (jfk, sjo) | Conversión a mayúsculas |
| `airline_name` | Casing inconsistente (RYANAIR / Ryanair) | Normalización con `.title()` |
| `ticket_price` | Números con coma decimal ("77,60") | Sustitución de `,` por `.` |
| `delay_min` | Vacío en vuelos CANCELLED | Relleno con 0 |
| `sales_channel` | Registros vacíos | Relleno con DESCONOCIDO |
| `passenger_id` | UUIDs duplicados en algunas filas | `drop_duplicates` en la dimensión |

---

## 4. Modelo Multidimensional

El modelo implementado es un **esquema estrella** con una tabla de hechos y cinco dimensiones:

```
                    ┌──────────────┐
                    │  dim_fecha   │
                    │──────────────│
                    │ id_fecha (PK)│
                    │ fecha        │
                    │ anio         │
                    │ trimestre    │
                    │ mes          │
                    │ nombre_mes   │
                    │ semana       │
                    │ dia          │
                    │ nombre_dia   │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌───────┴──────────────────────────┐   ┌──────────────────┐
│ dim_aerolinea│   │           fact_vuelo              │   │  dim_aeropuerto  │
│──────────────│   │──────────────────────────────────│   │──────────────────│
│ id_aerolinea │◄──│ id_hecho (PK)                    │──►│ id_aeropuerto    │
│ codigo       │   │ fk_fecha_salida                   │   │ codigo_aeropuerto│
│ nombre       │   │ fk_aerolinea                      │   └──────────────────┘
└──────────────┘   │ fk_origen                         │
                   │ fk_destino                         │
┌──────────────┐   │ fk_pasajero                       │   ┌──────────────────┐
│ dim_pasajero │   │ fk_tipo_vuelo                     │   │  dim_tipo_vuelo  │
│──────────────│   │ record_id_origen                  │   │──────────────────│
│ id_pasajero  │◄──│ asiento                           │──►│ id_tipo_vuelo    │
│ uuid_pasajero│   │ precio_usd  (medida)              │   │ numero_vuelo     │
│ genero       │   │ duracion_min (medida)             │   │ tipo_aeronave    │
│ edad         │   │ retraso_min  (medida)             │   │ clase_cabina     │
│ nacionalidad │   │ n_maletas_total (medida)          │   │ estado_vuelo     │
└──────────────┘   │ n_maletas_bodega (medida)         │   │ canal_venta      │
                   └───────────────────────────────────┘   │ metodo_pago      │
                                                            │ moneda_original  │
                                                            └──────────────────┘
```

### Tabla de hechos: `fact_vuelo`

| Columna | Tipo | Descripción |
|---|---|---|
| `id_hecho` | INT IDENTITY | Surrogate key |
| `fk_fecha_salida` | INT → dim_fecha | Fecha de salida (YYYYMMDD) |
| `fk_aerolinea` | INT → dim_aerolinea | Aerolínea operadora |
| `fk_origen` | INT → dim_aeropuerto | Aeropuerto de origen |
| `fk_destino` | INT → dim_aeropuerto | Aeropuerto de destino |
| `fk_pasajero` | INT → dim_pasajero | Pasajero |
| `fk_tipo_vuelo` | INT → dim_tipo_vuelo | Características del vuelo |
| `record_id_origen` | INT | Clave natural del CSV |
| `asiento` | VARCHAR(5) | Número de asiento |
| `precio_usd` | DECIMAL(10,2) | Precio en USD |
| `duracion_min` | SMALLINT | Duración del vuelo en minutos |
| `retraso_min` | SMALLINT | Retraso en minutos |
| `n_maletas_total` | TINYINT | Total de maletas |
| `n_maletas_bodega` | TINYINT | Maletas en bodega |

---

## 5. Proceso ETL

### 5.1 Fase 1 – Extracción

- Lectura del archivo `dataset_vuelos_crudo(1).csv` (10,000 registros, 26 columnas).
- Carga en DataFrame de Pandas con todos los campos como `string` para evitar conversiones automáticas incorrectas.

### 5.2 Fase 2 – Transformación

1. **Limpieza de strings:** eliminación de espacios en todos los campos.
2. **Estandarización de categóricos:** géneros, estados, canales, métodos de pago.
3. **Parseo de fechas:** hasta 7 formatos de fecha soportados mediante prueba secuencial.
4. **Normalización numérica:** precios con coma decimal, enteros nulos.
5. **Descarte de registros:** se eliminan filas sin fecha de salida (requisito de la dimensión tiempo).
6. **Construcción de dimensiones:** eliminación de duplicados y renombrado de columnas.

### 5.3 Fase 3 – Carga

1. Se eliminan datos previos en todas las tablas (modo truncate-and-reload).
2. Carga de dimensiones vía `pandas.to_sql`.
3. Lectura de surrogate keys desde la BD para resolver las FK del hecho.
4. Unión (merge) de los datos de staging con las dimensiones.
5. Carga del hecho con `chunksize=1000` (rendimiento).

---

## 6. Requisitos de Instalación

```bash
pip install pandas pyodbc sqlalchemy
```

**Driver ODBC requerido:** `ODBC Driver 17 for SQL Server`  
Descarga: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

---

## 7. Pasos de Ejecución

### Paso 1 – Crear el esquema en SQL Server

```sql
-- Ejecutar en SQL Server Management Studio (SSMS) o Azure Data Studio
-- Archivo: schema_ddl.sql
```

### Paso 2 – Configurar la conexión en el ETL

Editar las primeras líneas de `etl_vuelos.py`:

```python
DB_SERVER   = "localhost"          # Nombre o IP del servidor
DB_NAME     = "DW_Vuelos202112012"
DB_DRIVER   = "ODBC Driver 17 for SQL Server"
DB_TRUSTED  = True                 # True = Windows Auth
```

### Paso 3 – Ejecutar el ETL

```bash
python etl_vuelos.py
```

El proceso genera un archivo `etl_log.txt` con el detalle de cada fase.

### Paso 4 – Ejecutar consultas analíticas

```sql
-- Ejecutar en SSMS o Azure Data Studio
-- Archivo: consultas_analiticas.sql
```

---

## 8. Resumen de Consultas Analíticas

| # | Consulta | Propósito |
|---|---|---|
| 1.1 | Conteo por tabla | Validación de carga |
| 1.2 | FK nulas en hechos | Integridad referencial |
| 2.1 | Vuelos por estado | Distribución ON_TIME / DELAYED / CANCELLED |
| 2.2 | Top 10 rutas | Rutas más frecuentes |
| 2.3 | Top 5 aerolíneas | Volumen y retraso promedio |
| 2.4 | Vuelos por mes/año | Serie temporal |
| 2.5 | Vuelos por cabina | Distribución por clase |
| 3.1 | Vuelos por género | Distribución de pasajeros |
| 3.2 | Top 10 nacionalidades | Análisis demográfico |
| 3.3 | Rangos de edad | Segmentación etaria |
| 4.1 | Ingresos por canal | Análisis comercial |
| 4.2 | Método de pago | Preferencias de pago |
| 4.3 | Aeropuertos origen | Top salidas |
| 4.4 | Aeropuertos destino | Top llegadas |
| 5.1 | Ingresos por aerolínea/año | OLAP temporal |
| 5.2 | Ranking aerolíneas | RANK() por ingresos |
| 5.3 | Maletas por aerolínea | Análisis operativo |
| 5.4 | Retraso por aerolínea | Rendimiento puntualidad |

---

## 9. Tecnologías Utilizadas

| Tecnología | Versión | Uso |
|---|---|---|
| Python | 3.10+ | ETL y transformación |
| pandas | 2.x | Manipulación de datos |
| pyodbc | 4.x | Conectividad ODBC |
| SQLAlchemy | 2.x | ORM y carga a BD |
| SQL Server | 2019+ | Data Warehouse |
