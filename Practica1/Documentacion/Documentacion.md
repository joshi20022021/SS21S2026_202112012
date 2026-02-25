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

### Conclusión del Diseño
El resultado final es un modelo lógico y físico robusto, escalable y libre de anomalías de actualización. La arquitectura dimensional lograda asegura que el motor de SQL Server pueda resolver agregaciones complejas en tiempos de respuesta mínimos, estableciendo una base sólida para la Inteligencia de Negocios y la futura creación de cubos OLAP.