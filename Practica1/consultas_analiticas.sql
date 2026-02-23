-- ============================================================
--  PRÁCTICA 1 – Consultas Analíticas sobre el DW de Vuelos
--  Curso: Seminario de Sistemas 2  |  Carnet: 202112012
--  Fecha: 2026-02-22
-- ============================================================

USE DW_Vuelos202112012;
GO

-- ============================================================
--  SECCIÓN 1: VALIDACIÓN DE CARGA
-- ============================================================

-- 1.1 Total de registros por tabla
SELECT 'fact_vuelo'      AS tabla, COUNT(*) AS total FROM dbo.fact_vuelo
UNION ALL
SELECT 'dim_fecha',               COUNT(*) FROM dbo.dim_fecha
UNION ALL
SELECT 'dim_aerolinea',           COUNT(*) FROM dbo.dim_aerolinea
UNION ALL
SELECT 'dim_aeropuerto',          COUNT(*) FROM dbo.dim_aeropuerto
UNION ALL
SELECT 'dim_pasajero',            COUNT(*) FROM dbo.dim_pasajero
UNION ALL
SELECT 'dim_tipo_vuelo',          COUNT(*) FROM dbo.dim_tipo_vuelo;
GO

-- 1.2 Verificación de integridad referencial (FK nulas)
SELECT
    COUNT(CASE WHEN fk_fecha_salida IS NULL THEN 1 END) AS sin_fecha,
    COUNT(CASE WHEN fk_aerolinea    IS NULL THEN 1 END) AS sin_aerolinea,
    COUNT(CASE WHEN fk_origen       IS NULL THEN 1 END) AS sin_origen,
    COUNT(CASE WHEN fk_destino      IS NULL THEN 1 END) AS sin_destino,
    COUNT(CASE WHEN fk_pasajero     IS NULL THEN 1 END) AS sin_pasajero,
    COUNT(CASE WHEN fk_tipo_vuelo   IS NULL THEN 1 END) AS sin_tipo_vuelo
FROM dbo.fact_vuelo;
GO

-- ============================================================
--  SECCIÓN 2: ANÁLISIS DE VUELOS
-- ============================================================

-- 2.1 Total de vuelos por estado
SELECT
    tv.estado_vuelo,
    COUNT(*)                     AS total_vuelos,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS porcentaje
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_tipo_vuelo  tv ON fv.fk_tipo_vuelo = tv.id_tipo_vuelo
GROUP BY tv.estado_vuelo
ORDER BY total_vuelos DESC;
GO

-- 2.2 Top 10 rutas (origen → destino) más frecuentes
SELECT TOP 10
    ao.codigo_aeropuerto AS origen,
    ad.codigo_aeropuerto AS destino,
    COUNT(*)             AS total_vuelos,
    CAST(AVG(fv.precio_usd) AS DECIMAL(10,2)) AS precio_promedio_usd
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_aeropuerto  ao ON fv.fk_origen  = ao.id_aeropuerto
JOIN dbo.dim_aeropuerto  ad ON fv.fk_destino = ad.id_aeropuerto
GROUP BY ao.codigo_aeropuerto, ad.codigo_aeropuerto
ORDER BY total_vuelos DESC;
GO

-- 2.3 Top 5 aerolíneas con más vuelos y retraso promedio
SELECT TOP 5
    al.nombre_aerolinea,
    COUNT(*)                                   AS total_vuelos,
    CAST(AVG(CAST(fv.retraso_min AS FLOAT))
         AS DECIMAL(8,1))                      AS retraso_promedio_min,
    CAST(AVG(fv.precio_usd) AS DECIMAL(10,2))  AS precio_promedio_usd
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_aerolinea   al ON fv.fk_aerolinea = al.id_aerolinea
GROUP BY al.nombre_aerolinea
ORDER BY total_vuelos DESC;
GO

-- 2.4 Vuelos por mes y año
SELECT
    f.anio,
    f.mes,
    f.nombre_mes,
    COUNT(*)                                   AS total_vuelos,
    CAST(SUM(fv.precio_usd)  AS DECIMAL(14,2)) AS ingresos_usd,
    CAST(AVG(fv.precio_usd)  AS DECIMAL(10,2)) AS precio_promedio_usd
FROM dbo.fact_vuelo  fv
JOIN dbo.dim_fecha   f  ON fv.fk_fecha_salida = f.id_fecha
GROUP BY f.anio, f.mes, f.nombre_mes
ORDER BY f.anio, f.mes;
GO

-- 2.5 Distribución de vuelos por clase de cabina
SELECT
    tv.clase_cabina,
    COUNT(*)                     AS total_vuelos,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS porcentaje,
    CAST(AVG(fv.precio_usd) AS DECIMAL(10,2))     AS precio_promedio_usd
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_tipo_vuelo  tv ON fv.fk_tipo_vuelo = tv.id_tipo_vuelo
GROUP BY tv.clase_cabina
ORDER BY total_vuelos DESC;
GO

-- ============================================================
--  SECCIÓN 3: ANÁLISIS DE PASAJEROS
-- ============================================================

-- 3.1 Distribución de vuelos por género
SELECT
    p.genero,
    COUNT(*)                     AS total_vuelos,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS porcentaje,
    CAST(AVG(fv.precio_usd) AS DECIMAL(10,2))     AS precio_promedio_usd
FROM dbo.fact_vuelo   fv
JOIN dbo.dim_pasajero p  ON fv.fk_pasajero = p.id_pasajero
GROUP BY p.genero
ORDER BY total_vuelos DESC;
GO

-- 3.2 Top 10 nacionalidades con más vuelos
SELECT TOP 10
    p.nacionalidad,
    COUNT(*)                                   AS total_vuelos,
    CAST(AVG(fv.precio_usd) AS DECIMAL(10,2))  AS precio_promedio_usd
FROM dbo.fact_vuelo   fv
JOIN dbo.dim_pasajero p  ON fv.fk_pasajero = p.id_pasajero
WHERE p.nacionalidad <> 'XX'
GROUP BY p.nacionalidad
ORDER BY total_vuelos DESC;
GO

-- 3.3 Distribución de edades de pasajeros (rangos)
SELECT
    CASE
        WHEN p.edad < 18              THEN 'Menor de 18'
        WHEN p.edad BETWEEN 18 AND 30 THEN '18-30'
        WHEN p.edad BETWEEN 31 AND 45 THEN '31-45'
        WHEN p.edad BETWEEN 46 AND 60 THEN '46-60'
        ELSE 'Mayor de 60'
    END AS rango_edad,
    COUNT(*)                     AS total_vuelos,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS porcentaje
FROM dbo.fact_vuelo   fv
JOIN dbo.dim_pasajero p  ON fv.fk_pasajero = p.id_pasajero
WHERE p.edad IS NOT NULL
GROUP BY
    CASE
        WHEN p.edad < 18              THEN 'Menor de 18'
        WHEN p.edad BETWEEN 18 AND 30 THEN '18-30'
        WHEN p.edad BETWEEN 31 AND 45 THEN '31-45'
        WHEN p.edad BETWEEN 46 AND 60 THEN '46-60'
        ELSE 'Mayor de 60'
    END
ORDER BY total_vuelos DESC;
GO

-- ============================================================
--  SECCIÓN 4: ANÁLISIS COMERCIAL
-- ============================================================

-- 4.1 Ingresos totales y promedio por canal de venta
SELECT
    tv.canal_venta,
    COUNT(*)                                   AS total_vuelos,
    CAST(SUM(fv.precio_usd)  AS DECIMAL(14,2)) AS ingresos_usd,
    CAST(AVG(fv.precio_usd)  AS DECIMAL(10,2)) AS precio_promedio_usd
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_tipo_vuelo  tv ON fv.fk_tipo_vuelo = tv.id_tipo_vuelo
GROUP BY tv.canal_venta
ORDER BY ingresos_usd DESC;
GO

-- 4.2 Método de pago más utilizado
SELECT
    tv.metodo_pago,
    COUNT(*)                     AS total_vuelos,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS porcentaje
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_tipo_vuelo  tv ON fv.fk_tipo_vuelo = tv.id_tipo_vuelo
GROUP BY tv.metodo_pago
ORDER BY total_vuelos DESC;
GO

-- 4.3 Aeropuertos de origen más frecuentes (Top 10)
SELECT TOP 10
    ao.codigo_aeropuerto AS origen,
    COUNT(*)             AS total_vuelos_salida
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_aeropuerto  ao ON fv.fk_origen = ao.id_aeropuerto
GROUP BY ao.codigo_aeropuerto
ORDER BY total_vuelos_salida DESC;
GO

-- 4.4 Aeropuertos de destino más frecuentes (Top 10)
SELECT TOP 10
    ad.codigo_aeropuerto AS destino,
    COUNT(*)             AS total_vuelos_llegada
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_aeropuerto  ad ON fv.fk_destino = ad.id_aeropuerto
GROUP BY ad.codigo_aeropuerto
ORDER BY total_vuelos_llegada DESC;
GO

-- ============================================================
--  SECCIÓN 5: ANÁLISIS MULTIDIMENSIONAL (OLAP)
-- ============================================================

-- 5.1 Ingresos por aerolínea y año (PIVOT temporal)
SELECT
    al.nombre_aerolinea,
    f.anio,
    COUNT(*)                                   AS total_vuelos,
    CAST(SUM(fv.precio_usd) AS DECIMAL(14,2))  AS ingresos_usd
FROM dbo.fact_vuelo     fv
JOIN dbo.dim_aerolinea  al ON fv.fk_aerolinea   = al.id_aerolinea
JOIN dbo.dim_fecha      f  ON fv.fk_fecha_salida = f.id_fecha
GROUP BY al.nombre_aerolinea, f.anio
ORDER BY al.nombre_aerolinea, f.anio;
GO

-- 5.2 Ranking de aerolíneas por ingresos (con RANK)
SELECT
    al.nombre_aerolinea,
    COUNT(*)                                                    AS total_vuelos,
    CAST(SUM(fv.precio_usd) AS DECIMAL(14,2))                   AS ingresos_usd,
    RANK() OVER (ORDER BY SUM(fv.precio_usd) DESC)              AS ranking_ingresos
FROM dbo.fact_vuelo     fv
JOIN dbo.dim_aerolinea  al ON fv.fk_aerolinea = al.id_aerolinea
GROUP BY al.nombre_aerolinea
ORDER BY ranking_ingresos;
GO

-- 5.3 Total de maletas facturadas por aerolínea
SELECT
    al.nombre_aerolinea,
    SUM(fv.n_maletas_total)   AS maletas_totales,
    SUM(fv.n_maletas_bodega)  AS maletas_bodega,
    COUNT(*)                  AS total_vuelos
FROM dbo.fact_vuelo     fv
JOIN dbo.dim_aerolinea  al ON fv.fk_aerolinea = al.id_aerolinea
GROUP BY al.nombre_aerolinea
ORDER BY maletas_totales DESC;
GO

-- 5.4 Retraso promedio por aerolínea y estado
SELECT
    al.nombre_aerolinea,
    tv.estado_vuelo,
    COUNT(*)                                       AS total_vuelos,
    CAST(AVG(CAST(fv.retraso_min AS FLOAT))
         AS DECIMAL(8,1))                          AS retraso_promedio_min,
    MAX(fv.retraso_min)                            AS retraso_maximo_min
FROM dbo.fact_vuelo      fv
JOIN dbo.dim_aerolinea   al ON fv.fk_aerolinea  = al.id_aerolinea
JOIN dbo.dim_tipo_vuelo  tv ON fv.fk_tipo_vuelo = tv.id_tipo_vuelo
WHERE tv.estado_vuelo IN ('DELAYED', 'DIVERTED')
GROUP BY al.nombre_aerolinea, tv.estado_vuelo
ORDER BY retraso_promedio_min DESC;
GO
