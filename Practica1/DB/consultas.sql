-- Validar el número total de registros cargados en la tabla de hechos
SELECT COUNT(*) AS Total_Registros_Cargados 
FROM Fact_Vuelos;

-- Distribución del número de vuelos según su estado
SELECT 
    status AS Estado_Vuelo, 
    COUNT(id_hecho) AS Numero_de_Vuelos
FROM Fact_Vuelos
GROUP BY status
ORDER BY Numero_de_Vuelos DESC;

-- Top 5 destinos con mayor cantidad de vuelos
SELECT TOP 5
    da.airport_code AS Aeropuerto_Destino,
    COUNT(fv.id_hecho) AS Cantidad_Vuelos
FROM Fact_Vuelos fv
INNER JOIN Dim_Aeropuerto da ON fv.id_aeropuerto_destino = da.id_aeropuerto
GROUP BY da.airport_code
ORDER BY Cantidad_Vuelos DESC;

-- Distribución de vuelos comprados clasificados por género del pasajero
SELECT
    dp.Genero,
    COUNT(fv.id_hecho) AS Total_Vuelos
FROM Fact_Vuelos fv
INNER JOIN Dim_Pasajero dp ON fv.id_pasajero = dp.id_pasajero
GROUP BY dp.Genero
ORDER BY Total_Vuelos DESC;

-- Ingresos totales (en USD) y precio promedio del boleto por aerolínea
SELECT
    da.Nombre_Aerolinea,
    SUM(fv.ticket_price_usd_est) AS Ingresos_Totales_USD,
    AVG(fv.ticket_price_usd_est) AS Precio_Promedio_Boleto_USD
FROM Fact_Vuelos fv
INNER JOIN Dim_Aerolinea da ON fv.id_aerolinea = da.id_aerolinea
GROUP BY da.Nombre_Aerolinea
ORDER BY Ingresos_Totales_USD DESC;

-- Top 3 de Rutas Más Rentables POR CADA Aerolínea
WITH RentabilidadRutas AS (
    SELECT 
        da.Nombre_Aerolinea,
        orig.airport_code AS Origen,
        dest.airport_code AS Destino,
        SUM(fv.ticket_price_usd_est) AS Ingresos_Ruta_USD,
        COUNT(fv.id_hecho) AS Total_Vuelos
    FROM Fact_Vuelos fv
    INNER JOIN Dim_Aerolinea da ON fv.id_aerolinea = da.id_aerolinea
    INNER JOIN Dim_Aeropuerto orig ON fv.id_aeropuerto_origen = orig.id_aeropuerto
    INNER JOIN Dim_Aeropuerto dest ON fv.id_aeropuerto_destino = dest.id_aeropuerto
    WHERE fv.status != 'CANCELLED'
    GROUP BY da.Nombre_Aerolinea, orig.airport_code, dest.airport_code
),
RankingRutas AS (
    SELECT 
        Nombre_Aerolinea,
        Origen,
        Destino,
        Ingresos_Ruta_USD,
        Total_Vuelos,
        ROW_NUMBER() OVER(PARTITION BY Nombre_Aerolinea ORDER BY Ingresos_Ruta_USD DESC) AS Ranking_Interno
    FROM RentabilidadRutas
)
SELECT * FROM RankingRutas 
WHERE Ranking_Interno <= 3
ORDER BY Nombre_Aerolinea, Ranking_Interno;