-- CREACIÓN DE LA BASE DE DATOS
CREATE DATABASE Vuelos_DW;
GO

USE Vuelos_DW;
GO

-- CREACIÓN DE TABLAS DE DIMENSIONES


CREATE TABLE Dim_Tiempo (
    id_tiempo INT PRIMARY KEY IDENTITY(1,1),
    Fecha DATE,
    Anio INT,
    Trimestre INT,
    Mes INT,
    Dia INT,
    Hora VARCHAR(10)
);

CREATE TABLE Dim_Aeropuerto (
    id_aeropuerto INT PRIMARY KEY IDENTITY(1,1),
    airport_code VARCHAR(10)
);

CREATE TABLE Dim_Pasajero (
    id_pasajero INT PRIMARY KEY IDENTITY(1,1),
    passenger_id VARCHAR(50),
    Genero VARCHAR(10),
    Edad INT,
    Nacionalidad VARCHAR(50)
);

CREATE TABLE Dim_Aerolinea (
    id_aerolinea INT PRIMARY KEY IDENTITY(1,1),
    airline_code VARCHAR(10),
    Nombre_Aerolinea VARCHAR(100)
);

CREATE TABLE Dim_Cabina (
    id_cabina INT PRIMARY KEY IDENTITY(1,1),
    Clase VARCHAR(50),
    Aeronave VARCHAR(50),
    Asiento VARCHAR(10)
);

CREATE TABLE Dim_Canal (
    id_canal INT PRIMARY KEY IDENTITY(1,1),
    Canal_Venta VARCHAR(50),
    Metodo_Pago VARCHAR(50)
);


--  CREACIÓN DE LA TABLA DE HECHOS


CREATE TABLE Fact_Vuelos (
    id_hecho INT PRIMARY KEY IDENTITY(1,1),
    
    -- Llaves Foráneas
    id_aerolinea INT FOREIGN KEY REFERENCES Dim_Aerolinea(id_aerolinea),
    id_pasajero INT FOREIGN KEY REFERENCES Dim_Pasajero(id_pasajero),
    id_cabina INT FOREIGN KEY REFERENCES Dim_Cabina(id_cabina),
    id_canal INT FOREIGN KEY REFERENCES Dim_Canal(id_canal),
    
    -- Llaves Foráneas de aeropuertos
    id_aeropuerto_origen INT FOREIGN KEY REFERENCES Dim_Aeropuerto(id_aeropuerto),
    id_aeropuerto_destino INT FOREIGN KEY REFERENCES Dim_Aeropuerto(id_aeropuerto),
    
    -- Llaves Foráneas de fechas
    id_fecha_salida INT FOREIGN KEY REFERENCES Dim_Tiempo(id_tiempo),
    id_fecha_llegada INT FOREIGN KEY REFERENCES Dim_Tiempo(id_tiempo),
    id_fecha_reserva INT FOREIGN KEY REFERENCES Dim_Tiempo(id_tiempo),
    
    -- Dimensiones Degeneradas
    flight_number VARCHAR(20),
    status VARCHAR(20),
    
    -- Métricas (Datos Numéricos)
    ticket_price_usd_est FLOAT,
    duration_min FLOAT,
    delay_min FLOAT,
    bags_total INT,
    bags_checked INT
);
GO