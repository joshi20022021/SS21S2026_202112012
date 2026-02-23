-- ============================================================
--  PRÁCTICA 1 – Modelo Multidimensional de Inteligencia de Negocios
--  Curso: Seminario de Sistemas 2  |  Carnet: 202112012
--  Fecha: 2026-02-22
-- ============================================================

USE master;
GO

-- Crear base de datos si no existe
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DW_Vuelos202112012')
BEGIN
    CREATE DATABASE DW_Vuelos202112012;
END
GO

USE DW_Vuelos202112012;
GO

-- ============================================================
--  LIMPIEZA PREVIA (orden inverso a FK)
-- ============================================================
IF OBJECT_ID('dbo.fact_vuelo',      'U') IS NOT NULL DROP TABLE dbo.fact_vuelo;
IF OBJECT_ID('dbo.dim_tipo_vuelo',  'U') IS NOT NULL DROP TABLE dbo.dim_tipo_vuelo;
IF OBJECT_ID('dbo.dim_pasajero',    'U') IS NOT NULL DROP TABLE dbo.dim_pasajero;
IF OBJECT_ID('dbo.dim_aeropuerto',  'U') IS NOT NULL DROP TABLE dbo.dim_aeropuerto;
IF OBJECT_ID('dbo.dim_aerolinea',   'U') IS NOT NULL DROP TABLE dbo.dim_aerolinea;
IF OBJECT_ID('dbo.dim_fecha',       'U') IS NOT NULL DROP TABLE dbo.dim_fecha;
GO

-- ============================================================
--  DIMENSIÓN FECHA
-- ============================================================
CREATE TABLE dbo.dim_fecha (
    id_fecha     INT          NOT NULL,   -- YYYYMMDD como surrogate key
    fecha        DATE         NOT NULL,
    anio         SMALLINT     NOT NULL,
    trimestre    TINYINT      NOT NULL,
    mes          TINYINT      NOT NULL,
    nombre_mes   VARCHAR(20)  NOT NULL,
    semana       TINYINT      NOT NULL,
    dia          TINYINT      NOT NULL,
    nombre_dia   VARCHAR(15)  NOT NULL,
    CONSTRAINT PK_dim_fecha PRIMARY KEY (id_fecha)
);
GO

-- ============================================================
--  DIMENSIÓN AEROLÍNEA
-- ============================================================
CREATE TABLE dbo.dim_aerolinea (
    id_aerolinea      INT          NOT NULL IDENTITY(1,1),
    codigo_aerolinea  CHAR(2)      NOT NULL,
    nombre_aerolinea  VARCHAR(100) NOT NULL,
    CONSTRAINT PK_dim_aerolinea PRIMARY KEY (id_aerolinea),
    CONSTRAINT UQ_dim_aerolinea_codigo UNIQUE (codigo_aerolinea)
);
GO

-- ============================================================
--  DIMENSIÓN AEROPUERTO
-- ============================================================
CREATE TABLE dbo.dim_aeropuerto (
    id_aeropuerto      INT         NOT NULL IDENTITY(1,1),
    codigo_aeropuerto  CHAR(3)     NOT NULL,
    CONSTRAINT PK_dim_aeropuerto PRIMARY KEY (id_aeropuerto),
    CONSTRAINT UQ_dim_aeropuerto_codigo UNIQUE (codigo_aeropuerto)
);
GO

-- ============================================================
--  DIMENSIÓN PASAJERO
-- ============================================================
CREATE TABLE dbo.dim_pasajero (
    id_pasajero   INT          NOT NULL IDENTITY(1,1),
    uuid_pasajero VARCHAR(36)  NOT NULL,
    genero        VARCHAR(20)  NOT NULL DEFAULT 'DESCONOCIDO',
    edad          TINYINT      NULL,
    nacionalidad  CHAR(2)      NOT NULL DEFAULT 'XX',
    CONSTRAINT PK_dim_pasajero PRIMARY KEY (id_pasajero),
    CONSTRAINT UQ_dim_pasajero_uuid UNIQUE (uuid_pasajero)
);
GO

-- ============================================================
--  DIMENSIÓN TIPO DE VUELO
-- ============================================================
CREATE TABLE dbo.dim_tipo_vuelo (
    id_tipo_vuelo   INT          NOT NULL IDENTITY(1,1),
    numero_vuelo    VARCHAR(10)  NOT NULL,
    tipo_aeronave   VARCHAR(10)  NULL,
    clase_cabina    VARCHAR(20)  NOT NULL,
    estado_vuelo    VARCHAR(20)  NOT NULL,
    canal_venta     VARCHAR(20)  NOT NULL DEFAULT 'DESCONOCIDO',
    metodo_pago     VARCHAR(20)  NOT NULL,
    moneda_original CHAR(3)      NOT NULL,
    CONSTRAINT PK_dim_tipo_vuelo PRIMARY KEY (id_tipo_vuelo)
);
GO

-- ============================================================
--  TABLA DE HECHOS
-- ============================================================
CREATE TABLE dbo.fact_vuelo (
    id_hecho             INT            NOT NULL IDENTITY(1,1),
    fk_fecha_salida      INT            NOT NULL,
    fk_aerolinea         INT            NOT NULL,
    fk_origen            INT            NOT NULL,
    fk_destino           INT            NOT NULL,
    fk_pasajero          INT            NOT NULL,
    fk_tipo_vuelo        INT            NOT NULL,
    -- Atributos degenerados
    record_id_origen     INT            NOT NULL,  -- clave natural del CSV
    asiento              VARCHAR(5)     NULL,
    -- Medidas
    precio_usd           DECIMAL(10,2)  NOT NULL DEFAULT 0,
    duracion_min         SMALLINT       NULL,
    retraso_min          SMALLINT       NOT NULL DEFAULT 0,
    n_maletas_total      TINYINT        NOT NULL DEFAULT 0,
    n_maletas_bodega     TINYINT        NOT NULL DEFAULT 0,
    -- FK constraints
    CONSTRAINT PK_fact_vuelo PRIMARY KEY (id_hecho),
    CONSTRAINT FK_fact_fecha      FOREIGN KEY (fk_fecha_salida) REFERENCES dbo.dim_fecha      (id_fecha),
    CONSTRAINT FK_fact_aerolinea  FOREIGN KEY (fk_aerolinea)    REFERENCES dbo.dim_aerolinea  (id_aerolinea),
    CONSTRAINT FK_fact_origen     FOREIGN KEY (fk_origen)        REFERENCES dbo.dim_aeropuerto (id_aeropuerto),
    CONSTRAINT FK_fact_destino    FOREIGN KEY (fk_destino)       REFERENCES dbo.dim_aeropuerto (id_aeropuerto),
    CONSTRAINT FK_fact_pasajero   FOREIGN KEY (fk_pasajero)      REFERENCES dbo.dim_pasajero   (id_pasajero),
    CONSTRAINT FK_fact_tipo_vuelo FOREIGN KEY (fk_tipo_vuelo)    REFERENCES dbo.dim_tipo_vuelo (id_tipo_vuelo)
);
GO

-- ============================================================
--  ÍNDICES DE RENDIMIENTO
-- ============================================================
CREATE NONCLUSTERED INDEX IX_fact_fecha
    ON dbo.fact_vuelo (fk_fecha_salida);
CREATE NONCLUSTERED INDEX IX_fact_aerolinea
    ON dbo.fact_vuelo (fk_aerolinea);
CREATE NONCLUSTERED INDEX IX_fact_origen
    ON dbo.fact_vuelo (fk_origen);
CREATE NONCLUSTERED INDEX IX_fact_destino
    ON dbo.fact_vuelo (fk_destino);
CREATE NONCLUSTERED INDEX IX_fact_pasajero
    ON dbo.fact_vuelo (fk_pasajero);
GO

PRINT 'Esquema DW_Vuelos202112012 creado exitosamente.';
GO
