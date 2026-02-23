"""
============================================================
PRÁCTICA 1 – ETL: Extracción, Transformación y Carga
Curso : Seminario de Sistemas 2
Carnet: 202112012
Fecha : 2026-02-22
============================================================

Dependencias:
    pip install pandas pyodbc sqlalchemy

Uso:
    python etl_vuelos.py

Variables de conexión a SQL Server configurables al inicio del script.
============================================================
"""

import os
import re
import logging
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

warnings.filterwarnings("ignore")

# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================
LOG_FILE = Path(__file__).parent / "etl_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN DE CONEXIÓN – AJUSTAR SEGÚN ENTORNO
# ============================================================
DB_SERVER   = "localhost"          # Nombre o IP del servidor SQL Server
DB_NAME     = "DW_Vuelos202112012"
DB_DRIVER   = "ODBC Driver 17 for SQL Server"
DB_TRUSTED  = True                 # True = Windows Auth; False = usuario/contraseña
DB_USER     = "sa"                 # Ignorado si DB_TRUSTED=True
DB_PASSWORD = "YourPassword123"    # Ignorado si DB_TRUSTED=True

# ============================================================
# RUTA AL ARCHIVO FUENTE
# ============================================================
BASE_DIR = Path(__file__).parent
CSV_PATH = BASE_DIR / "dataset_vuelos_crudo(1).csv"


# ============================================================
# UTILIDADES
# ============================================================

def build_engine():
    """Construye el engine de SQLAlchemy para SQL Server."""
    if DB_TRUSTED:
        conn_str = (
            f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}"
            f"?driver={DB_DRIVER.replace(' ', '+')}&trusted_connection=yes"
        )
    else:
        conn_str = (
            f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
            f"?driver={DB_DRIVER.replace(' ', '+')}"
        )
    return create_engine(conn_str, fast_executemany=True)


def normalizar_genero(valor: str) -> str:
    """Estandariza los distintos formatos de género a M / F / X / DESCONOCIDO."""
    v = str(valor).strip().upper()
    mapeo = {
        "M": "M", "MASCULINO": "M",
        "F": "F", "FEMENINO": "F",
        "X": "X", "NOBINARIO": "X",
    }
    return mapeo.get(v, "DESCONOCIDO")


_DATE_FORMATS = [
    "%d/%m/%Y %H:%M",
    "%m-%d-%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%d/%m/%Y",
    "%m-%d-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]


def parse_fecha(valor) -> datetime | None:
    """Intenta parsear una fecha con múltiples formatos posibles."""
    if pd.isna(valor) or str(valor).strip() == "":
        return None
    s = str(valor).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    log.warning("Fecha no reconocida: %s", s)
    return None


def normalizar_precio(valor) -> float:
    """Convierte cadenas de precio con coma decimal o vacías a float."""
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
    s = str(valor).strip().replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def fecha_a_id(dt: datetime | None) -> int | None:
    """Convierte datetime a clave entega YYYYMMDD."""
    if dt is None:
        return None
    return int(dt.strftime("%Y%m%d"))


# ============================================================
# FASE 1 – EXTRACCIÓN
# ============================================================

def extraer(csv_path: Path) -> pd.DataFrame:
    log.info("=== FASE 1: EXTRACCIÓN ===")
    log.info("Leyendo archivo: %s", csv_path)
    df = pd.read_csv(csv_path, dtype=str, na_values=["", "NA", "NULL", "null"])
    log.info("Registros leídos: %d | Columnas: %d", len(df), len(df.columns))
    return df


# ============================================================
# FASE 2 – TRANSFORMACIÓN
# ============================================================

def transformar(df_raw: pd.DataFrame) -> dict[str, pd.DataFrame]:
    log.info("=== FASE 2: TRANSFORMACIÓN ===")

    df = df_raw.copy()

    # ── 2.1 Limpieza básica de strings ──────────────────────
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # ── 2.2 Estandarización de campos categóricos ───────────
    df["airline_code"]        = df["airline_code"].str.upper()
    df["airline_name"]        = df["airline_name"].str.title()
    df["origin_airport"]      = df["origin_airport"].str.upper()
    df["destination_airport"] = df["destination_airport"].str.upper()
    df["passenger_gender"]    = df["passenger_gender"].apply(normalizar_genero)
    df["cabin_class"]         = df["cabin_class"].str.upper().fillna("DESCONOCIDO")
    df["status"]              = df["status"].str.upper().fillna("DESCONOCIDO")
    df["sales_channel"]       = df["sales_channel"].str.upper().fillna("DESCONOCIDO")
    df["payment_method"]      = df["payment_method"].str.upper().fillna("DESCONOCIDO")
    df["currency"]            = df["currency"].str.upper().fillna("USD")
    df["passenger_nationality"] = df["passenger_nationality"].str.upper().fillna("XX")

    # ── 2.3 Parseo de fechas ─────────────────────────────────
    df["departure_dt"] = df["departure_datetime"].apply(parse_fecha)
    df["booking_dt"]   = df["booking_datetime"].apply(parse_fecha)

    # Filtrar registros sin fecha de salida (sin llave de tiempo no se puede cargar)
    antes = len(df)
    df = df.dropna(subset=["departure_dt"])
    rechazados = antes - len(df)
    if rechazados:
        log.warning("Registros descartados por fecha_salida nula: %d", rechazados)

    df["fecha_salida_id"] = df["departure_dt"].apply(fecha_a_id)

    # ── 2.4 Normalización numérica ───────────────────────────
    df["ticket_price_usd_est"] = df["ticket_price_usd_est"].apply(normalizar_precio)
    df["ticket_price"]         = df["ticket_price"].apply(normalizar_precio)
    df["duration_min"]  = pd.to_numeric(df["duration_min"],  errors="coerce")
    df["delay_min"]     = pd.to_numeric(df["delay_min"],     errors="coerce").fillna(0).astype(int)
    df["bags_total"]    = pd.to_numeric(df["bags_total"],    errors="coerce").fillna(0).astype(int)
    df["bags_checked"]  = pd.to_numeric(df["bags_checked"],  errors="coerce").fillna(0).astype(int)
    df["passenger_age"] = pd.to_numeric(df["passenger_age"], errors="coerce")

    log.info("Registros válidos tras transformación: %d", len(df))

    # ── 2.5 Construcción de dimensiones ─────────────────────

    # dim_fecha – una fila por fecha único YYYYMMDD
    fechas_unicas = pd.DataFrame(
        {"departure_dt": df["departure_dt"].unique()}
    ).dropna()
    fechas_unicas["id_fecha"]   = fechas_unicas["departure_dt"].apply(fecha_a_id)
    fechas_unicas["fecha"]      = fechas_unicas["departure_dt"].dt.date
    fechas_unicas["anio"]       = fechas_unicas["departure_dt"].dt.year
    fechas_unicas["trimestre"]  = fechas_unicas["departure_dt"].dt.quarter
    fechas_unicas["mes"]        = fechas_unicas["departure_dt"].dt.month
    fechas_unicas["nombre_mes"] = fechas_unicas["departure_dt"].dt.strftime("%B")
    fechas_unicas["semana"]     = fechas_unicas["departure_dt"].dt.isocalendar().week.astype(int)
    fechas_unicas["dia"]        = fechas_unicas["departure_dt"].dt.day
    fechas_unicas["nombre_dia"] = fechas_unicas["departure_dt"].dt.strftime("%A")
    dim_fecha = fechas_unicas.drop(columns=["departure_dt"]).drop_duplicates(subset=["id_fecha"])

    # dim_aerolinea
    dim_aerolinea = (
        df[["airline_code", "airline_name"]]
        .drop_duplicates(subset=["airline_code"])
        .rename(columns={"airline_code": "codigo_aerolinea",
                         "airline_name": "nombre_aerolinea"})
        .reset_index(drop=True)
    )

    # dim_aeropuerto (unión origen + destino)
    aeropuertos = pd.concat([
        df[["origin_airport"]].rename(columns={"origin_airport": "codigo_aeropuerto"}),
        df[["destination_airport"]].rename(columns={"destination_airport": "codigo_aeropuerto"}),
    ]).dropna().drop_duplicates().reset_index(drop=True)
    dim_aeropuerto = aeropuertos

    # dim_pasajero
    dim_pasajero = (
        df[["passenger_id", "passenger_gender", "passenger_age", "passenger_nationality"]]
        .drop_duplicates(subset=["passenger_id"])
        .rename(columns={
            "passenger_id":          "uuid_pasajero",
            "passenger_gender":      "genero",
            "passenger_age":         "edad",
            "passenger_nationality": "nacionalidad",
        })
        .dropna(subset=["uuid_pasajero"])
        .reset_index(drop=True)
    )

    # dim_tipo_vuelo
    dim_tipo_vuelo = (
        df[["flight_number", "aircraft_type", "cabin_class",
            "status", "sales_channel", "payment_method", "currency"]]
        .rename(columns={
            "flight_number":  "numero_vuelo",
            "aircraft_type":  "tipo_aeronave",
            "cabin_class":    "clase_cabina",
            "status":         "estado_vuelo",
            "sales_channel":  "canal_venta",
            "payment_method": "metodo_pago",
            "currency":       "moneda_original",
        })
        .fillna({"tipo_aeronave": "DESCONOCIDO",
                 "canal_venta":   "DESCONOCIDO"})
        .drop_duplicates()
        .reset_index(drop=True)
    )

    log.info("dim_fecha      : %d filas", len(dim_fecha))
    log.info("dim_aerolinea  : %d filas", len(dim_aerolinea))
    log.info("dim_aeropuerto : %d filas", len(dim_aeropuerto))
    log.info("dim_pasajero   : %d filas", len(dim_pasajero))
    log.info("dim_tipo_vuelo : %d filas", len(dim_tipo_vuelo))

    return {
        "dim_fecha":      dim_fecha,
        "dim_aerolinea":  dim_aerolinea,
        "dim_aeropuerto": dim_aeropuerto,
        "dim_pasajero":   dim_pasajero,
        "dim_tipo_vuelo": dim_tipo_vuelo,
        "staging":        df,         # datos limpios para construir el hecho
    }


# ============================================================
# FASE 3 – CARGA
# ============================================================

def cargar(tablas: dict[str, pd.DataFrame], engine) -> int:
    log.info("=== FASE 3: CARGA ===")

    with engine.begin() as conn:

        # ── 3.1 dim_fecha ───────────────────────────────────
        log.info("Cargando dim_fecha …")
        dim_fecha = tablas["dim_fecha"]
        conn.execute(text("DELETE FROM dbo.dim_fecha"))
        dim_fecha[["id_fecha","fecha","anio","trimestre","mes",
                   "nombre_mes","semana","dia","nombre_dia"]].to_sql(
            "dim_fecha", engine, if_exists="append", index=False
        )
        log.info("  → %d filas insertadas", len(dim_fecha))

        # ── 3.2 dim_aerolinea ───────────────────────────────
        log.info("Cargando dim_aerolinea …")
        dim_aerolinea = tablas["dim_aerolinea"]
        conn.execute(text("DELETE FROM dbo.dim_aerolinea"))
        dim_aerolinea.to_sql("dim_aerolinea", engine, if_exists="append", index=False)
        log.info("  → %d filas insertadas", len(dim_aerolinea))

        # ── 3.3 dim_aeropuerto ──────────────────────────────
        log.info("Cargando dim_aeropuerto …")
        dim_aeropuerto = tablas["dim_aeropuerto"]
        conn.execute(text("DELETE FROM dbo.dim_aeropuerto"))
        dim_aeropuerto.to_sql("dim_aeropuerto", engine, if_exists="append", index=False)
        log.info("  → %d filas insertadas", len(dim_aeropuerto))

        # ── 3.4 dim_pasajero ────────────────────────────────
        log.info("Cargando dim_pasajero …")
        dim_pasajero = tablas["dim_pasajero"]
        conn.execute(text("DELETE FROM dbo.dim_pasajero"))
        dim_pasajero.to_sql("dim_pasajero", engine, if_exists="append", index=False)
        log.info("  → %d filas insertadas", len(dim_pasajero))

        # ── 3.5 dim_tipo_vuelo ──────────────────────────────
        log.info("Cargando dim_tipo_vuelo …")
        dim_tipo_vuelo = tablas["dim_tipo_vuelo"]
        conn.execute(text("DELETE FROM dbo.dim_tipo_vuelo"))
        dim_tipo_vuelo.to_sql("dim_tipo_vuelo", engine, if_exists="append", index=False)
        log.info("  → %d filas insertadas", len(dim_tipo_vuelo))

        # ── 3.6 Lectura de llaves desde BD ──────────────────
        log.info("Leyendo surrogate keys de dimensiones …")
        df_alf_k  = pd.read_sql("SELECT id_aerolinea, codigo_aerolinea FROM dbo.dim_aerolinea",  conn)
        df_apt_k  = pd.read_sql("SELECT id_aeropuerto, codigo_aeropuerto FROM dbo.dim_aeropuerto", conn)
        df_pas_k  = pd.read_sql("SELECT id_pasajero, uuid_pasajero FROM dbo.dim_pasajero",        conn)
        df_tv_k   = pd.read_sql(
            "SELECT id_tipo_vuelo, numero_vuelo, clase_cabina, estado_vuelo, "
            "       canal_venta, metodo_pago, moneda_original "
            "FROM dbo.dim_tipo_vuelo", conn
        )

        # ── 3.7 Construcción del hecho ──────────────────────
        log.info("Construyendo tabla de hechos …")
        stg = tablas["staging"].copy()

        # Joins para obtener surrogate keys
        stg = stg.merge(
            df_alf_k, left_on="airline_code",
            right_on="codigo_aerolinea", how="left"
        )
        stg = stg.merge(
            df_apt_k.rename(columns={"id_aeropuerto": "id_origen",
                                     "codigo_aeropuerto": "o_code"}),
            left_on="origin_airport", right_on="o_code", how="left"
        )
        stg = stg.merge(
            df_apt_k.rename(columns={"id_aeropuerto": "id_destino",
                                     "codigo_aeropuerto": "d_code"}),
            left_on="destination_airport", right_on="d_code", how="left"
        )
        stg = stg.merge(
            df_pas_k, left_on="passenger_id",
            right_on="uuid_pasajero", how="left"
        )
        stg = stg.merge(
            df_tv_k,
            left_on=["flight_number", "cabin_class", "status",
                     "sales_channel", "payment_method", "currency"],
            right_on=["numero_vuelo", "clase_cabina", "estado_vuelo",
                      "canal_venta",  "metodo_pago",  "moneda_original"],
            how="left"
        )

        fact = stg[[
            "fecha_salida_id", "id_aerolinea", "id_origen", "id_destino",
            "id_pasajero", "id_tipo_vuelo",
            "record_id", "seat",
            "ticket_price_usd_est", "duration_min", "delay_min",
            "bags_total", "bags_checked",
        ]].rename(columns={
            "fecha_salida_id":     "fk_fecha_salida",
            "id_aerolinea":        "fk_aerolinea",
            "id_origen":           "fk_origen",
            "id_destino":          "fk_destino",
            "id_pasajero":         "fk_pasajero",
            "id_tipo_vuelo":       "fk_tipo_vuelo",
            "record_id":           "record_id_origen",
            "seat":                "asiento",
            "ticket_price_usd_est":"precio_usd",
            "duration_min":        "duracion_min",
            "delay_min":           "retraso_min",
            "bags_total":          "n_maletas_total",
            "bags_checked":        "n_maletas_bodega",
        })

        # Descartar filas con FK nulas (registros que no pudieron resolverse)
        antes = len(fact)
        fact  = fact.dropna(subset=["fk_fecha_salida","fk_aerolinea",
                                    "fk_origen","fk_destino",
                                    "fk_pasajero","fk_tipo_vuelo"])
        log.warning("Registros descartados por FK sin resolver: %d", antes - len(fact))

        # Convertir tipos
        int_cols = ["fk_fecha_salida","fk_aerolinea","fk_origen","fk_destino",
                    "fk_pasajero","fk_tipo_vuelo","record_id_origen",
                    "retraso_min","n_maletas_total","n_maletas_bodega"]
        for c in int_cols:
            fact[c] = fact[c].astype(int)

        fact["precio_usd"] = fact["precio_usd"].fillna(0).astype(float)

        # ── 3.8 Insertar hechos ─────────────────────────────
        log.info("Cargando fact_vuelo …")
        conn.execute(text("DELETE FROM dbo.fact_vuelo"))
        fact.to_sql("fact_vuelo", engine, if_exists="append", index=False, chunksize=1000)
        log.info("  → %d filas insertadas en fact_vuelo", len(fact))

    return len(fact)


# ============================================================
# PUNTO DE ENTRADA PRINCIPAL
# ============================================================

def main():
    log.info("============================================================")
    log.info(" ETL VUELOS – Inicio: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("============================================================")

    try:
        # EXTRACCIÓN
        df_raw = extraer(CSV_PATH)

        # TRANSFORMACIÓN
        tablas = transformar(df_raw)

        # CARGA
        engine = build_engine()
        total  = cargar(tablas, engine)

        log.info("============================================================")
        log.info(" ETL completado exitosamente. Registros en fact_vuelo: %d", total)
        log.info("============================================================")

    except FileNotFoundError as e:
        log.error("Archivo fuente no encontrado: %s", e)
        raise
    except SQLAlchemyError as e:
        log.error("Error de base de datos: %s", e)
        raise
    except Exception as e:
        log.error("Error inesperado: %s", e)
        raise


if __name__ == "__main__":
    main()
