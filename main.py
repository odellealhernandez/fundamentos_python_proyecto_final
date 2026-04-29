import os
import json
import pandas as pd
from datetime import datetime

# IMPORTAR LOS MÓDULOS DE LAS FUNCIONES COMPLEMENTARIAS.
from cargar_datos import cargar_datos
from limpiar_y_transformar_datos import limpiar_y_transformar_datos
from output import generar_outputs

def ejecutar_etl_completo(tc_interfaz=None, modo_cierre_interfaz=None):
    # Setup de directorios
    rutas = {"config": "config", "data": "data", "output": "output", "logs": "output/logs"}
    for ruta in rutas.values():
        os.makedirs(ruta, exist_ok=True)

    # Setup de Timestamp y Logger
    tiempo_inicio = datetime.now()
    timestamp = tiempo_inicio.strftime("%Y%m%d%H%M%S")
    archivo_log = f"{rutas['logs']}/Log_Ejecucion_{timestamp}.txt"

    def log_print(mensaje):
        print(mensaje)
        with open(archivo_log, "a", encoding="utf-8") as file:
            file.write(mensaje + "\n")

    # Carga de Parámetros
    archivo_parametros = f"{rutas['config']}/parametros.json"
    if not os.path.exists(archivo_parametros):
        log_print("❌ ERROR: No se encontró el archivo de parámetros JSON.")
        return pd.DataFrame(), pd.DataFrame()

    with open(archivo_parametros, "r", encoding="utf-8") as f:
        parametros = json.load(f)

    ES_CIERRE = modo_cierre_interfaz if modo_cierre_interfaz is not None else parametros.get("ES_CIERRE", False)
    TC_CRCUSD = tc_interfaz if tc_interfaz is not None else parametros.get("TC_CRCUSD", 475.00)

    log_print("============================================================")
    log_print("🚀 INICIANDO PROCESO ETL EXPO MOVIL")
    log_print(f"🕒 Fecha de Inicio: {tiempo_inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    log_print("============================================================\n")

    # --- EJECUCIÓN PROCESO MODULAR ---
    
    # 1. Extracción
    df_raw = cargar_datos(rutas["data"], log_print)
    
    if not df_raw.empty:
        # 2. Transformación
        df_limpio = limpiar_y_transformar_datos(df_raw, parametros, TC_CRCUSD, log_print)
        
        # 3. Carga (Outputs)
        df_resumen = generar_outputs(df_limpio, parametros, ES_CIERRE, timestamp, rutas["output"], rutas["logs"], log_print)
    else:
        df_limpio, df_resumen = pd.DataFrame(), pd.DataFrame()

    tiempo_fin = datetime.now()
    duracion = tiempo_fin - tiempo_inicio
    
    log_print(f"\n============================================================")
    log_print(f"🏁 PROCESO FINALIZADO")
    log_print(f"⏳ Tiempo de duración: {duracion.total_seconds():.2f} segundos")
    log_print(f"============================================================")
    
    return df_limpio, df_resumen

if __name__ == "__main__":
    df_final, df_resumen_final = ejecutar_etl_completo()