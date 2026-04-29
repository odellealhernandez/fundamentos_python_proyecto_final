import pandas as pd
import numpy as np
import os

def limpiar_y_transformar_datos(df, parametros, tc_crcusd, log_print):
    """
    Aplica limpieza de texto, conversión de monedas, mapeo de prioridades y validaciones.
    """
    log_print("\n➡️ PASO 7: Aplicando Reglas de Negocio (Estados, Prioridades y Validaciones)...")
    
    # Limpieza básica
    df['IDENTIFICACION'] = df['IDENTIFICACION'].astype(str).str.strip().str.replace("-", "", regex=False).str.lstrip("0").str.replace(" ", "", regex=False)
    
    columnas_a_mayusculas = ["IDENTIFICACION", "AGENCIA", "MARCA", "MONEDA_CREDITO", "ESTADO"]
    for col in columnas_a_mayusculas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()

    df['MONEDA_CREDITO'] = df['MONEDA_CREDITO'].replace({"COLONES": "CRC", "DOLARES": "USD"})
    df['MONTO_CREDITO_DOLARIZADO'] = np.where(df['MONEDA_CREDITO'] == 'CRC', df['MONTO_CREDITO'] / tc_crcusd, df['MONTO_CREDITO'])

    # Homologación de estados
    dicc_estados = {
        "APROBADO - VERDE EN FIRME": "APROBADO", "APROBADO": "APROBADO", "APROBAR": "APROBADO",
        "VB EJECUTIVO": "APROBADO", "EN CAMBIOS CARATULA": "APROBADO", "CAMBIOS NO PROCEDEN": "APROBADO",
        "EN CAMBIOS": "APROBADO", "SEGUROS": "APROBADO", "RECOMIENDA": "APROBADO",
        "CONDICIONADO": "CONDICIONADO", "CONDICIONADO PERSONA EXTRANJERA": "CONDICIONADO",
        "DENEGADO": "RECHAZADO", "RECHAZO": "RECHAZADO", "RECHAZAR": "RECHAZADO",
        "DESCARTADO ERROR EN SCORING": "RECHAZADO", "DESCARTADO": "RECHAZADO", "ANALISIS": "EN PROCESO",
        "ANÁLISIS DE DEVOLUCIÓN": "EN PROCESO", "DEVOLUCION": "EN PROCESO", "INICIO": "EN PROCESO",
        "IR A COTIZACION": "EN PROCESO", "": "EN PROCESO", "NAN": "EN PROCESO"
    }
    df['ESTADO_HOMOLOGADO'] = df['ESTADO'].map(dicc_estados).fillna("EN PROCESO")
    df['DIA'] = df['FECHA_SOLICITUD'].dt.day
    df['PRIORIDAD'] = df['ESTADO_HOMOLOGADO'].map(parametros.get("PRIORIDADES_ESTADO", {}))

    # Deduplicación
    log_print(f"   Eliminando duplicados de Identificación manteniendo la prioridad más alta (1=Aprobado)...")
    df = df.sort_values(by='PRIORIDAD', ascending=True).drop_duplicates(subset=['IDENTIFICACION'], keep='first')
    log_print(f"   Registros únicos después de limpiar duplicados: {len(df)}")

    df['ESTADO_CORREGIDO'] = np.where(df['ESTADO_HOMOLOGADO'] == "EN PROCESO", "CONDICIONADO", df['ESTADO_HOMOLOGADO'])

    # Redistribución de condicionados
    aprobados = len(df[df['ESTADO_CORREGIDO'] == 'APROBADO'])
    rechazados = len(df[df['ESTADO_CORREGIDO'] == 'RECHAZADO'])
    base = aprobados + rechazados
    tasa_aprobacion = aprobados / base if base > 0 else 0
    
    log_print(f"   > Tasa de aprobación calculada: {tasa_aprobacion:.2%}")

    condicionados = df[df['ESTADO_CORREGIDO'] == 'CONDICIONADO'].copy()
    otros = df[df['ESTADO_CORREGIDO'] != 'CONDICIONADO'].copy()
    
    if len(condicionados) > 0:
        cant_a_aprobar = int(round(len(condicionados) * tasa_aprobacion, 0))
        log_print(f"   > Se redistribuirán {cant_a_aprobar} condicionados a APROBADO y {len(condicionados)-cant_a_aprobar} a RECHAZADO.")
        if cant_a_aprobar > 0:
            aprobados_azar = condicionados.sample(n=cant_a_aprobar, random_state=42)
            condicionados.loc[aprobados_azar.index, 'ESTADO_FINAL'] = 'APROBADO'
            condicionados.loc[~condicionados.index.isin(aprobados_azar.index), 'ESTADO_FINAL'] = 'RECHAZADO'
        else:
            condicionados['ESTADO_FINAL'] = 'RECHAZADO'
    
    otros['ESTADO_FINAL'] = otros['ESTADO_CORREGIDO']
    df = pd.concat([otros, condicionados], ignore_index=True)

    # Agencias y Validaciones
    archivo_homologacion = "config/agencias_homologacion.xlsx"

    # Si no existe la configuración, la creamos (generará el log_print correspondiente)
    if not os.path.exists(archivo_homologacion):
        dicc_agencias_base = {
            "AGENCIA DANISSA" : "DANISSA", "AGENCIA DATSUN" : "DANISSA", "AMBACAR " : "AMBACAR",
            "AMBACAR" : "AMBACAR", "AMBACAR S.A." : "AMBACAR", "AUTO STAR" : "AUTO STAR",
            "AUTOMOTRIZ CR CA S.A." : "PURDY MOTOR", "AUTOMOTRIZ CR/CA" : "PURDY MOTOR", "BAIC" : "CORI MOTORS",
            "BAVARIAN MOTORS" : "BAVARIAN MOTORS", "CORI CAR" : "CORI CAR", "CORI MOTORS" : "CORI MOTORS",
            "CORI MOTORS S.A" : "CORI MOTORS", "CORPORACIÓN GRUPO Q" : "GRUPO Q", "DANISSA" : "DANISSA",
            "DISTRITO AUTOMOTRIZ" : "INCHCAPE", "ELECTRO AUTOS" : "ELECTRO AUTOS", "EURO ADVANCE" : "OMOJAE MOTORS (EURO ADVANCE)",
            "EURO ADVANCE S.A" : "OMOJAE MOTORS (EURO ADVANCE)", "FACO" : "FACO", "GRAND MOTORS" : "GRAND MOTORS",
            "GRUPO DANISSA" : "DANISSA", "GRUPO Q" : "GRUPO Q", "INCHCAPE" : "INCHCAPE",
            "INCHCAPOE" : "INCHCAPE", "KIA" : "QUALITY MOTORS", "MOTORES BRITÁNICOS" : "MOTORES BRITÁNICOS",
            "MOTORES BRITÁNICOS (GAC MOTOR)" : "MOTORES BRITÁNICOS", "MOTORES BRITÁNICOS DE CR" : "MOTORES BRITÁNICOS",
            "OMOJAE MOTORS" : "OMOJAE MOTORS (EURO ADVANCE)", "OMOJAE MOTORS (EURO ADVANCE)" : "OMOJAE MOTORS (EURO ADVANCE)",
            "PACIFIC MOTORS S.A" : "AUTO STAR", "PURDY MOTOR" : "PURDY MOTOR", "QUALITY MOTORS" : "QUALITY MOTORS",
            "SAVA-FACO" : "FACO", "TOYO DEL ATLANTICO" : "TOYO DEL ATLANTICO", "VEINSA" : "VEINSA",
            "VEINSA MOTORS" : "VEINSA", "VETRASA" : "INCHCAPE"
        }
        if not os.path.exists("config"):
            os.makedirs("config")
        df_base = pd.DataFrame(list(dicc_agencias_base.items()), columns=["AGENCIA_ORIGEN", "AGENCIA_HOMOLOGADA"])
        df_base.to_excel(archivo_homologacion, index=False)
        log_print(f"   ⚠️ Se creó la plantilla de configuración de agencias en: {archivo_homologacion}")

    try:
        df_agencias = pd.read_excel(archivo_homologacion)
        dicc_agencias = dict(zip(df_agencias.iloc[:, 0].astype(str).str.upper().str.strip(), df_agencias.iloc[:, 1].astype(str).str.upper().str.strip()))
        log_print(f"   > Diccionario de agencias cargado desde Excel ({len(dicc_agencias)} mapeos).")
    except Exception as e:
        log_print(f"   ❌ Error leyendo {archivo_homologacion}: {e}")
        dicc_agencias = {}

    df['AGENCIA'] = df['AGENCIA'].map(dicc_agencias).fillna(df['AGENCIA'] + "***NO HOMOLOG**")

    log_print("   Generando validaciones de Montos y Monedas...")
    limites = parametros.get("LIMITES_VALIDACION_MONTOS", {})
    cond_crc = (df['MONEDA_CREDITO'] == 'CRC') & ((df['MONTO_CREDITO'] < limites.get("CRC", {}).get("MINIMO", 3000000)) | (df['MONTO_CREDITO'] > limites.get("CRC", {}).get("MAXIMO", 60000000)))
    cond_usd = (df['MONEDA_CREDITO'] == 'USD') & ((df['MONTO_CREDITO'] < limites.get("USD", {}).get("MINIMO", 6000)) | (df['MONTO_CREDITO'] > limites.get("USD", {}).get("MAXIMO", 150000)))
    df['VALIDACION'] = np.where(cond_crc | cond_usd, "REVISE", "OK")

    # Orden final
    log_print("\n➡️ PASO 8: Ordenando columnas finales...")
    columnas_finales = [
        "SOLICITUD", "CANAL", "IDENTIFICACION", "FECHA_SOLICITUD", "ESTADO",
        "ESTADO_HOMOLOGADO", "ESTADO_CORREGIDO", "ESTADO_FINAL", "MONEDA_CREDITO",
        "MONTO_CREDITO", "MONTO_CREDITO_DOLARIZADO", "AGENCIA", "MARCA",
        "SCORE_FILL", "VALIDACION", "PRIORIDAD", "DIA"
    ]
    for col in columnas_finales:
        if col not in df.columns:
            df[col] = np.nan

    return df[columnas_finales]