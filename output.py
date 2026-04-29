import pandas as pd
import numpy as np

def generar_outputs(df, parametros, es_cierre, timestamp, ruta_output, ruta_logs, log_print):
    """
    Exporta los DataFrames finales y calcula el resumen diario.
    """
    log_print(f"\n➡️ PASOS 9 y 10: Exportando resultados...")
    nombres_out = parametros.get("NOMBRES_ARCHIVOS_SALIDA", {})
    
    # 1. Verdes en Firme
    df_verdes = df[(df['CANAL'] == 'FG-EXPRESS') & (df['ESTADO'] == 'APROBADO - VERDE EN FIRME')].copy()
    if not df_verdes.empty:
        df_verdes.to_excel(f"{ruta_output}/{nombres_out.get('VERDES_FIRME', 'Verdes')}.xlsx", index=False)
        df_verdes.to_excel(f"{ruta_logs}/{nombres_out.get('VERDES_FIRME', 'Verdes')}_{timestamp}.xlsx", index=False)
        log_print(f"   ✅ Verdes en Firme guardado ({len(df_verdes)} registros)")

    # 2. Base Principal
    df.to_excel(f"{ruta_output}/{nombres_out.get('PRINCIPAL', 'Principal')}.xlsx", index=False)
    df.to_excel(f"{ruta_logs}/{nombres_out.get('PRINCIPAL', 'Principal')}_{timestamp}.xlsx", index=False)
    log_print(f"   ✅ Archivo Principal guardado ({len(df)} registros)")

    # 3. Resumen Diario
    columna_aprobado = 'ESTADO_HOMOLOGADO' if es_cierre else 'ESTADO_FINAL'
    df_temp = df.copy()
    df_temp['es_aprobado'] = (df_temp[columna_aprobado] == 'APROBADO').astype(int)
    df_temp['no_es_fill'] = (df_temp['CANAL'] != 'FG-EXPRESS').astype(int)
    df_temp['aprob_sin_fill'] = df_temp['es_aprobado'] * df_temp['no_es_fill']

    df_resumen = df_temp.groupby('DIA').agg(
        Q_TOTAL=('SOLICITUD', 'count'),                         
        Q_TOTAL_SIN_FILL_N_GO=('no_es_fill', 'sum'),            
        Q_TOTAL_APROB=('es_aprobado', 'sum'),                   
        Q_TOTAL_APROB_SIN_FILL_N_GO=('aprob_sin_fill', 'sum')   
    ).reset_index()

    df_resumen['%APROB_DIARIO'] = np.where(df_resumen['Q_TOTAL'] > 0, df_resumen['Q_TOTAL_APROB'] / df_resumen['Q_TOTAL'], 0)
    df_resumen['%APROB_DIARIO_SIN_FILL_N_GO'] = np.where(df_resumen['Q_TOTAL_SIN_FILL_N_GO'] > 0, df_resumen['Q_TOTAL_APROB_SIN_FILL_N_GO'] / df_resumen['Q_TOTAL_SIN_FILL_N_GO'], 0)

    df_resumen.to_excel(f"{ruta_output}/{nombres_out.get('RESUMEN', 'Resumen')}.xlsx", index=False)
    df_resumen.to_excel(f"{ruta_logs}/{nombres_out.get('RESUMEN', 'Resumen')}_{timestamp}.xlsx", index=False)
    log_print(f"   ✅ Archivo Resumen guardado")
    
    return df_resumen