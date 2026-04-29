import pandas as pd
import os
import glob
import warnings

# Ignoramos alertas de Excel temporales
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def cargar_datos(ruta_data, log_print):
    """
    Escanea el directorio, convierte archivos .xls a .xlsx y consolida las fuentes de datos.
    """
    log_print("➡️ PASO 1: Buscando y convirtiendo archivos .xls a .xlsx...")
    archivos_xls = glob.glob(f"{ruta_data}/*.xls")

    if archivos_xls:
        for archivo in archivos_xls:
            try:
                log_print(f"   Convirtiendo: {archivo}")
                try:
                    df_temp = pd.read_excel(archivo, engine="xlrd")
                except:
                    try:
                        df_temp = pd.read_html(archivo)[0]
                    except:
                        df_temp = pd.read_csv(archivo, sep='\t')

                nuevo_nombre = archivo + "x" 
                df_temp.to_excel(nuevo_nombre, index=False)
                os.remove(archivo) 
                log_print(f"   ✅ Conversión exitosa: {nuevo_nombre}")
                
            except Exception as e:
                log_print(f"   ❌ Error convirtiendo {archivo}. Detalle: {e}")

    log_print("\n➡️ PASOS 2 al 6: Leyendo, tipando, renombrando y consolidando Dataframes...")
    configs_archivos = {
        "fg_express": {
            "file": f"{ruta_data}/fg_express.xlsx",
            "cols": ["IDOPERATION", "CUSTOMERID", "CREATEDDATE", "NAME", "STATUS_GENERAL", "Comercio Sucursal", "Moneda Credito", "Agencia", "Marca", "Estilo", "Monto Financiar", "Ejecutivo", "Ejecutivo Email", "Usuario", "SCORE_FILL"],
            "types": {"CUSTOMERID": str, "Monto Financiar": float},
            "dates": ["CREATEDDATE"],
            "rename": {
                "IDOPERATION": "SOLICITUD", "CUSTOMERID": "IDENTIFICACION", "CREATEDDATE": "FECHA_SOLICITUD",
                "STATUS_GENERAL": "ESTADO", "Moneda Credito": "MONEDA_CREDITO", "Agencia": "AGENCIA",
                "Marca": "MARCA", "Monto Financiar": "MONTO_CREDITO", "SCORE_FILL": "SCORE_FILL",
                "NAME": "NAME", "Comercio Sucursal": "Comercio Sucursal", "Estilo": "Estilo", 
                "Ejecutivo": "Ejecutivo", "Ejecutivo Email": "Ejecutivo Email", "Usuario": "Usuario"
            },
            "canal": "FG-EXPRESS"
        },
        "he_bpm_prend": {
            "file": f"{ruta_data}/he_bpm_prend.xlsx",
            "cols": ["Solicitud", "Fecha de Solicitud", "Etapa", "Numero de Identificación", "Agencia (Agencia del Vehiculo)", "Marca (Marca de Vehiculo)", "Moneda (Moneda del Credito)", "Monto del Credito"],
            "types": {"Numero de Identificación": str, "Monto del Credito": float},
            "dates": ["Fecha de Solicitud"],
            "rename": {
                "Solicitud": "SOLICITUD", "Fecha de Solicitud": "FECHA_SOLICITUD", "Etapa": "ESTADO",
                "Numero de Identificación": "IDENTIFICACION", "Agencia (Agencia del Vehiculo)": "AGENCIA",
                "Marca (Marca de Vehiculo)": "MARCA", "Moneda (Moneda del Credito)": "MONEDA_CREDITO",
                "Monto del Credito": "MONTO_CREDITO"
            },
            "canal": "HE-BPM"
        },
        "he_bpm_leas": {
            "file": f"{ruta_data}/he_bpm_leas.xlsx",
            "cols": ["Solicitud", "Fecha de Solicitud", "Etapa", "Numero de Identificacion", "Monto del credito", "Moneda (Moneda)", "Agencia", "Marca"],
            "types": {"Numero de Identificacion": str, "Monto del credito": float},
            "dates": ["Fecha de Solicitud"],
            "rename": {
                "Solicitud": "SOLICITUD", "Fecha de Solicitud": "FECHA_SOLICITUD", "Etapa": "ESTADO",
                "Numero de Identificacion": "IDENTIFICACION", "Monto del credito": "MONTO_CREDITO",
                "Moneda (Moneda)": "MONEDA_CREDITO", "Agencia": "AGENCIA", "Marca": "MARCA"
            },
            "canal": "HE-BPM"
        },
        "ultm_bpm": {
            "file": f"{ruta_data}/ultm_bpm.xlsx",
            "cols": ["Fecha Cotiza", "Incidente", "Identificación", "Agencia Vehículo", "Marca Vehículo", "Moneda", "Monto Préstamo", "Decisión Crédito"],
            "types": {"Identificación": str, "Monto Préstamo": float},
            "dates": ["Fecha Cotiza"],
            "rename": {
                "Fecha Cotiza": "FECHA_SOLICITUD", "Incidente": "SOLICITUD", "Identificación": "IDENTIFICACION",
                "Agencia Vehículo": "AGENCIA", "Marca Vehículo": "MARCA", "Moneda": "MONEDA_CREDITO",
                "Monto Préstamo": "MONTO_CREDITO", "Decisión Crédito": "ESTADO"
            },
            "canal": "ULTM-BPM"
        }
    }

    lista_dataframes = []
    for nombre, config in configs_archivos.items():
        if os.path.exists(config["file"]):
            log_print(f"   Procesando: {config['file']}")
            df = pd.read_excel(config["file"], usecols=config["cols"], dtype=config["types"])
            for col_fecha in config["dates"]:
                if col_fecha in df.columns:
                    df[col_fecha] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df = df.rename(columns=config["rename"])
            df["CANAL"] = config["canal"]
            lista_dataframes.append(df)
        else:
            log_print(f"   ⚠️ ADVERTENCIA: No se encontró el archivo {config['file']}.")

    if not lista_dataframes:
        log_print("❌ ERROR CRÍTICO: No hay datos para consolidar.")
        return pd.DataFrame()

    df_consolidado = pd.concat(lista_dataframes, ignore_index=True)
    log_print(f"   Total de registros consolidados: {len(df_consolidado)}")
    return df_consolidado