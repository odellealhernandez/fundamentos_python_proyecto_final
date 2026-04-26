import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os 
from proceso_etl import ejecutar_etl_completo

plt.style.use("seaborn-v0_8-whitegrid")

# Configuración de la página
st.set_page_config(page_title="Dashboard Expo Móvil 2026", layout="wide")

st.title("🚀 Sistema de Gestión Expo Móvil 2026")
st.sidebar.header("Configuración del Proceso")

# --- 1. PARÁMETROS EN LA BARRA LATERAL ---
tipo_cambio = st.sidebar.number_input("Tipo de Cambio (CRC/USD)", value=475.0)
modo_cierre = st.sidebar.checkbox("Activar Modo Cierre")

# Rutas de los archivos fijos esperados
ruta_principal = "output/Rep_Preliminar.xlsx"
ruta_resumen = "output/Rep_Resumen_Diario.xlsx"

# --- LÓGICA DE CARGA INICIAL DESDE DISCO ---
# Si no hemos cargado datos en esta sesión, intentamos leerlos de la carpeta output
if 'df_master' not in st.session_state:
    if os.path.exists(ruta_principal) and os.path.exists(ruta_resumen):
        # Leemos los archivos y los subimos a la memoria temporal de Streamlit
        st.session_state['df_master'] = pd.read_excel(ruta_principal)
        st.session_state['df_resumen'] = pd.read_excel(ruta_resumen)
        st.toast('Datos cargados exitosamente desde la carpeta local', icon='📂')
    else:
        # Si no existen, activamos una bandera para mostrar el mensaje de alerta
        st.session_state['requiere_etl'] = True

# --- 2. BOTÓN PARA EJECUTAR ETL (FORZAR ACTUALIZACIÓN) ---
if st.sidebar.button("Ejecutar ETL y Actualizar Datos"):
    with st.spinner('Procesando datos de los archivos de origen: FG-Express, HE-BPM y ULTM-BPM...'):
        # Ejecutamos el script ETL
        df_final, df_resumen = ejecutar_etl_completo(tipo_cambio, modo_cierre)
        
        # Actualizamos la memoria de la sesión con los nuevos DataFrames
        st.session_state['df_master'] = df_final
        st.session_state['df_resumen'] = df_resumen
        
        # Si existía la alerta de "requiere_etl", la limpiamos
        if 'requiere_etl' in st.session_state:
            del st.session_state['requiere_etl']
            
        st.success("¡Proceso ETL completado y datos actualizados con éxito!")

# --- 3. ANÁLISIS Y VISUALIZACIÓN ---
if 'df_master' in st.session_state:
    df = st.session_state['df_master']
    
    # 1. Reordenamos los nombres de las pestañas
    tab1, tab2, tab3 = st.tabs(["📈 Gráficos", "🗓️ Resumen Diario", "📊 Vista de Datos"])
    
    # --- NUEVA PESTAÑA 1: GRÁFICOS ---
    with tab1:
        st.subheader("Análisis Visual (Paneles de Control)")
        
        # FILA 1 (2 Gráficos)
        col1, col2 = st.columns(2)
        with col1:
            fig5, ax5 = plt.subplots(figsize=(6, 4)) 
            ax5.grid(False)
            df_ap_sin = df[(df['CANAL'] != 'FG-EXPRESS') & (df['ESTADO_FINAL'] == 'APROBADO')]
            df_ap_con = df[(df['CANAL'] == 'FG-EXPRESS') & (df['ESTADO_FINAL'] == 'APROBADO')]
            cant_sin = df_ap_sin.groupby("DIA")["SOLICITUD"].count().rename("SIN_FG")
            cant_con = df_ap_con.groupby("DIA")["SOLICITUD"].count().rename("CON_FG")
            df_barras = pd.concat([cant_sin, cant_con], axis=1).fillna(0).astype(int).sort_index()
            x = np.arange(len(df_barras.index))
            width = 0.35
            ax5.bar(x - width/2, df_barras["SIN_FG"], width, label="Sin FG-EXPRESS", color="#4CAF50")
            ax5.bar(x + width/2, df_barras["CON_FG"], width, label="Solo FG-EXPRESS", color="#505363")
            ax5.set_title("Aprobadas: Sin FG vs Solo FG", fontsize=10, fontweight="bold")
            ax5.set_xticks(x)
            ax5.set_xticklabels(df_barras.index)
            ax5.legend(frameon=False, fontsize=8)
            st.pyplot(fig5)
            
        with col2:
            fig1, ax1 = plt.subplots(figsize=(4, 3)) 
            df['ESTADO_FINAL'].value_counts().plot(
                kind='pie', 
                autopct='%1.1f%%', 
                ax=ax1, 
                colors=["#505363", "#4CAF50", "#DBD40A"],
                textprops={'fontsize': 8, 'color': 'white', 'weight': 'bold'}
            )
            ax1.set_title("Distribución de Estados de Crédito", fontsize=10)
            ax1.set_ylabel('')
            st.pyplot(fig1, width="content")
            

        st.divider()

        # FILA 2 (2 Gráficos)
        col3, col4 = st.columns(2)
        
        with col3:
            fig3, ax3 = plt.subplots(figsize=(6, 4))
            df_canal_dia = df.groupby(['DIA', 'ESTADO_FINAL']).size().unstack(fill_value=0)
            colores_estados = {'APROBADO': '#4CAF50', 'CONDICIONADO': '#FFC107', 'RECHAZADO': '#505363'}
            colores_usados = [colores_estados.get(col, '#9E9E9E') for col in df_canal_dia.columns]
            
            df_canal_dia.plot(kind='bar', stacked=True, ax=ax3, color=colores_usados, grid=False)
            ax3.set_title("Estados de Solicitud por Día", fontsize=10)
            ax3.set_xlabel("Día de la Expo", fontsize=8)
            ax3.set_ylabel("Cantidad", fontsize=8)
            st.pyplot(fig3)

        with col4:
            fig2, ax2 = plt.subplots(figsize=(6, 4))
            df.groupby('CANAL')['MONTO_CREDITO_DOLARIZADO'].sum().sort_values().plot(kind='barh', ax=ax2, color="#505363", grid=False)
            ax2.set_title("Monto Total Dolarizado por Canal", fontsize=10)
            ax2.set_xlabel("Monto (USD)", fontsize=8)
            ax2.set_ylabel("Canal", fontsize=8)
            st.pyplot(fig2)


        st.divider()
        
        # FILA 3 (Comparativa de Aprobaciones)
        col5, col6 = st.columns(2)
        with col5:
            fig4, ax4 = plt.subplots(figsize=(6, 4))
            df['AGENCIA'].value_counts().head(5).sort_values().plot(kind='barh', ax=ax4, color='#4CAF50', grid=False)
            ax4.set_title("Top 5 Agencias con más solicitudes", fontsize=10)
            ax4.set_xlabel("Cantidad", fontsize=8)
            ax4.set_ylabel("Agencia", fontsize=8)
            st.pyplot(fig4)

    # --- NUEVA PESTAÑA 2: RESUMEN DIARIO ---
    with tab2:
        st.subheader("Indicadores del Resumen Diario")
        st.dataframe(st.session_state['df_resumen'])

    # --- NUEVA PESTAÑA 3: VISTA DE DATOS ---
    with tab3:
        st.subheader("Base Consolidada (Muestra)")
        st.dataframe(df.head(100))
        
else:
    st.info("👈 Ajusta los parámetros y presiona 'Ejecutar ETL' para comenzar el análisis.")