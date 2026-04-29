import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os 
import sys 

# importa el archivo "main.py" para ejecutar todos los módulos. 
from main import ejecutar_etl_completo 

plt.style.use("seaborn-v0_8-whitegrid")

class CapturadorConsola:
    """
    Intercepta los prints del sistema y los inyecta en un contenedor de Streamlit.
    """
    def __init__(self, contenedor_st):
        self.contenedor_st = contenedor_st
        self.texto = ""

    def write(self, texto):
        self.texto += texto
        self.contenedor_st.code(self.texto, language='bash')

    def flush(self):
        pass

st.set_page_config(page_title="Dashboard Expo Móvil 2026", layout="wide")

st.title("🚀 Sistema de Gestión Expo Móvil 2026")
st.sidebar.header("Configuración del Proceso")

tipo_cambio = st.sidebar.number_input("Tipo de Cambio (CRC/USD)", value=475.0)
modo_cierre = st.sidebar.checkbox("Activar Modo Cierre")

ruta_principal = "output/Rep_Preliminar.xlsx"
ruta_resumen = "output/Rep_Resumen_Diario.xlsx"

if 'df_master' not in st.session_state:
    if os.path.exists(ruta_principal) and os.path.exists(ruta_resumen):
        st.session_state['df_master'] = pd.read_excel(ruta_principal)
        st.session_state['df_resumen'] = pd.read_excel(ruta_resumen)
        st.toast('Datos cargados exitosamente desde la carpeta local', icon='📂')
    else:
        st.session_state['requiere_etl'] = True

if st.sidebar.button("Ejecutar ETL y Actualizar Datos"):
    
    st.divider()
    st.subheader("🖥️ Terminal de Ejecución ETL")
    
    contenedor_consola = st.empty() 
    
    stdout_original = sys.stdout
    sys.stdout = CapturadorConsola(contenedor_consola)
    
    try:
        with st.spinner('Procesando datos... revisa la terminal arriba para más detalles.'):
            df_final, df_resumen = ejecutar_etl_completo(tipo_cambio, modo_cierre)
            
            st.session_state['df_master'] = df_final
            st.session_state['df_resumen'] = df_resumen
            
            if 'requiere_etl' in st.session_state:
                del st.session_state['requiere_etl']
                
            st.success("¡Proceso ETL completado y datos actualizados con éxito!")
    except Exception as e:
        st.error(f"Ocurrió un error crítico durante el ETL: {e}")
    finally:
        sys.stdout = stdout_original

if 'df_master' in st.session_state:
    df = st.session_state['df_master']
    
    tab1, tab2, tab3 = st.tabs(["📈 Gráficos", "🗓️ Resumen Diario", "📊 Vista de Datos"])
    
    with tab1:
        st.subheader("Análisis Visual (Paneles de Control)")
        
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
            st.pyplot(fig5, use_container_width=True) # <-- CORREGIDO
            
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
            st.pyplot(fig1, use_container_width=True) # <-- CORREGIDO
            
        st.divider()

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
            st.pyplot(fig3, use_container_width=True) # <-- CORREGIDO

        with col4:
            fig2, ax2 = plt.subplots(figsize=(6, 4))
            df.groupby('CANAL')['MONTO_CREDITO_DOLARIZADO'].sum().sort_values().plot(kind='barh', ax=ax2, color="#505363", grid=False)
            ax2.set_title("Monto Total Dolarizado por Canal", fontsize=10)
            ax2.set_xlabel("Monto (USD)", fontsize=8)
            ax2.set_ylabel("Canal", fontsize=8)
            st.pyplot(fig2, use_container_width=True) # <-- CORREGIDO

        st.divider()
        
        col5, col6 = st.columns(2)
        with col5:
            fig4, ax4 = plt.subplots(figsize=(6, 4))
            df['AGENCIA'].value_counts().head(5).sort_values().plot(kind='barh', ax=ax4, color='#4CAF50', grid=False)
            ax4.set_title("Top 5 Agencias con más solicitudes", fontsize=10)
            ax4.set_xlabel("Cantidad", fontsize=8)
            ax4.set_ylabel("Agencia", fontsize=8)
            st.pyplot(fig4, use_container_width=True) # <-- CORREGIDO

    with tab2:
        st.subheader("Indicadores del Resumen Diario")
        df_res = st.session_state['df_resumen']
        
        if not df_res.empty:
            total_solicitudes = df_res['Q_TOTAL'].sum() if 'Q_TOTAL' in df_res.columns else 0
            total_aprobadas = df_res['Q_TOTAL_APROB'].sum() if 'Q_TOTAL_APROB' in df_res.columns else 0
            tasa_aprobacion = (total_aprobadas / total_solicitudes * 100) if total_solicitudes > 0 else 0
            
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Total Solicitudes (Acumulado)", f"{total_solicitudes:,.0f}")
            kpi2.metric("Total Aprobadas", f"{total_aprobadas:,.0f}")
            kpi3.metric("Tasa de Aprobación Global", f"{tasa_aprobacion:.1f}%")
            
            st.divider()
            
            columna_tiempo = 'DIA' if 'DIA' in df_res.columns else df_res.columns[0]
            
            st.markdown("**Evolución de Volumen (Total vs Aprobadas)**")
            df_tendencia = df_res.set_index(columna_tiempo)
            if 'Q_TOTAL' in df_tendencia.columns and 'Q_TOTAL_APROB' in df_tendencia.columns:
                st.line_chart(df_tendencia[['Q_TOTAL', 'Q_TOTAL_APROB']])
        
        st.markdown("**Tabla Resumen Detallada**")
        st.dataframe(df_res, use_container_width=True) # <-- CORREGIDO

    with tab3:
        st.subheader("Explorador de Base Consolidada")
        
        st.markdown("Utiliza los filtros para buscar casos específicos:")
        col_f1, col_f2, col_f3 = st.columns(3)
        
        with col_f1:
            estados_unicos = df['ESTADO_FINAL'].dropna().unique()
            filtro_estado = st.multiselect("Estado de Solicitud", options=estados_unicos, default=estados_unicos)
            
        with col_f2:
            canales_unicos = df['CANAL'].dropna().unique()
            filtro_canal = st.multiselect("Canal de Ingreso", options=canales_unicos, default=canales_unicos)
            
        with col_f3:
            agencias_unicas = df['AGENCIA'].dropna().unique()
            filtro_agencia = st.multiselect("Agencia", options=agencias_unicas, default=agencias_unicas)
            
        df_filtrado = df[
            (df['ESTADO_FINAL'].isin(filtro_estado)) & 
            (df['CANAL'].isin(filtro_canal)) & 
            (df['AGENCIA'].isin(filtro_agencia))
        ]
        
        st.markdown(f"*Mostrando **{len(df_filtrado):,}** registros según los filtros aplicados.*")
        st.dataframe(df_filtrado, use_container_width=True, height=400) # <-- CORREGIDO
        
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar datos filtrados (CSV)",
            data=csv,
            file_name='datos_filtrados.csv',
            mime='text/csv',
        )
        
else:
    st.info("👈 Ajusta los parámetros y presiona 'Ejecutar ETL' para comenzar el análisis.")