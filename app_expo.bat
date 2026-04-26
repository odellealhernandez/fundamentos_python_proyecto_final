@echo off
title Lanzador Dashboard - Expo Movil 2026
echo ==========================================
echo Iniciando Servidor Web del ETL...
echo Por favor, espera a que se abra el navegador.
echo ==========================================

:: Cambiar a la carpeta donde esta el script
cd /d "%~dp0"

:: Lanzar Streamlit
streamlit run app_expo.py

pause