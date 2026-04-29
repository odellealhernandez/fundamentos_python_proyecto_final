@echo off
title Ejecutador de ETL - Expo Movil 2026
echo ==========================================
echo Iniciando Proceso ETL de Banca...
echo ==========================================

:: 1. Cambiar a la carpeta donde esta el script
:: %~dp0 asegura que funcione aunque lo ejecutes como admin
cd /d "%~dp0"

:: 2. Ejecutar el script de Python
:: Si usas un entorno virtual, cambia 'python' por la ruta al python.exe del venv
python main.py

echo ==========================================
echo Proceso finalizado.
pause