@echo off
REM Quick start script for ETL Dashboard (Windows)

echo ETL Dashboard - Quick Start
echo ============================
echo.

REM Check if virtual environment exists
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo Installing dependencies...
pip install -q -r requirements.txt

echo.
echo Starting ETL Dashboard...
echo Dashboard will open at http://localhost:8501
echo.
streamlit run dashboard.py
