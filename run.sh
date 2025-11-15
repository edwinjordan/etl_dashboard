#!/bin/bash
# Quick start script for ETL Dashboard

echo "ETL Dashboard - Quick Start"
echo "============================"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate 2>/dev/null || source venv/Scripts/activate

echo "Installing dependencies..."
pip install -q -r requirements.txt

echo ""
echo "Starting ETL Dashboard..."
echo "Dashboard will open at http://localhost:8501"
echo ""
streamlit run dashboard.py
