#!/usr/bin/env python3
"""
Trading Bot Dashboard - Entry Point

Run this file to start the dashboard:
    python run_dashboard.py

The dashboard will be available at http://localhost:8050
"""
import sys
import os

# Ensure we can import from the project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dashboard.app import app

if __name__ == '__main__':
    print('=' * 50)
    print('  Trading Bot Dashboard')
    print('  Open http://localhost:8050 in your browser')
    print('=' * 50)
    app.run(debug=True, host='0.0.0.0', port=8050)
