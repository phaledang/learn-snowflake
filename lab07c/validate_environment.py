#!/usr/bin/env python3
"""
Lab 07 Environment Validation Script
Tests that all required packages can be imported successfully.
"""

import sys

def test_imports():
    """Test importing key packages for Lab 07."""
    results = {}
    packages_to_test = [
        # Core Python data packages
        ('pandas', 'import pandas'),
        ('numpy', 'import numpy'),
        
        # Snowflake connectivity
        ('snowflake-connector-python', 'import snowflake.connector'),
        
        # LangChain framework
        ('langchain', 'import langchain'),
        ('langchain-core', 'from langchain_core.messages import HumanMessage'),
        ('langchain-openai', 'from langchain_openai import ChatOpenAI'),
        ('langchain-community', 'import langchain_community'),
        
        # OpenAI
        ('openai', 'import openai'),
        
        # Environment management
        ('python-dotenv', 'from dotenv import load_dotenv'),
        
        # File processing
        ('openpyxl', 'import openpyxl'),
        ('pypdf2', 'import PyPDF2'),
        ('python-docx', 'import docx'),
        
        # Web framework
        ('streamlit', 'import streamlit'),
        ('flask', 'import flask'),
        
        # Utilities
        ('requests', 'import requests'),
        ('sqlparse', 'import sqlparse'),
        ('tiktoken', 'import tiktoken'),
    ]
    
    print("🧪 Testing Lab 07 Environment...")
    print("=" * 50)
    
    success_count = 0
    for package_name, import_statement in packages_to_test:
        try:
            exec(import_statement)
            print(f"✅ {package_name}: OK")
            results[package_name] = True
            success_count += 1
        except ImportError as e:
            print(f"❌ {package_name}: FAILED - {e}")
            results[package_name] = False
        except Exception as e:
            print(f"⚠️  {package_name}: ERROR - {e}")
            results[package_name] = False
    
    print("=" * 50)
    print(f"📊 Results: {success_count}/{len(packages_to_test)} packages imported successfully")
    
    if success_count == len(packages_to_test):
        print("🎉 Environment setup is complete and ready for Lab 07!")
        return True
    else:
        print("⚠️  Some packages failed to import. Check your installation.")
        print("💡 Try: pip install -r python/requirements.txt")
        return False

def print_environment_info():
    """Print Python environment information."""
    print("\n🐍 Python Environment Information:")
    print(f"   Python Version: {sys.version}")
    print(f"   Python Path: {sys.executable}")
    
    # Check if we're in a virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("   Virtual Environment: ✅ Active")
    else:
        print("   Virtual Environment: ❌ Not detected")

if __name__ == "__main__":
    print_environment_info()
    print()
    success = test_imports()
    
    if not success:
        sys.exit(1)
    
    print("\n🚀 Ready to start Lab 07!")
    print("   Try: python python/demo_cli.py")
    print("   Or:  streamlit run python/streamlit_app.py")