#!/usr/bin/env python3
"""
Simple test to check the employees table structure
"""
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_connection_info

load_dotenv()

def check_table_structure():
    """Check what columns exist in the employees table"""
    try:
        from sqlalchemy import create_engine, text
        from urllib.parse import urlparse
        
        # Get connection string
        conn_string = os.getenv('SNOWFLAKE_CONNECTION_STRING')
        
        if conn_string:
            parsed = urlparse(conn_string)
            account = parsed.hostname
            if account and account.endswith('.snowflakecomputing.com'):
                account = account.replace('.snowflakecomputing.com', '')
            
            user = parsed.username
            password = parsed.password
            database = parsed.path.split('/')[1] if len(parsed.path.split('/')) > 1 else None
            schema = parsed.path.split('/')[2] if len(parsed.path.split('/')) > 2 else None
            query = parsed.query
            
            connection_url = f"snowflake://{user}:{password}@{account}/{database}/{schema}"
            if query:
                connection_url += f"?{query}"
        
        print("🔍 Checking employees table structure...")
        engine = create_engine(connection_url)
        
        # Check table structure
        with engine.connect() as conn:
            # First, let's see what tables exist
            result = conn.execute(text("SHOW TABLES"))
            tables = result.fetchall()
            print(f"\n📋 Available tables:")
            for table in tables:
                print(f"   - {table[1]}")  # table name is usually in second column
            
            # Try to describe the employees table
            try:
                result = conn.execute(text("DESCRIBE TABLE employees"))
                columns = result.fetchall()
                print(f"\n📊 EMPLOYEES table structure:")
                for column in columns:
                    print(f"   - {column[0]} ({column[1]})")  # name and type
            except Exception as e:
                print(f"\n❌ Could not describe employees table: {e}")
                
            # Try a simple select to see first row
            try:
                result = conn.execute(text("SELECT * FROM employees LIMIT 1"))
                first_row = result.fetchone()
                if first_row:
                    column_names = result.keys()
                    print(f"\n🎯 First row column names:")
                    for i, col_name in enumerate(column_names):
                        print(f"   - {col_name}: {first_row[i]}")
            except Exception as e:
                print(f"\n❌ Could not select from employees: {e}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_table_structure()