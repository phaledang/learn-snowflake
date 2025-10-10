#!/usr/bin/env python3
"""
Simple test for the connection string configuration
"""
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_snowflake_connection, get_connection_info

def test_employees_query():
    """Test querying the employees table"""
    try:
        print("=== Connection String Test ===")
        print(f"Connection config: {get_connection_info()}")
        
        print("\nConnecting to Snowflake...")
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        print("Querying employees table...")
        cursor.execute("SELECT ID, NAME, DEPARTMENT, SALARY FROM EMPLOYEES LIMIT 5")
        results = cursor.fetchall()
        
        print(f"\nEmployee List ({len(results)} rows):")
        print("ID | Name | Department | Salary")
        print("-" * 50)
        for row in results:
            print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_employees_query()
    print(f"\nTest {'PASSED' if success else 'FAILED'}")