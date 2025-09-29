# Basic Snowflake Connection Example
import snowflake.connector
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_snowflake_connection, get_connection_info

# Load environment variables
load_dotenv()

def create_connection():
    """Create and return Snowflake connection using connection string utility"""
    try:
        print("Creating Snowflake connection...")
        print(f"Connection config: {get_connection_info()}")
        
        conn = get_snowflake_connection()
        print("✅ Successfully connected to Snowflake using connection utility!")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        return None

def test_connection():
    """Test basic connection and query"""
    conn = create_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Test query
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            
            print(f"Database: {result[0]}")
            print(f"Schema: {result[1]}")
            print(f"Warehouse: {result[2]}")
            
            # Test data query
            cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = CURRENT_SCHEMA()")
            table_count = cursor.fetchone()[0]
            print(f"Tables in current schema: {table_count}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error executing query: {e}")

if __name__ == "__main__":
    test_connection()