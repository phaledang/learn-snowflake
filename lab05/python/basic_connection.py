# Basic Snowflake Connection Example
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_connection():
    """Create and return Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        print("✅ Successfully connected to Snowflake!")
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