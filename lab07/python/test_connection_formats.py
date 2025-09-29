#!/usr/bin/env python3
"""
Test both connection string formats to ensure compatibility
"""
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import SnowflakeConnection

def test_connection_format(connection_string, description):
    """Test a specific connection string format"""
    print(f"\n=== Testing {description} ===")
    print(f"Connection string: {connection_string}")
    
    # Temporarily set the connection string
    original_conn_string = os.getenv('SNOWFLAKE_CONNECTION_STRING')
    os.environ['SNOWFLAKE_CONNECTION_STRING'] = connection_string
    
    try:
        # Create a new connection instance
        conn_instance = SnowflakeConnection()
        
        print(f"Parsed config: {conn_instance.get_connection_info()}")
        
        # Test the connection
        conn = conn_instance.get_connection()
        cursor = conn.cursor()
        
        # Test a simple query
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT_NAME()")
        result = cursor.fetchone()
        print(f"✓ Connected successfully as {result[0]} on account {result[1]}")
        
        # Test employee query
        cursor.execute("SELECT COUNT(*) FROM EMPLOYEES")
        count = cursor.fetchone()[0]
        print(f"✓ Employee count: {count}")
        
        cursor.close()
        conn_instance.close_connection()
        
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False
        
    finally:
        # Restore original connection string
        if original_conn_string:
            os.environ['SNOWFLAKE_CONNECTION_STRING'] = original_conn_string
        elif 'SNOWFLAKE_CONNECTION_STRING' in os.environ:
            del os.environ['SNOWFLAKE_CONNECTION_STRING']

def main():
    """Test both connection string formats"""
    load_dotenv()
    
    # Get credentials from environment
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')
    
    # URL encode password for connection string
    from urllib.parse import quote_plus
    encoded_password = quote_plus(password)
    
    # Test cases - both formats should work
    test_cases = [
        {
            "connection_string": f"snowflake://{user}:{encoded_password}@hwa72902.east-us-2.azure/{database}/{schema}?warehouse={warehouse}&role={role}",
            "description": "Short Account Format (without .snowflakecomputing.com)"
        },
        {
            "connection_string": f"snowflake://{user}:{encoded_password}@hwa72902.east-us-2.azure.snowflakecomputing.com/{database}/{schema}?warehouse={warehouse}&role={role}",
            "description": "Full Account Format (with .snowflakecomputing.com)"
        }
    ]
    
    print("Testing Connection String Compatibility")
    print("=" * 50)
    
    results = []
    for test_case in test_cases:
        success = test_connection_format(
            test_case["connection_string"], 
            test_case["description"]
        )
        results.append((test_case["description"], success))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    all_passed = True
    for description, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{description}: {status}")
        if not success:
            all_passed = False
    
    print(f"\nOverall Result: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    print("\nBoth connection string formats should now work seamlessly!")

if __name__ == "__main__":
    main()