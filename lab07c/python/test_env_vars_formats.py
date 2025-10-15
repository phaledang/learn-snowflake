#!/usr/bin/env python3
"""
Test individual environment variables with different account formats
"""
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import SnowflakeConnection

def test_env_vars_format(account_format, description):
    """Test individual environment variables with a specific account format"""
    print(f"\n=== Testing {description} ===")
    print(f"Account format: {account_format}")
    
    # Temporarily modify environment variables
    original_account = os.getenv('SNOWFLAKE_ACCOUNT')
    original_conn_string = os.getenv('SNOWFLAKE_CONNECTION_STRING')
    
    try:
        # Remove connection string to force use of individual params
        if 'SNOWFLAKE_CONNECTION_STRING' in os.environ:
            del os.environ['SNOWFLAKE_CONNECTION_STRING']
        
        # Set the account format to test
        os.environ['SNOWFLAKE_ACCOUNT'] = account_format
        
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
        
        cursor.close()
        conn_instance.close_connection()
        
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False
        
    finally:
        # Restore original values
        if original_account:
            os.environ['SNOWFLAKE_ACCOUNT'] = original_account
        if original_conn_string:
            os.environ['SNOWFLAKE_CONNECTION_STRING'] = original_conn_string

def main():
    """Test individual environment variables with different account formats"""
    load_dotenv()
    
    # Test cases - both account formats should work
    test_cases = [
        {
            "account_format": "hwa72902.east-us-2.azure",
            "description": "Individual Env Vars - Short Account Format"
        },
        {
            "account_format": "hwa72902.east-us-2.azure.snowflakecomputing.com",
            "description": "Individual Env Vars - Full Account Format"
        }
    ]
    
    print("Testing Individual Environment Variables Compatibility")
    print("=" * 60)
    
    results = []
    for test_case in test_cases:
        success = test_env_vars_format(
            test_case["account_format"], 
            test_case["description"]
        )
        results.append((test_case["description"], success))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for description, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{description}: {status}")
        if not success:
            all_passed = False
    
    print(f"\nOverall Result: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")

if __name__ == "__main__":
    main()