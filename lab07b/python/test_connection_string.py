#!/usr/bin/env python3
"""
Test Snowflake connection using connection string format
"""
import os
import sys
from urllib.parse import quote_plus
from dotenv import load_dotenv
import snowflake.connector

def main():
    # Load environment variables
    load_dotenv()
    
    # Get credentials from environment
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')
    
    print("Testing Snowflake connection using connection string...")
    print(f"Account: {account}")
    print(f"User: {user}")
    print(f"Database: {database}")
    print(f"Schema: {schema}")
    print(f"Warehouse: {warehouse}")
    print(f"Role: {role}")
    
    # URL encode the password
    encoded_password = quote_plus(password)
    print(f"URL encoded password: {encoded_password}")
    
    # Build connection string
    connection_string = f"snowflake://{user}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
    
    print(f"\nConnection string: {connection_string}")
    
    try:
        print("\nAttempting to connect using connection string...")
        
        # Test with snowflake.connector using connection string
        # Note: snowflake.connector doesn't directly support connection strings
        # But we can parse it and use the components
        
        # Parse components for traditional connection
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
            # Additional parameters that might help
            login_timeout=60,
            network_timeout=60
        )
        
        print("✓ Connection successful!")
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        
        print(f"Current User: {result[0]}")
        print(f"Current Role: {result[1]}")
        print(f"Current Database: {result[2]}")
        print(f"Current Schema: {result[3]}")
        print(f"Current Warehouse: {result[4]}")
        
        # Test querying employees table
        print("\nTesting employee query...")
        cursor.execute("SELECT COUNT(*) as employee_count FROM employees")
        count_result = cursor.fetchone()
        print(f"Employee count: {count_result[0]}")
        
        cursor.close()
        conn.close()
        
        print("\n✓ All tests passed!")
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # Try with different account formats if the main one fails
        if "250001" in str(e):
            print("\nTrying alternative account formats...")
            
            alternative_accounts = [
                "hwa72902",  # Just the account identifier
                "hwa72902.east-us-2.azure",  # Without snowflakecomputing.com
                account.replace(".azure.snowflakecomputing.com", ""),  # Strip domain
            ]
            
            for alt_account in alternative_accounts:
                try:
                    print(f"Trying account format: {alt_account}")
                    conn = snowflake.connector.connect(
                        account=alt_account,
                        user=user,
                        password=password,
                        database=database,
                        schema=schema,
                        warehouse=warehouse,
                        role=role,
                        login_timeout=60,
                        network_timeout=60
                    )
                    print(f"✓ Success with account format: {alt_account}")
                    
                    # Quick test
                    cursor = conn.cursor()
                    cursor.execute("SELECT CURRENT_USER()")
                    result = cursor.fetchone()
                    print(f"Connected as: {result[0]}")
                    cursor.close()
                    conn.close()
                    
                    # Update the working account format
                    print(f"\n🎉 Working account format found: {alt_account}")
                    print(f"Connection string format: snowflake://{user}:{encoded_password}@{alt_account}/{database}/{schema}?warehouse={warehouse}&role={role}")
                    return True
                    
                except Exception as alt_e:
                    print(f"Failed with {alt_account}: {alt_e}")
                    continue
        
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)