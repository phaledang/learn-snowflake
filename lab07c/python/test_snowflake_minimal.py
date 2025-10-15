#!/usr/bin/env python3
"""
Simple Snowflake Credential Test
Test Snowflake credentials with minimal configuration
"""

import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def test_minimal_connection():
    """Test Snowflake connection with minimal settings"""
    
    print("🔧 MINIMAL SNOWFLAKE CONNECTION TEST")
    print("=" * 50)
    
    # Test with just account, user, password - no warehouse/database
    try:
        print("Attempting connection with minimal settings...")
        print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
        print(f"User: {os.getenv('SNOWFLAKE_USER')}")
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role=os.getenv('SNOWFLAKE_ROLE', 'PUBLIC'),  # Default to PUBLIC role if not specified
            # Don't specify warehouse, database, schema initially
            login_timeout=30,  # Increase timeout
            network_timeout=30
        )
        
        print("✅ Basic authentication successful!")
        
        cursor = conn.cursor()
        
        # Test basic queries
        print("\nTesting basic queries...")
        
        cursor.execute("SELECT CURRENT_USER()")
        user = cursor.fetchone()[0]
        print(f"✅ Current user: {user}")
        
        cursor.execute("SELECT CURRENT_ACCOUNT()")
        account = cursor.fetchone()[0]
        print(f"✅ Current account: {account}")
        
        cursor.execute("SELECT CURRENT_REGION()")
        region = cursor.fetchone()[0]
        print(f"✅ Current region: {region}")
        
        # List available warehouses
        print("\nChecking available warehouses...")
        cursor.execute("SHOW WAREHOUSES")
        warehouses = cursor.fetchall()
        
        if warehouses:
            print(f"✅ Found {len(warehouses)} warehouses:")
            for wh in warehouses:
                print(f"   - {wh[0]} (State: {wh[1]})")
        else:
            print("❌ No warehouses found")
        
        # Test warehouse access
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        print(f"\nTesting warehouse access: {warehouse}")
        try:
            cursor.execute(f"USE WAREHOUSE {warehouse}")
            print(f"✅ Successfully switched to warehouse: {warehouse}")
            
            # Test database access
            database = os.getenv('SNOWFLAKE_DATABASE')
            print(f"Testing database access: {database}")
            cursor.execute(f"USE DATABASE {database}")
            print(f"✅ Successfully switched to database: {database}")
            
            # Test schema access
            schema = os.getenv('SNOWFLAKE_SCHEMA')
            print(f"Testing schema access: {schema}")
            cursor.execute(f"USE SCHEMA {schema}")
            print(f"✅ Successfully switched to schema: {schema}")
            
            # List tables in the schema
            print("\nListing tables in current schema...")
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            if tables:
                print(f"✅ Found {len(tables)} tables:")
                for table in tables[:10]:  # Show first 10 tables
                    print(f"   - {table[1]} (Rows: {table[4] if len(table) > 4 else 'Unknown'})")
            else:
                print("❌ No tables found in current schema")
                print("   This might be why the AI assistant can't find employee data")
                
        except Exception as e:
            print(f"❌ Error accessing resources: {e}")
            
        cursor.close()
        conn.close()
        
        print("\n✅ CONNECTION TEST COMPLETED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        print(f"\n❌ CONNECTION FAILED: {e}")
        
        error_code = str(e).split(':')[0] if ':' in str(e) else str(e)
        print(f"Error code: {error_code}")
        
        if "250001" in str(e):
            print("\n💡 Error 250001 typically means:")
            print("   - Account identifier is wrong")
            print("   - Network connectivity issues") 
            print("   - Snowflake service problems")
            
        elif "250003" in str(e) or "authentication" in str(e).lower():
            print("\n💡 Authentication error typically means:")
            print("   - Wrong username or password")
            print("   - Account locked or disabled")
            print("   - User doesn't have access to this account")
            
        return False

def provide_next_steps():
    """Provide next steps based on test results"""
    
    print("\n" + "=" * 50)
    print("📋 RECOMMENDATIONS")
    print("=" * 50)
    
    print("\n1. Test in Snowflake Web Console:")
    print(f"   - Go to: https://app.snowflake.com/")
    print(f"   - Login with account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(f"   - Username: {os.getenv('SNOWFLAKE_USER')}")
    print("   - Use the same password")
    
    print("\n2. If web login works, the issue might be:")
    print("   - Python connector version compatibility")
    print("   - MFA requirements (if enabled)")
    print("   - IP restrictions")
    
    print("\n3. If web login doesn't work:")
    print("   - Verify account URL with your admin")
    print("   - Check if account is suspended")
    print("   - Confirm username/password")
    
    print("\n4. Alternative account formats to try:")
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    if '.snowflakecomputing.com' in account:
        base_account = account.replace('.snowflakecomputing.com', '')
        print(f"   - Try: SNOWFLAKE_ACCOUNT={base_account}")
        
        # Try just the identifier part
        account_parts = base_account.split('.')
        if len(account_parts) > 1:
            print(f"   - Or try: SNOWFLAKE_ACCOUNT={account_parts[0]}")

if __name__ == "__main__":
    success = test_minimal_connection()
    provide_next_steps()