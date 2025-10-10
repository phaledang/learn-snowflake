#!/usr/bin/env python3
"""
Snowflake Connection Diagnostic Tool
This script helps diagnose Snowflake connection issues
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def diagnose_snowflake_connection():
    """Diagnose Snowflake connection issues"""
    
    print("=" * 60)
    print("🔍 SNOWFLAKE CONNECTION DIAGNOSIS")
    print("=" * 60)
    
    # Check environment variables
    print("\n1. Environment Variables Check:")
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER', 
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ]
    
    all_set = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Show account info but mask password
            if var == 'SNOWFLAKE_PASSWORD':
                display_value = '*' * len(value)
            else:
                display_value = value
            print(f"   ✅ {var}: {display_value}")
        else:
            print(f"   ❌ {var}: NOT SET")
            all_set = False
    
    if not all_set:
        print("\n❌ Missing environment variables. Check your .env file.")
        return False
    
    # Analyze account URL format
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    print(f"\n2. Account URL Analysis:")
    print(f"   Account: {account}")
    
    if '.snowflakecomputing.com' in account:
        print("   ✅ Account URL format looks correct")
    else:
        print("   ⚠️  Account format might be incorrect")
        print("   Expected format: account.region.cloud.snowflakecomputing.com")
        print("   Example: ab12345.us-east-1.snowflakecomputing.com")
    
    # Test basic connection
    print(f"\n3. Testing Basic Connection...")
    try:
        import snowflake.connector
        
        # Test with minimal timeout first
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            login_timeout=10,  # Short timeout for testing
            network_timeout=10
        )
        
        print("   ✅ Initial connection successful!")
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"   ✅ Snowflake version: {version}")
        
        # Test warehouse access
        cursor.execute("SELECT CURRENT_WAREHOUSE()")
        warehouse = cursor.fetchone()[0]
        print(f"   ✅ Current warehouse: {warehouse}")
        
        # Test database access
        cursor.execute("SELECT CURRENT_DATABASE()")
        database = cursor.fetchone()[0]
        print(f"   ✅ Current database: {database}")
        
        # Test schema access
        cursor.execute("SELECT CURRENT_SCHEMA()")
        schema = cursor.fetchone()[0]
        print(f"   ✅ Current schema: {schema}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ SUCCESS: Snowflake connection is working!")
        return True
        
    except Exception as e:
        print(f"\n❌ CONNECTION FAILED: {e}")
        
        # Provide specific troubleshooting
        error_msg = str(e).lower()
        
        if "250001" in str(e) or "backend" in error_msg:
            print("\n💡 TROUBLESHOOTING: Backend Connection Error")
            print("   This usually means:")
            print("   1. Account URL is incorrect")
            print("   2. Network/firewall blocking connection")
            print("   3. Snowflake service is unreachable")
            print()
            print("   Solutions:")
            print("   - Verify account URL format")
            print("   - Check if you can access Snowflake web UI")
            print("   - Try from different network")
            
        elif "authentication" in error_msg or "login" in error_msg:
            print("\n💡 TROUBLESHOOTING: Authentication Error")
            print("   - Check username and password")
            print("   - Verify account permissions")
            print("   - Check for special characters in password")
            
        elif "warehouse" in error_msg:
            print("\n💡 TROUBLESHOOTING: Warehouse Error")
            print("   - Check warehouse name spelling")
            print("   - Verify warehouse is running")
            print("   - Check access permissions")
            
        elif "database" in error_msg or "schema" in error_msg:
            print("\n💡 TROUBLESHOOTING: Database/Schema Error")
            print("   - Check database and schema names")
            print("   - Verify access permissions")
            print("   - Try with default PUBLIC schema")
        
        return False

def test_web_connectivity():
    """Test if we can reach Snowflake over the internet"""
    
    print("\n4. Network Connectivity Test:")
    
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    if not account:
        print("   ❌ No account to test")
        return False
    
    import socket
    import urllib.parse
    
    # Extract hostname from account URL
    if 'snowflakecomputing.com' in account:
        hostname = account
    else:
        hostname = f"{account}.snowflakecomputing.com"
    
    print(f"   Testing connection to: {hostname}")
    
    try:
        # Test DNS resolution
        ip = socket.gethostbyname(hostname)
        print(f"   ✅ DNS resolution: {hostname} -> {ip}")
        
        # Test port connectivity
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((hostname, 443))
        sock.close()
        
        if result == 0:
            print("   ✅ Port 443 (HTTPS) is reachable")
            return True
        else:
            print("   ❌ Port 443 (HTTPS) is not reachable")
            print("   This might indicate firewall or network issues")
            return False
            
    except Exception as e:
        print(f"   ❌ Network test failed: {e}")
        return False

if __name__ == "__main__":
    print("Diagnosing Snowflake connection issues...")
    
    connection_ok = diagnose_snowflake_connection()
    network_ok = test_web_connectivity() if not connection_ok else True
    
    print("\n" + "=" * 60)
    print("📋 DIAGNOSIS SUMMARY")
    print("=" * 60)
    
    if connection_ok:
        print("✅ Snowflake connection is working perfectly!")
        print("You can proceed to test the AI assistant.")
    else:
        print("❌ Snowflake connection issues detected.")
        print()
        print("Next steps:")
        print("1. Verify your credentials in Snowflake web console")
        print("2. Check account URL format")
        print("3. Test from different network if needed")
        print("4. Contact your Snowflake administrator if issues persist")