#!/usr/bin/env python3
"""
Debug Snowflake Connection Issue
Tests both direct connector and SQLAlchemy approaches
"""
import os
import sys
from dotenv import load_dotenv
from urllib.parse import urlparse

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_connection_info

load_dotenv()

def debug_connection_config():
    """Debug connection configuration"""
    print("🔍 DEBUGGING SNOWFLAKE CONNECTION")
    print("=" * 50)
    
    # Check environment variables
    print("1. Environment Variables:")
    conn_string = os.getenv('SNOWFLAKE_CONNECTION_STRING')
    if conn_string:
        print(f"   SNOWFLAKE_CONNECTION_STRING: Found ({len(conn_string)} chars)")
        # Parse and show components
        parsed = urlparse(conn_string)
        print(f"   - Protocol: {parsed.scheme}")
        print(f"   - Username: {parsed.username}")
        print(f"   - Password: {'*' * len(parsed.password) if parsed.password else 'None'}")
        print(f"   - Hostname: {parsed.hostname}")
        print(f"   - Path: {parsed.path}")
        print(f"   - Query: {parsed.query}")
    else:
        print("   SNOWFLAKE_CONNECTION_STRING: Not found")
        print(f"   SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
        print(f"   SNOWFLAKE_USER: {os.getenv('SNOWFLAKE_USER')}")
        print(f"   SNOWFLAKE_PASSWORD: {'*' * len(os.getenv('SNOWFLAKE_PASSWORD', ''))}")
    
    print("\n2. Connection Utility Configuration:")
    try:
        config = get_connection_info()
        for key, value in config.items():
            if key == 'password':
                print(f"   {key}: {'*' * len(str(value)) if value else 'None'}")
            else:
                print(f"   {key}: {value}")
    except Exception as e:
        print(f"   Error getting config: {e}")
    
    print("\n3. Testing Direct Snowflake Connector:")
    try:
        import snowflake.connector
        # Try to create a connection with the config
        config = get_connection_info()
        # Remove None values
        clean_config = {k: v for k, v in config.items() if v is not None}
        print(f"   Attempting connection with: {list(clean_config.keys())}")
        
        conn = snowflake.connector.connect(**clean_config)
        print("   ✅ Direct connector: SUCCESS")
        conn.close()
    except Exception as e:
        print(f"   ❌ Direct connector failed: {e}")
    
    print("\n4. Testing SQLAlchemy Engine:")
    try:
        from sqlalchemy import create_engine
        from urllib.parse import quote_plus
        
        config = get_connection_info()
        
        # Build connection URL similar to sqlalchemy_integration.py
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
        else:
            account = config.get('account')
            user = config.get('user')
            password = config.get('password')
            warehouse = config.get('warehouse')
            database = config.get('database')
            schema = config.get('schema')
            role = config.get('role')
            
            encoded_password = quote_plus(password) if password else ''
            connection_url = f"snowflake://{user}:{encoded_password}@{account}/{database}/{schema}"
            if warehouse:
                connection_url += f"?warehouse={warehouse}"
            if role:
                separator = '&' if '?' in connection_url else '?'
                connection_url += f"{separator}role={role}"
        
        print(f"   Connection URL: snowflake://{user}:***@{account}/{database}/{schema}?...")
        engine = create_engine(connection_url)
        
        # Test the engine with proper SQLAlchemy syntax
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT CURRENT_VERSION()"))
            version = result.fetchone()[0]
            print(f"   ✅ SQLAlchemy engine: SUCCESS (Snowflake {version})")
            
    except Exception as e:
        print(f"   ❌ SQLAlchemy engine failed: {e}")

if __name__ == "__main__":
    debug_connection_config()