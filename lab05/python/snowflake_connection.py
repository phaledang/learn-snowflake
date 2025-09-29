#!/usr/bin/env python3
"""
Snowflake Connection Utility
Supports both individual parameters and connection string formats
"""
import os
from urllib.parse import urlparse, parse_qs, unquote_plus
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

class SnowflakeConnection:
    """Utility class for managing Snowflake connections"""
    
    def __init__(self):
        self._connection = None
        self._connection_params = self._parse_connection_config()
    
    def _parse_connection_config(self):
        """Parse connection configuration from environment variables"""
        # Check if connection string is provided
        conn_string = os.getenv('SNOWFLAKE_CONNECTION_STRING')
        
        if conn_string:
            return self._parse_connection_string(conn_string)
        else:
            return self._parse_individual_params()
    
    def _normalize_account_format(self, account):
        """
        Normalize account format to work with Snowflake connector.
        Handles both full domain and short format automatically.
        
        Examples:
        - hwa72902.east-us-2.azure.snowflakecomputing.com -> hwa72902.east-us-2.azure
        - hwa72902.east-us-2.azure -> hwa72902.east-us-2.azure (no change)
        """
        if not account:
            return account
            
        # Remove .snowflakecomputing.com suffix if present
        if account.endswith('.snowflakecomputing.com'):
            account = account.replace('.snowflakecomputing.com', '')
            
        return account
    
    def _parse_connection_string(self, conn_string):
        """Parse Snowflake connection string format"""
        # Parse: snowflake://user:password@account/database/schema?warehouse=wh&role=role
        parsed = urlparse(conn_string)
        query_params = parse_qs(parsed.query)
        
        # Normalize the account format (remove .snowflakecomputing.com if present)
        account = self._normalize_account_format(parsed.hostname)
        
        return {
            'account': account,
            'user': parsed.username,
            'password': unquote_plus(parsed.password) if parsed.password else None,
            'database': parsed.path.split('/')[1] if len(parsed.path.split('/')) > 1 else None,
            'schema': parsed.path.split('/')[2] if len(parsed.path.split('/')) > 2 else None,
            'warehouse': query_params.get('warehouse', [None])[0],
            'role': query_params.get('role', [None])[0],
            'login_timeout': 60,
            'network_timeout': 60
        }
    
    def _parse_individual_params(self):
        """Parse individual environment variables"""
        account = self._normalize_account_format(os.getenv('SNOWFLAKE_ACCOUNT'))
        
        return {
            'account': account,
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'login_timeout': 60,
            'network_timeout': 60
        }
    
    def get_connection(self):
        """Get or create a Snowflake connection with automatic fallback for account formats"""
        if self._connection is None:
            # Get base connection parameters
            params = {k: v for k, v in self._connection_params.items() if v is not None}
            base_account = params.get('account')
            
            # List of account formats to try
            account_formats = self._get_account_format_variations(base_account)
            
            connection_error = None
            for i, account_format in enumerate(account_formats):
                try:
                    params['account'] = account_format
                    print(f"Attempting connection with account format: {account_format}")
                    
                    self._connection = snowflake.connector.connect(**params)
                    print("✓ Snowflake connection established successfully")
                    
                    # Store the working account format for future reference
                    self._connection_params['account'] = account_format
                    break
                    
                except Exception as e:
                    connection_error = e
                    if i < len(account_formats) - 1:  # Not the last attempt
                        print(f"✗ Failed with {account_format}: {str(e)}")
                        continue
                    else:
                        # Last attempt failed, raise the error
                        raise Exception(f"Failed to connect to Snowflake with all account formats. Last error: {str(e)}")
        
        return self._connection
    
    def _get_account_format_variations(self, base_account):
        """Generate different account format variations to try"""
        if not base_account:
            return []
        
        variations = []
        
        # If it has .snowflakecomputing.com, try without it first (this usually works)
        if '.snowflakecomputing.com' in base_account:
            # Try without the full domain first
            short_format = base_account.replace('.snowflakecomputing.com', '')
            variations.append(short_format)
            # Then try with the full domain
            variations.append(base_account)
        else:
            # If it doesn't have the domain, try as-is first, then with domain
            variations.append(base_account)
            # Try adding the domain
            full_format = f"{base_account}.snowflakecomputing.com"
            variations.append(full_format)
        
        # Additional variations based on common patterns
        if '.' in base_account and not base_account.endswith('.snowflakecomputing.com'):
            # For formats like "hwa72902.east-us-2.azure"
            parts = base_account.split('.')
            if len(parts) >= 3:
                # Try just the account identifier
                variations.append(parts[0])
        
        # Remove duplicates while preserving order
        unique_variations = []
        for var in variations:
            if var and var not in unique_variations:
                unique_variations.append(var)
        
        return unique_variations
    
    def close_connection(self):
        """Close the connection if it exists"""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def get_connection_info(self):
        """Get connection parameters (for debugging)"""
        return {k: v if k != 'password' else '***' for k, v in self._connection_params.items()}

# Global connection instance
_connection_instance = None

def get_snowflake_connection():
    """Get a shared Snowflake connection instance"""
    global _connection_instance
    if _connection_instance is None:
        _connection_instance = SnowflakeConnection()
    return _connection_instance.get_connection()

def get_connection_info():
    """Get connection configuration info"""
    global _connection_instance
    if _connection_instance is None:
        _connection_instance = SnowflakeConnection()
    return _connection_instance.get_connection_info()

if __name__ == "__main__":
    # Test the connection
    try:
        print("Testing Snowflake connection...")
        print(f"Connection config: {get_connection_info()}")
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        
        print(f"✓ Connected successfully!")
        print(f"  User: {result[0]}")
        print(f"  Role: {result[1]}")
        print(f"  Database: {result[2]}")
        print(f"  Schema: {result[3]}")
        print(f"  Warehouse: {result[4]}")
        
        # Test employees query
        cursor.execute("SELECT COUNT(*) as employee_count FROM employees")
        count_result = cursor.fetchone()
        print(f"  Employee count: {count_result[0]}")
        
        cursor.close()
        
    except Exception as e:
        print(f"✗ Connection test failed: {e}")