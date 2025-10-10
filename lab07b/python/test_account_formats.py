#!/usr/bin/env python3
"""
Test Different Snowflake Account Formats
Try various account format combinations to find the working one
"""

import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def test_account_formats():
    """Test different account format variations"""
    
    print("🧪 TESTING DIFFERENT SNOWFLAKE ACCOUNT FORMATS")
    print("=" * 60)
    
    base_user = os.getenv('SNOWFLAKE_USER')
    base_password = os.getenv('SNOWFLAKE_PASSWORD')
    base_role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    
    # Different account formats to try
    account_formats = [
        ("Full URL", "hwa72902.east-us-2.azure.snowflakecomputing.com"),
        ("Without domain", "hwa72902.east-us-2.azure"),
        ("Account only", "hwa72902"),
        ("With region only", "hwa72902.east-us-2"),
        ("Azure format", "hwa72902-east-us-2.azure")
    ]
    
    for format_name, account in account_formats:
        print(f"\n{len(account_formats) - account_formats.index((format_name, account))}. Testing {format_name}: {account}")
        print("-" * 50)
        
        try:
            conn = snowflake.connector.connect(
                account=account,
                user=base_user,
                password=base_password,
                role=base_role,
                login_timeout=15,
                network_timeout=15
            )
            
            print(f"✅ SUCCESS! Account format works: {account}")
            
            # Test basic query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")
            result = cursor.fetchone()
            
            print(f"   - User: {result[0]}")
            print(f"   - Account: {result[1]}")
            print(f"   - Region: {result[2]}")
            
            cursor.close()
            conn.close()
            
            print(f"\n🎉 WORKING FORMAT FOUND: {account}")
            print("Update your .env file with:")
            print(f"SNOWFLAKE_ACCOUNT={account}")
            
            return account
            
        except Exception as e:
            error_code = str(e).split(':')[0] if ':' in str(e) else str(e)
            print(f"❌ FAILED: {error_code}")
            if "250001" not in str(e):
                print(f"   Details: {str(e)[:100]}...")
    
    print(f"\n❌ None of the account formats worked.")
    print("This suggests there might be a different issue:")
    print("- Account credentials are incorrect")
    print("- Account is suspended or inactive") 
    print("- Network/firewall restrictions")
    print("- MFA is required")
    
    return None

if __name__ == "__main__":
    working_format = test_account_formats()
    
    if not working_format:
        print(f"\n💡 NEXT STEPS:")
        print("1. Verify credentials in Snowflake web console")
        print("2. Check with your Snowflake administrator")
        print("3. Confirm account status and permissions")
        print("4. Check if MFA is enabled on your account")