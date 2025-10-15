#!/usr/bin/env python3
"""
Debug Azure AD Authentication
"""

import os
import sys
from dotenv import load_dotenv

print("🔍 Environment Loading Debug")
print("=" * 50)

# Check current working directory
print(f"Current working directory: {os.getcwd()}")

# Try loading environment variables with different paths
env_paths = [
    '../.env',
    '.env',
    os.path.join(os.path.dirname(__file__), '..', '.env'),
    os.path.join(os.path.dirname(__file__), '.env')
]

for env_path in env_paths:
    print(f"Trying to load: {env_path}")
    if os.path.exists(env_path):
        print(f"  ✅ File exists")
        result = load_dotenv(env_path)
        print(f"  Load result: {result}")
    else:
        print(f"  ❌ File not found")

print("\n🔍 Azure AD Configuration")
print("=" * 50)

# Check environment variables
client_id = os.getenv("AZURE_AD_CLIENT_ID", "")
tenant_id = os.getenv("AZURE_AD_TENANT_ID", "")
redirect_uri = os.getenv("AZURE_AD_REDIRECT_URI", "")

print(f"AZURE_AD_CLIENT_ID: {'✅ Set' if client_id else '❌ Not set'}")
print(f"AZURE_AD_TENANT_ID: {'✅ Set' if tenant_id else '❌ Not set'}")
print(f"AZURE_AD_REDIRECT_URI: {'✅ Set' if redirect_uri else '❌ Not set'}")

if client_id:
    print(f"Client ID: {client_id[:8]}...")
if tenant_id:
    print(f"Tenant ID: {tenant_id[:8]}...")
if redirect_uri:
    print(f"Redirect URI: {redirect_uri}")

print("\n🔧 Testing MSAL Configuration")
print("=" * 50)

try:
    from msal import PublicClientApplication
    
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://graph.microsoft.com/User.Read"]
    
    print(f"Authority: {authority}")
    print(f"Scopes: {scopes}")
    
    # Create MSAL app
    app = PublicClientApplication(
        client_id=client_id,
        authority=authority
    )
    
    print("✅ MSAL PublicClientApplication created successfully")
    
    # Try to get accounts
    accounts = app.get_accounts()
    print(f"Cached accounts: {len(accounts)}")
    
    if accounts:
        for i, account in enumerate(accounts):
            print(f"  Account {i+1}: {account.get('username', 'Unknown')}")
    
    print("\n🌐 Testing Interactive Authentication")
    print("=" * 50)
    
    # Try interactive authentication
    print("Attempting acquire_token_interactive...")
    result = app.acquire_token_interactive(
        scopes=scopes,
        prompt="select_account"
    )
    
    if result:
        if "access_token" in result:
            print("✅ Authentication successful!")
            print(f"Token type: {result.get('token_type', 'Unknown')}")
            print(f"Expires in: {result.get('expires_in', 'Unknown')} seconds")
            if 'account' in result:
                account = result['account']
                print(f"Account: {account.get('username', 'Unknown')}")
        else:
            print("❌ Authentication failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
            print(f"Error description: {result.get('error_description', 'No description')}")
            if 'error_codes' in result:
                print(f"Error codes: {result['error_codes']}")
    else:
        print("❌ No result returned from authentication")
        
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please install required packages: pip install msal azure-identity")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()