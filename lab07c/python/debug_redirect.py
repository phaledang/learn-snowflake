#!/usr/bin/env python3
"""
Debug Authentication with Fixed Redirect URI
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../.env')

# Add the python directory to the Python path
sys.path.append(os.path.dirname(__file__))

def debug_auth():
    """Debug the authentication process"""
    print("🔍 Debug: Azure AD Authentication with Fixed Redirect URI")
    print("=" * 60)
    
    # Check environment variables
    client_id = os.getenv("AZURE_AD_CLIENT_ID")
    tenant_id = os.getenv("AZURE_AD_TENANT_ID") 
    redirect_uri = os.getenv("AZURE_AD_REDIRECT_URI")
    
    print(f"Client ID: {client_id[:10]}... (truncated)" if client_id else "Client ID: Not set")
    print(f"Tenant ID: {tenant_id[:10]}... (truncated)" if tenant_id else "Tenant ID: Not set")
    print(f"Redirect URI: {redirect_uri}")
    print()
    
    if not client_id:
        print("❌ AZURE_AD_CLIENT_ID not found in environment variables")
        return
        
    try:
        from msal import PublicClientApplication
        
        # Initialize MSAL app
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scopes = ["https://graph.microsoft.com/User.Read"]
        
        print(f"Authority: {authority}")
        print(f"Scopes: {scopes}")
        print(f"Redirect URI: {redirect_uri}")
        print()
        
        app = PublicClientApplication(
            client_id=client_id,
            authority=authority
        )
        
        print("✅ MSAL PublicClientApplication initialized successfully")
        print("🌐 Attempting interactive authentication...")
        print(f"📍 This should use redirect URI: {redirect_uri}")
        print()
        
        # Try interactive authentication
        result = app.acquire_token_interactive(
            scopes=scopes,
            redirect_uri=redirect_uri,
            prompt="select_account"
        )
        
        if result and "access_token" in result:
            print("✅ Authentication successful!")
            print(f"User: {result.get('account', {}).get('username', 'Unknown')}")
            return True
        else:
            print("❌ Authentication failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
            print(f"Error Description: {result.get('error_description', 'No description')}")
            return False
            
    except Exception as e:
        print(f"❌ Exception during authentication: {str(e)}")
        return False

if __name__ == "__main__":
    debug_auth()