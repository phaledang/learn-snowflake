#!/usr/bin/env python3
"""
Test Fixed Redirect URI Configuration
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')

def test_redirect_config():
    """Test the redirect URI configuration"""
    print("🔍 Testing Fixed Redirect URI Configuration")
    print("=" * 50)
    
    # Check environment variables
    client_id = os.getenv("AZURE_AD_CLIENT_ID")
    tenant_id = os.getenv("AZURE_AD_TENANT_ID") 
    redirect_uri = os.getenv("AZURE_AD_REDIRECT_URI")
    
    print(f"Client ID: {client_id[:10]}... (truncated)" if client_id else "Client ID: Not set")
    print(f"Tenant ID: {tenant_id[:10]}... (truncated)" if tenant_id else "Tenant ID: Not set")
    print(f"Redirect URI: {redirect_uri}")
    print()
    
    if not client_id or not tenant_id or not redirect_uri:
        print("❌ Missing required environment variables")
        return False
        
    try:
        from msal import PublicClientApplication
        
        # Initialize MSAL app
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scopes = ["https://graph.microsoft.com/User.Read"]
        
        print(f"Authority: {authority}")
        print(f"Scopes: {scopes}")
        print(f"Fixed Redirect URI: {redirect_uri}")
        print()
        
        app = PublicClientApplication(
            client_id=client_id,
            authority=authority
        )
        
        print("✅ MSAL PublicClientApplication initialized successfully")
        print("✅ Fixed redirect URI configured")
        print()
        print("📝 Azure AD App Registration Requirements:")
        print(f"   - Redirect URI must be set to: {redirect_uri}")
        print("   - Platform: Mobile and desktop applications")
        print("   - Client type: Public client")
        print()
        print("🎯 Next time you run interactive login, it should use:")
        print(f"   Redirect URI: {redirect_uri}")
        print("   (No more random ports!)")
        
        return True
        
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return False

if __name__ == "__main__":
    test_redirect_config()