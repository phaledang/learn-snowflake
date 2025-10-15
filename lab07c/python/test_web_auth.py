#!/usr/bin/env python3
"""
Test Web Server Authentication
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')

# Add the python directory to the Python path
sys.path.append(os.path.dirname(__file__))

def test_web_auth():
    """Test the web server authentication"""
    print("🧪 Testing Web Server Authentication")
    print("=" * 50)
    
    # Check environment variables
    client_id = os.getenv("AZURE_AD_CLIENT_ID")
    tenant_id = os.getenv("AZURE_AD_TENANT_ID") 
    redirect_uri = os.getenv("AZURE_AD_REDIRECT_URI")
    
    print(f"Client ID: {client_id[:10]}... (truncated)" if client_id else "Client ID: Not set")
    print(f"Tenant ID: {tenant_id[:10]}... (truncated)" if tenant_id else "Tenant ID: Not set")
    print(f"Redirect URI: {redirect_uri}")
    print()
    
    if not client_id or not tenant_id:
        print("❌ Missing required environment variables")
        return False
        
    try:
        from azure_auth import AzureADAuth
        
        # Initialize authentication
        auth = AzureADAuth()
        
        print("✅ AzureADAuth initialized successfully")
        print("🚀 Starting web server authentication test...")
        print()
        
        # Test web server authentication
        success, msg = auth.login_interactive()
        
        if success:
            print("✅ Authentication successful!")
            user_info = auth.get_current_user()
            if user_info:
                print(f"👤 User: {user_info.display_name}")
                print(f"📧 Email: {user_info.email}")
            return True
        else:
            print(f"❌ Authentication failed: {msg}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_web_auth()