#!/usr/bin/env python3
"""
Test script for API-based Azure AD authentication
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../.env')

from azure_auth import AzureADAuth

def test_api_authentication():
    """Test the new API-based authentication flow"""
    print("🧪 Testing API-based Azure AD Authentication")
    print("=" * 50)
    
    # Initialize auth
    auth = AzureADAuth()
    
    # Check if we have cached tokens
    cached_token = auth.get_cached_token()
    if cached_token:
        print("✅ Found cached token!")
        user_info = auth.get_current_user()
        if user_info:
            print(f"✅ Already authenticated as: {user_info.display_name} ({user_info.email})")
            return True
    
    print("🔐 No cached token found. Starting interactive login...")
    
    # Test interactive login
    success, message = auth.login_interactive()
    
    if success:
        print(f"✅ Authentication successful: {message}")
        
        # Test getting user info
        user_info = auth.get_current_user()
        if user_info:
            print(f"👤 User: {user_info.display_name}")
            print(f"📧 Email: {user_info.email}")
            print(f"🆔 User ID: {user_info.user_id}")
        
        # Test token refresh
        print("\n🔄 Testing token refresh...")
        refresh_success, refresh_message = auth.refresh_token()
        if refresh_success:
            print(f"✅ Token refresh successful: {refresh_message}")
        else:
            print(f"⚠️ Token refresh failed: {refresh_message}")
        
        return True
    else:
        print(f"❌ Authentication failed: {message}")
        return False

if __name__ == "__main__":
    try:
        success = test_api_authentication()
        if success:
            print("\n🎉 All tests passed!")
        else:
            print("\n💥 Tests failed!")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Test cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)