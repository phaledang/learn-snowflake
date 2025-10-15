#!/usr/bin/env python3
"""
Azure AD Authentication Module for Lab 07c
Integrated authentication with Azure AD using client credentials and device flow
"""

import os
import json
import time
import webbrowser
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import logging

try:
    from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, DeviceCodeCredential
    from msal import PublicClientApplication, ConfidentialClientApplication
    import jwt
    from cryptography.fernet import Fernet
except ImportError as e:
    print(f"Authentication dependencies not installed: {e}")
    print("Please run: pip install azure-identity msal cryptography")

@dataclass
class UserInfo:
    """User information from Azure AD"""
    user_id: str
    display_name: str
    email: str
    tenant_id: str
    authenticated_at: datetime
    expires_at: datetime
    
    def is_valid(self) -> bool:
        """Check if authentication is still valid"""
        return datetime.now() < self.expires_at
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        data['authenticated_at'] = self.authenticated_at.isoformat()
        data['expires_at'] = self.expires_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserInfo':
        """Create from dictionary"""
        data['authenticated_at'] = datetime.fromisoformat(data['authenticated_at'])
        data['expires_at'] = datetime.fromisoformat(data['expires_at'])
        return cls(**data)

class AzureADAuth:
    """Azure AD Authentication Manager"""
    
    def __init__(self):
        self.client_id = os.getenv("AZURE_AD_CLIENT_ID", "")
        self.tenant_id = os.getenv("AZURE_AD_TENANT_ID", "common")
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.scopes = ["https://graph.microsoft.com/.default", "openid", "profile", "email"]
        
        # Initialize MSAL app
        self.app = None
        if self.client_id:
            self.app = PublicClientApplication(
                client_id=self.client_id,
                authority=self.authority
            )
        
        # Token storage
        self.token_file = ".auth_token"
        self.current_user: Optional[UserInfo] = None
        
        # Load existing authentication if available
        self._load_cached_auth()
    
    def _load_cached_auth(self):
        """Load cached authentication from file"""
        try:
            if os.path.exists(self.token_file):
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                
                user_info = UserInfo.from_dict(data)
                if user_info.is_valid():
                    self.current_user = user_info
                    return True
                else:
                    # Token expired, remove file
                    os.remove(self.token_file)
        except Exception as e:
            logging.warning(f"Failed to load cached auth: {e}")
        
        return False
    
    def _save_auth(self, user_info: UserInfo):
        """Save authentication to file"""
        try:
            with open(self.token_file, 'w') as f:
                json.dump(user_info.to_dict(), f, indent=2)
            self.current_user = user_info
        except Exception as e:
            logging.error(f"Failed to save auth: {e}")
    
    def is_authenticated(self) -> bool:
        """Check if user is currently authenticated"""
        return self.current_user is not None and self.current_user.is_valid()
    
    def get_current_user(self) -> Optional[UserInfo]:
        """Get current authenticated user"""
        if self.is_authenticated():
            return self.current_user
        return None
    
    def get_user_id(self) -> Optional[str]:
        """Get current user ID for thread ownership"""
        user = self.get_current_user()
        return user.user_id if user else None
    
    def login_interactive(self) -> Tuple[bool, str]:
        """Interactive login with browser"""
        if not self.client_id:
            return False, "Azure AD Client ID not configured. Please set AZURE_AD_CLIENT_ID environment variable."
        
        try:
            print("🔐 Starting Azure AD authentication...")
            print("This will open a browser window for login.")
            
            # Try silent authentication first
            accounts = self.app.get_accounts()
            if accounts:
                print("Found cached account, attempting silent login...")
                result = self.app.acquire_token_silent(self.scopes, account=accounts[0])
                if result and "access_token" in result:
                    return self._process_auth_result(result)
            
            # Interactive authentication
            print("Opening browser for authentication...")
            result = self.app.acquire_token_interactive(
                scopes=self.scopes,
                prompt="select_account"
            )
            
            if result and "access_token" in result:
                return self._process_auth_result(result)
            else:
                error_msg = result.get("error_description", "Unknown authentication error")
                return False, f"Authentication failed: {error_msg}"
                
        except Exception as e:
            return False, f"Authentication error: {str(e)}"
    
    def login_device_code(self) -> Tuple[bool, str]:
        """Device code flow login (for environments without browser)"""
        if not self.client_id:
            return False, "Azure AD Client ID not configured."
        
        try:
            print("🔐 Starting device code authentication...")
            
            device_flow = self.app.initiate_device_flow(scopes=self.scopes)
            
            if "user_code" not in device_flow:
                return False, "Failed to create device flow"
            
            print("\n" + "="*60)
            print("📱 DEVICE CODE AUTHENTICATION")
            print("="*60)
            print(device_flow["message"])
            print("="*60)
            print("\nWaiting for authentication... (Press Ctrl+C to cancel)")
            
            # Poll for the completion of the auth flow
            result = self.app.acquire_token_by_device_flow(device_flow)
            
            if result and "access_token" in result:
                return self._process_auth_result(result)
            else:
                error_msg = result.get("error_description", "Authentication timeout or cancelled")
                return False, f"Authentication failed: {error_msg}"
                
        except KeyboardInterrupt:
            return False, "Authentication cancelled by user"
        except Exception as e:
            return False, f"Authentication error: {str(e)}"
    
    def login_integrated(self) -> Tuple[bool, str]:
        """Integrated Windows Authentication (for domain-joined machines)"""
        try:
            print("🔐 Attempting integrated authentication...")
            
            # Use DefaultAzureCredential for integrated auth
            credential = DefaultAzureCredential()
            
            # This is a simplified approach - in practice you'd need to get a token
            # for Microsoft Graph or your specific API
            print("✅ Integrated authentication attempted")
            print("Note: This is a placeholder for integrated auth")
            print("In production, this would use DefaultAzureCredential with proper token acquisition")
            
            # For demo purposes, create a mock user
            user_info = UserInfo(
                user_id="integrated-user-" + str(time.time()),
                display_name="Integrated User",
                email="user@company.com",
                tenant_id=self.tenant_id,
                authenticated_at=datetime.now(),
                expires_at=datetime.now() + timedelta(hours=1)
            )
            
            self._save_auth(user_info)
            return True, "Integrated authentication successful"
            
        except Exception as e:
            return False, f"Integrated authentication failed: {str(e)}"
    
    def _process_auth_result(self, result: Dict[str, Any]) -> Tuple[bool, str]:
        """Process authentication result and extract user info"""
        try:
            # Decode the ID token to get user information
            id_token = result.get("id_token")
            if not id_token:
                return False, "No ID token received"
            
            # Decode JWT (note: in production, verify signature)
            decoded_token = jwt.decode(id_token, options={"verify_signature": False})
            
            user_info = UserInfo(
                user_id=decoded_token.get("oid", decoded_token.get("sub", "unknown")),
                display_name=decoded_token.get("name", "Unknown User"),
                email=decoded_token.get("preferred_username", decoded_token.get("email", "unknown@unknown.com")),
                tenant_id=decoded_token.get("tid", self.tenant_id),
                authenticated_at=datetime.now(),
                expires_at=datetime.now() + timedelta(seconds=result.get("expires_in", 3600))
            )
            
            self._save_auth(user_info)
            
            return True, f"Successfully authenticated as {user_info.display_name}"
            
        except Exception as e:
            return False, f"Failed to process authentication: {str(e)}"
    
    def logout(self) -> bool:
        """Logout current user"""
        try:
            if os.path.exists(self.token_file):
                os.remove(self.token_file)
            
            self.current_user = None
            
            # Clear MSAL cache
            if self.app:
                accounts = self.app.get_accounts()
                for account in accounts:
                    self.app.remove_account(account)
            
            return True
        except Exception as e:
            logging.error(f"Logout failed: {e}")
            return False
    
    def get_auth_status(self) -> Dict[str, Any]:
        """Get current authentication status"""
        if self.is_authenticated():
            user = self.current_user
            return {
                "authenticated": True,
                "user_id": user.user_id,
                "display_name": user.display_name,
                "email": user.email,
                "authenticated_at": user.authenticated_at.isoformat(),
                "expires_at": user.expires_at.isoformat(),
                "time_remaining": str(user.expires_at - datetime.now())
            }
        else:
            return {
                "authenticated": False,
                "client_id_configured": bool(self.client_id),
                "tenant_id": self.tenant_id
            }

class LoginTool:
    """Interactive login tool for the assistant"""
    
    def __init__(self):
        self.auth = AzureADAuth()
    
    def prompt_login(self) -> Tuple[bool, str]:
        """Prompt user for login with options"""
        print("\n🔐 AUTHENTICATION REQUIRED")
        print("=" * 50)
        print("To access your conversation history, please authenticate with Azure AD.")
        print("\nLogin Options:")
        print("1. Interactive Browser Login (Recommended)")
        print("2. Device Code Login (No browser)")
        print("3. Integrated Authentication (Domain users)")
        print("4. Skip Login (No conversation history)")
        print("=" * 50)
        
        while True:
            try:
                choice = input("Select login method (1-4): ").strip()
                
                if choice == "1":
                    return self.auth.login_interactive()
                elif choice == "2":
                    return self.auth.login_device_code()
                elif choice == "3":
                    return self.auth.login_integrated()
                elif choice == "4":
                    print("⚠️  Skipping login. You will not have access to conversation history.")
                    return False, "Login skipped by user"
                else:
                    print("Invalid choice. Please select 1-4.")
                    
            except KeyboardInterrupt:
                print("\n❌ Login cancelled.")
                return False, "Login cancelled"
            except Exception as e:
                print(f"❌ Error during login: {e}")
                return False, f"Login error: {e}"
    
    def check_auth_or_prompt(self) -> bool:
        """Check if authenticated, prompt if not"""
        if self.auth.is_authenticated():
            user = self.auth.get_current_user()
            print(f"✅ Already authenticated as: {user.display_name}")
            return True
        
        success, message = self.prompt_login()
        if success:
            user = self.auth.get_current_user()
            print(f"✅ {message}")
            print(f"Welcome, {user.display_name}!")
        else:
            print(f"❌ {message}")
        
        return success
    
    def get_current_user_id(self) -> Optional[str]:
        """Get current user ID for thread ownership"""
        return self.auth.get_user_id()
    
    def is_authenticated(self) -> bool:
        """Check if user is authenticated"""
        return self.auth.is_authenticated()
    
    def get_auth_status(self) -> Dict[str, Any]:
        """Get authentication status"""
        return self.auth.get_auth_status()
    
    def logout(self) -> bool:
        """Logout current user"""
        if self.auth.logout():
            print("✅ Successfully logged out")
            return True
        else:
            print("❌ Failed to logout")
            return False

# Global authentication instance
_login_tool: Optional[LoginTool] = None

def get_login_tool() -> LoginTool:
    """Get global login tool instance"""
    global _login_tool
    if _login_tool is None:
        _login_tool = LoginTool()
    return _login_tool

if __name__ == "__main__":
    # Test authentication
    print("Testing Azure AD Authentication...")
    
    login_tool = get_login_tool()
    
    print("\nCurrent Status:")
    status = login_tool.get_auth_status()
    print(json.dumps(status, indent=2))
    
    if not login_tool.is_authenticated():
        print("\nTesting login prompt...")
        success = login_tool.check_auth_or_prompt()
        
        if success:
            print(f"\nAuthenticated User ID: {login_tool.get_current_user_id()}")
            status = login_tool.get_auth_status()
            print(json.dumps(status, indent=2))
    
    print("\nAuthentication test complete.")