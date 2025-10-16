#!/usr/bin/env python3
"""
Azure AD Authentication Module for Lab 07c
Integrated authentication with Azure AD using client credentials and device flow
"""
import os
import json
import time
import webbrowser
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import logging

try:
    from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, DeviceCodeCredential
    from msal import PublicClientApplication, ConfidentialClientApplication
    import jwt
    from cryptography.fernet import Fernet
    MSAL_AVAILABLE = True
except ImportError as e:
    print(f"Authentication dependencies not installed: {e}")
    print("Please run: pip install azure-identity msal cryptography")
    MSAL_AVAILABLE = False
    # Define dummy classes to prevent NameError
    PublicClientApplication = None
    ConfidentialClientApplication = None

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
        if not MSAL_AVAILABLE:
            raise ImportError("MSAL library not available. Please run: pip install msal")
            
        self.client_id = os.getenv("AZURE_AD_CLIENT_ID", "")
        self.tenant_id = os.getenv("AZURE_AD_TENANT_ID", "common")
        self.redirect_uri = os.getenv("AZURE_AD_REDIRECT_URI", "http://localhost:8000/auth/callback")
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.scopes = ["https://graph.microsoft.com/User.Read"]
        
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
        """Load cached authentication from file with timeout"""
        try:
            if not os.path.exists(self.token_file):
                return False
                
            # Quick file size check - if too large, skip
            if os.path.getsize(self.token_file) > 10000:  # 10KB limit
                print("⚠️ Auth cache file too large, skipping...")
                return False
            
            with open(self.token_file, 'r') as f:
                data = json.load(f)
            
            # Quick validation without full object creation
            required_fields = ['user_id', 'display_name', 'email', 'expires_at']
            if not all(field in data for field in required_fields):
                os.remove(self.token_file)
                return False
            
            # Quick expiry check
            try:
                expires_at = datetime.fromisoformat(data['expires_at'])
                if datetime.now() >= expires_at:
                    os.remove(self.token_file)
                    return False
            except (ValueError, KeyError):
                os.remove(self.token_file)
                return False
            
            # Only create full UserInfo object if validation passes
            user_info = UserInfo.from_dict(data)
            self.current_user = user_info
            return True
            
        except Exception as e:
            # If any error occurs, just proceed without cached auth
            logging.warning(f"Failed to load cached auth (skipping): {e}")
            try:
                if os.path.exists(self.token_file):
                    os.remove(self.token_file)
            except:
                pass
        
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
        """API-based interactive login with token storage and reuse"""
        if not self.client_id:
            return False, "Azure AD Client ID not configured. Please set AZURE_AD_CLIENT_ID environment variable."
        
        try:
            print("🔐 Starting Azure AD API authentication...")
            print("🔧 This will use MSAL's optimized desktop app flow.")
            print(f"🏢 Tenant: {self.tenant_id}")
            print(f"📱 Client ID: {self.client_id[:8]}...")
            
            # Verify app configuration
            if not self.app:
                return False, "MSAL PublicClientApplication not initialized. Check client configuration."
            
            # Try silent authentication first (cached tokens)
            accounts = self.app.get_accounts()
            if accounts:
                print(f"🔍 Found {len(accounts)} cached account(s), attempting silent login...")
                result = self.app.acquire_token_silent(self.scopes, account=accounts[0])
                if result and "access_token" in result:
                    print("✅ Using cached authentication tokens!")
                    return self._process_auth_result(result)
                else:
                    print("⚠️  Cached tokens expired, proceeding with interactive login...")
            
            # Interactive authentication using MSAL's optimized flow
            print("🌐 Opening browser for Azure AD authentication...")
            print("💡 MSAL will handle the redirect securely with random ports")
            print("💡 If you get 'client_assertion' error, the Azure AD app may need to be configured as 'Public client'")
            
            result = self.app.acquire_token_interactive(
                scopes=self.scopes,
                prompt="select_account"  # Allow user to choose account
            )
            
            if result and "access_token" in result:
                print("✅ Authentication successful!")
                print("📝 Tokens cached automatically for future use")
                return self._process_auth_result(result)
            else:
                error_msg = result.get("error_description", "Unknown authentication error") if result else "No result returned"
                error_code = result.get("error", "unknown_error") if result else "no_result"
                
                # Provide specific guidance for common errors
                troubleshooting = ""
                if "client_assertion" in error_msg or "client_secret" in error_msg:
                    troubleshooting = "\n\n🔧 TROUBLESHOOTING:\n"
                    troubleshooting += "This error suggests the Azure AD app is configured as 'Confidential' instead of 'Public'.\n"
                    troubleshooting += "1. Go to Azure Portal > Azure Active Directory > App registrations\n"
                    troubleshooting += f"2. Find your app (Client ID: {self.client_id})\n"
                    troubleshooting += "3. Go to Authentication > Advanced settings\n"
                    troubleshooting += "4. Set 'Allow public client flows' to 'Yes'\n"
                    troubleshooting += "5. Make sure 'Supported account types' allows the intended users\n"
                    troubleshooting += "6. Add redirect URI: http://localhost (or use Device Code Flow instead)"
                
                return False, f"Authentication failed [{error_code}]: {error_msg}{troubleshooting}"
                
        except Exception as e:
            error_str = str(e)
            if "client_assertion" in error_str or "client_secret" in error_str:
                troubleshooting = "\n\n🔧 The Azure AD app registration needs to be configured as a PUBLIC client, not confidential."
                return False, f"Authentication error: {error_str}{troubleshooting}"
            return False, f"Authentication error: {error_str}"
    
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
            print("🔐 Attempting integrated Windows authentication...")
            print("This will use your current Windows identity or Azure CLI credentials.")
            
            from azure.identity import DefaultAzureCredential
            from azure.core.credentials import AccessToken
            import requests
            
            # Use DefaultAzureCredential for integrated auth
            credential = DefaultAzureCredential()
            
            # Get a token for Microsoft Graph to verify authentication
            try:
                # Request token for Microsoft Graph
                token = credential.get_token("https://graph.microsoft.com/.default")
                
                if token and token.token:
                    print("✅ Successfully obtained authentication token")
                    
                    # Use the token to get user information from Microsoft Graph
                    headers = {
                        'Authorization': f'Bearer {token.token}',
                        'Content-Type': 'application/json'
                    }
                    
                    # Get user profile information
                    response = requests.get(
                        'https://graph.microsoft.com/v1.0/me',
                        headers=headers,
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        user_data = response.json()
                        
                        user_info = UserInfo(
                            user_id=user_data.get('id', 'unknown'),
                            display_name=user_data.get('displayName', 'Unknown User'),
                            email=user_data.get('mail') or user_data.get('userPrincipalName', 'unknown@domain.com'),
                            tenant_id=self.tenant_id,
                            authenticated_at=datetime.now(),
                            expires_at=datetime.now() + timedelta(seconds=token.expires_on - time.time() if hasattr(token, 'expires_on') else 3600)
                        )
                        
                        self._save_auth(user_info)
                        print(f"✅ Authenticated as: {user_info.display_name} ({user_info.email})")
                        return True, f"Integrated authentication successful for {user_info.display_name}"
                    else:
                        print(f"❌ Failed to get user profile: {response.status_code}")
                        return False, f"Failed to retrieve user profile: {response.status_code}"
                        
                else:
                    return False, "Failed to obtain authentication token"
                    
            except Exception as token_error:
                print(f"❌ Token acquisition failed: {str(token_error)}")
                return False, f"Token acquisition failed: {str(token_error)}"
            
        except ImportError:
            return False, "Azure Identity library not available for integrated authentication"
        except Exception as e:
            print(f"❌ Integrated authentication failed: {str(e)}")
            return False, f"Integrated authentication failed: {str(e)}"

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
    
    if not login_tool.is_authenticated():
        print("\nTesting login prompt...")
        success = login_tool.check_auth_or_prompt()
        
        if success:
            print(f"\nAuthenticated User ID: {login_tool.get_current_user_id()}")
    
    print("\nAuthentication test complete.")