"""
Enhanced FastAPI Server with Azure AD Authentication
Supports both Azure AD login and fallback to default user
"""

from fastapi import FastAPI, HTTPException, Depends, status, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional, Dict
import uvicorn
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import secrets
import jwt
import urllib.parse
import requests
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment from parent directory
env_path = Path(__file__).parent.parent / '.env'
logger.debug(f"Loading .env from: {env_path}")
load_dotenv(env_path)
logger.debug(f"ENABLE_SNOWFLAKE: {os.getenv('ENABLE_SNOWFLAKE')}")
logger.debug(f"DATABASE_CONNECTION_STRING: {'Set' if os.getenv('DATABASE_CONNECTION_STRING') else 'Not set'}")

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from snowflake_ai_assistant import SnowflakeAIAssistant
from mongodb_checkpointer import MongoDBCheckpointSaver

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Azure AD Configuration
AZURE_AD_TENANT_ID = os.getenv("AZURE_AD_TENANT_ID", "")
AZURE_AD_CLIENT_ID = os.getenv("AZURE_AD_CLIENT_ID", "")
AZURE_AD_REDIRECT_URI = os.getenv("AZURE_AD_REDIRECT_URI", "http://localhost:8080/auth/callback")

# Initialize FastAPI app
app = FastAPI(
    title="Snowflake AI Assistant API with Azure AD",
    description="""
    REST API for interacting with Snowflake AI Assistant using LangGraph and OpenAI.
    
    ## Authentication Methods:
    1. **Azure AD OAuth2** - Login with Microsoft account
    2. **No Auth** - Use as default_user for testing
    
    ## Features:
    - Chat with AI assistant
    - Persistent conversation history in Cosmos DB
    - User-isolated threads
    - Optional Snowflake integration
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer(auto_error=False)

# Pydantic Models
class AzureADConfig(BaseModel):
    """Azure AD Configuration"""
    client_id: str
    tenant_id: Optional[str] = "common"
    redirect_uri: Optional[str] = "http://localhost:8080/auth/callback"

class ChatRequest(BaseModel):
    """Chat message request"""
    message: str
    thread_id: Optional[str] = None

class ChatResponse(BaseModel):
    """Chat message response"""
    response: str
    thread_id: str
    user_id: str

class UserInfo(BaseModel):
    """User information"""
    user_id: str
    email: Optional[str] = None
    name: Optional[str] = None
    auth_method: str

class ThreadInfo(BaseModel):
    """Thread information"""
    thread_id: str
    message_count: int
    last_updated: Optional[str] = None

# Global storage for assistants and Azure AD config
assistants: Dict[str, SnowflakeAIAssistant] = {}
azure_ad_config: Dict[str, str] = {
    "client_id": AZURE_AD_CLIENT_ID,
    "tenant_id": AZURE_AD_TENANT_ID,
    "redirect_uri": AZURE_AD_REDIRECT_URI
}

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Dict:
    """Verify JWT token and return user info, or return default user if no token"""
    if credentials is None:
        # No authentication - use default user
        return {
            "user_id": "default_user",
            "email": None,
            "name": "Default User",
            "auth_method": "none"
        }
    
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return {
            "user_id": payload.get("sub"),
            "email": payload.get("email"),
            "name": payload.get("name"),
            "auth_method": payload.get("auth_method", "azure_ad")
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except (jwt.PyJWTError, jwt.DecodeError, Exception) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )

def get_or_create_assistant(user_id: str, user_email: Optional[str] = None, user_name: Optional[str] = None) -> SnowflakeAIAssistant:
    """Get or create assistant for user"""
    if user_id not in assistants:
        print(f"Creating assistant for user: {user_id}")
        assistant = SnowflakeAIAssistant(use_azure=True)
        
        # Only set user context if email and name are provided (i.e., authenticated user)
        if user_email and user_name:
            assistant.user_id = user_id
            assistant.user_name = user_name
            assistant.user_email = user_email
            print(f"  ✅ User context set: {user_name} ({user_email})")
        
        assistants[user_id] = assistant
    return assistants[user_id]

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    print("\n" + "="*70)
    print("🚀 Snowflake AI Assistant API Server with Azure AD")
    print("="*70)
    print(f"\n📚 Swagger Documentation: http://localhost:8080/docs")
    print(f"🔐 Azure AD Authentication:")
    print(f"   - Client ID: {azure_ad_config['client_id'][:20]}..." if azure_ad_config['client_id'] else "   - Not configured")
    print(f"   - Tenant ID: {azure_ad_config['tenant_id']}")
    print(f"\n💡 You can also use the API without logging in (uses default_user)")
    print("="*70 + "\n")

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Snowflake AI Assistant API with Azure AD",
        "version": "2.0.0",
        "docs": "/docs",
        "azure_ad_login": "/auth/login",
        "status": "running"
    }

# Azure AD Authentication Endpoints

@app.post("/config/azure-ad", tags=["Configuration"])
async def configure_azure_ad(config: AzureADConfig):
    """Configure Azure AD settings (for dynamic client ID setup)"""
    global azure_ad_config
    azure_ad_config["client_id"] = config.client_id
    azure_ad_config["tenant_id"] = config.tenant_id or "common"
    azure_ad_config["redirect_uri"] = config.redirect_uri or "http://localhost:8080/auth/callback"
    
    return {
        "message": "Azure AD configured successfully",
        "client_id": config.client_id,
        "tenant_id": azure_ad_config["tenant_id"],
        "login_url": f"/auth/login"
    }

@app.get("/auth/login", tags=["Authentication"], response_class=HTMLResponse)
async def azure_ad_login():
    """Initiate Azure AD OAuth2 login flow"""
    if not azure_ad_config.get("client_id"):
        return HTMLResponse(content="""
            <html>
                <head><title>Azure AD Configuration Required</title></head>
                <body style="font-family: Arial, sans-serif; padding: 50px; max-width: 800px; margin: 0 auto;">
                    <h1>⚙️ Azure AD Configuration Required</h1>
                    <p>Please configure Azure AD settings first:</p>
                    <form id="configForm">
                        <div style="margin-bottom: 15px;">
                            <label><strong>Client ID:</strong></label><br>
                            <input type="text" id="clientId" style="width: 100%; padding: 8px; margin-top: 5px;" 
                                   placeholder="Enter Azure AD Client ID" required>
                        </div>
                        <div style="margin-bottom: 15px;">
                            <label><strong>Tenant ID:</strong></label><br>
                            <input type="text" id="tenantId" value="common" style="width: 100%; padding: 8px; margin-top: 5px;">
                        </div>
                        <div style="margin-bottom: 15px;">
                            <label><strong>Redirect URI:</strong></label><br>
                            <input type="text" id="redirectUri" value="http://localhost:8080/auth/callback" 
                                   style="width: 100%; padding: 8px; margin-top: 5px;">
                        </div>
                        <button type="submit" style="background: #0078d4; color: white; padding: 10px 20px; border: none; cursor: pointer; font-size: 16px;">
                            Save & Login
                        </button>
                    </form>
                    <script>
                        document.getElementById('configForm').addEventListener('submit', async (e) => {
                            e.preventDefault();
                            const config = {
                                client_id: document.getElementById('clientId').value,
                                tenant_id: document.getElementById('tenantId').value,
                                redirect_uri: document.getElementById('redirectUri').value
                            };
                            
                            try {
                                const response = await fetch('/config/azure-ad', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify(config)
                                });
                                const result = await response.json();
                                if (response.ok) {
                                    window.location.href = '/auth/login';
                                } else {
                                    alert('Configuration failed: ' + JSON.stringify(result));
                                }
                            } catch (error) {
                                alert('Error: ' + error.message);
                            }
                        });
                    </script>
                </body>
            </html>
        """)
    
    # Build Azure AD authorization URL
    tenant_id = azure_ad_config.get("tenant_id", "common")
    client_id = azure_ad_config["client_id"]
    redirect_uri = azure_ad_config["redirect_uri"]
    
    auth_url = (
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize?"
        f"client_id={client_id}&"
        f"response_type=code&"
        f"redirect_uri={urllib.parse.quote(redirect_uri)}&"
        f"response_mode=query&"
        f"scope=openid%20profile%20email&"
        f"state={secrets.token_urlsafe(16)}"
    )
    
    return RedirectResponse(url=auth_url)

@app.get("/auth/callback", tags=["Authentication"])
async def azure_ad_callback(code: str = Query(...), state: str = Query(None)):
    """Handle Azure AD OAuth2 callback"""
    if not code:
        raise HTTPException(status_code=400, detail="No authorization code received")
    
    tenant_id = azure_ad_config.get("tenant_id", "common")
    client_id = azure_ad_config["client_id"]
    redirect_uri = azure_ad_config["redirect_uri"]
    
    # Exchange code for token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    token_data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "code": code,
        "redirect_uri": redirect_uri,
    }
    
    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        token_response = response.json()
        
        # Decode ID token to get user info
        id_token = token_response.get("id_token")
        user_info = jwt.decode(id_token, options={"verify_signature": False})
        
        # Create our own JWT token
        access_token = create_access_token(
            data={
                "sub": user_info.get("oid") or user_info.get("sub"),
                "email": user_info.get("email") or user_info.get("preferred_username"),
                "name": user_info.get("name"),
                "auth_method": "azure_ad"
            },
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        
        # Return HTML page with token
        bearer_token = f"Bearer {access_token}"
        return HTMLResponse(content=f"""
            <html>
                <head><title>Login Successful</title></head>
                <body style="font-family: Arial, sans-serif; padding: 50px; text-align: center;">
                    <h1>✅ Login Successful!</h1>
                    <p>Welcome, {user_info.get('name')}!</p>
                    <p><strong>Your Bearer Token:</strong></p>
                    <textarea readonly style="width: 80%; height: 120px; padding: 10px; margin: 20px 0; font-family: monospace; font-size: 12px;">{bearer_token}</textarea>
                    <p>Copy this token and use it in the Swagger UI:</p>
                    <ol style="text-align: left; max-width: 600px; margin: 20px auto;">
                        <li>Go to <a href="/docs" target="_blank">Swagger UI</a></li>
                        <li>Click the "Authorize" button (🔓)</li>
                        <li>Paste the <strong>entire Bearer token</strong> in the "Value" field</li>
                        <li>Click "Authorize" then "Close"</li>
                    </ol>
                    <button onclick="navigator.clipboard.writeText('{bearer_token}'); alert('Bearer token copied to clipboard!');" 
                            style="background: #0078d4; color: white; padding: 10px 20px; border: none; cursor: pointer; font-size: 16px; margin-right: 10px;">
                        Copy Bearer Token
                    </button>
                    <button onclick="navigator.clipboard.writeText('{access_token}'); alert('Token (without Bearer) copied to clipboard!');" 
                            style="background: #28a745; color: white; padding: 10px 20px; border: none; cursor: pointer; font-size: 16px;">
                        Copy Token Only
                    </button>
                    <br><br>
                    <a href="/docs" style="color: #0078d4; text-decoration: none; font-size: 18px;">→ Go to API Documentation</a>
                </body>
            </html>
        """)
        
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to exchange code for token: {str(e)}")

@app.post("/auth/token-info", tags=["Authentication"])
async def get_token_info(azure_token: str):
    """
    Extract user information from an Azure AD access token or ID token.
    This allows you to use tokens obtained from other Azure AD login flows.
    
    Parameters:
    - azure_token: The Azure AD access token or ID token (with or without "Bearer " prefix)
    
    Returns:
    - User information and a new API token for this service
    """
    try:
        # Remove "Bearer " prefix if present
        token = azure_token.strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        
        logger.debug("Decoding Azure AD token (without signature verification)")
        # Decode without verification to get user info
        # In production, you should verify the signature
        user_info = jwt.decode(token, options={"verify_signature": False})
        logger.debug(f"Token claims: {user_info.keys()}")
        
        # Extract user information from token claims
        user_id = user_info.get("oid") or user_info.get("sub") or user_info.get("unique_name")
        email = user_info.get("email") or user_info.get("preferred_username") or user_info.get("upn")
        name = user_info.get("name") or email
        
        if not user_id:
            raise HTTPException(status_code=400, detail="Could not extract user ID from token")
        
        logger.info(f"Extracted user info - ID: {user_id}, Email: {email}, Name: {name}")
        
        # Create our own JWT token for this API
        access_token = create_access_token(
            data={
                "sub": user_id,
                "email": email,
                "name": name,
                "auth_method": "azure_ad_external"
            },
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        
        return {
            "user_info": {
                "user_id": user_id,
                "email": email,
                "name": name,
                "auth_method": "azure_ad_external"
            },
            "api_token": access_token,
            "bearer_token": f"Bearer {access_token}",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "message": "Use the bearer_token for subsequent API calls"
        }
        
    except jwt.DecodeError as e:
        logger.error(f"Failed to decode token: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid token format: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing token: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing token: {str(e)}")

@app.get("/me", response_model=UserInfo, tags=["Authentication"])
async def get_current_user(user: Dict = Depends(verify_token)):
    """Get current user information"""
    return UserInfo(**user)

# Chat Endpoints

@app.post("/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(
    request: ChatRequest,
    user: Dict = Depends(verify_token)
):
    """
    Send a message to the AI assistant
    - Works without authentication (uses default_user)
    - With authentication, uses user-specific context
    """
    try:
        user_id = user["user_id"]
        user_email = user.get("email")
        user_name = user.get("name")
        
        assistant = get_or_create_assistant(user_id, user_email, user_name)
        
        # Update assistant's thread_id if provided
        if request.thread_id:
            assistant.thread_id = request.thread_id
        
        # Get response
        response = assistant.chat(request.message)
        
        return ChatResponse(
            response=response,
            thread_id=assistant.thread_id,
            user_id=user_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/threads", response_model=List[ThreadInfo], tags=["Chat"])
async def list_threads(user: Dict = Depends(verify_token)):
    """List all conversation threads for the current user"""
    try:
        user_id = user["user_id"]
        
        # Get threads from Cosmos DB
        connection_string = os.getenv('DATABASE_CONNECTION_STRING')
        if not connection_string:
            return []
        
        from pymongo import MongoClient
        client = MongoClient(connection_string)
        db = client["langgraph_db"]
        collection = db["checkpoints"]
        
        # Get distinct thread_ids for this user
        threads = collection.distinct("thread_id", {"user_id": user_id})
        
        thread_infos = []
        for thread_id in threads:
            count = collection.count_documents({"user_id": user_id, "thread_id": thread_id})
            latest = collection.find_one(
                {"user_id": user_id, "thread_id": thread_id},
                sort=[("created_at", -1)]
            )
            
            thread_infos.append(ThreadInfo(
                thread_id=thread_id,
                message_count=count,
                last_updated=latest.get("created_at").isoformat() if latest and latest.get("created_at") else None
            ))
        
        client.close()
        return thread_infos
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/threads/{thread_id}", tags=["Chat"])
async def delete_thread(thread_id: str, user: Dict = Depends(verify_token)):
    """Delete a conversation thread"""
    try:
        user_id = user["user_id"]
        
        # Delete thread from Cosmos DB
        connection_string = os.getenv('DATABASE_CONNECTION_STRING')
        if not connection_string:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from pymongo import MongoClient
        client = MongoClient(connection_string)
        db = client["langgraph_db"]
        collection = db["checkpoints"]
        
        result = collection.delete_many({"user_id": user_id, "thread_id": thread_id})
        client.close()
        
        # Clear assistant if exists
        if user_id in assistants and assistants[user_id].thread_id == thread_id:
            assistants[user_id].clear_memory()
        
        return {
            "message": f"Thread deleted successfully",
            "thread_id": thread_id,
            "deleted_count": result.deleted_count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    enable_snowflake = os.getenv('ENABLE_SNOWFLAKE', 'false').lower() == 'true'
    db_connection = os.getenv('DATABASE_CONNECTION_STRING', '')
    
    print("\n🚀 Starting Snowflake AI Assistant API with Azure AD...")
    print(f"   Snowflake: {'✅ Enabled' if enable_snowflake else '❌ Disabled'}")
    print(f"   Cosmos DB: {'✅ Enabled' if 'cosmos' in db_connection.lower() else '❌ Disabled'}")
    print(f"   Azure AD: {'✅ Configured' if AZURE_AD_CLIENT_ID else '⚙️  Configure at /auth/login'}")
    print(f"   Default user assistant will be created on first request")
    print(f"   User-specific assistants will be created on login\n")
    
    uvicorn.run(app, host="localhost", port=8080)
