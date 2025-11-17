"""
Enhanced FastAPI Server for Snowflake AI Assistant with Optional Authentication
Provides REST API endpoints with integrated Swagger login support
- If user logs in: Uses their identity and isolated chat history
- If no login: Uses default user for testing
"""

from fastapi import FastAPI, HTTPException, Depends, status, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordBearer, OAuth2PasswordRequestForm
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

# Load environment from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from snowflake_ai_assistant import SnowflakeAIAssistant
from mongodb_checkpointer import MongoDBCheckpointSaver

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Simple user database (in production, use a real database)
USERS_DB = {
    "admin": {"username": "admin", "password": "admin123", "email": "admin@example.com"},
    "user1": {"username": "user1", "password": "password123", "email": "user1@example.com"},
    "demo": {"username": "demo", "password": "demo", "email": "demo@example.com"},
}

# Initialize FastAPI app
app = FastAPI(
    title="Snowflake AI Assistant API with Authentication",
    description="""
    REST API for interacting with Snowflake AI Assistant using LangGraph and OpenAI.
    
    ## Authentication (Optional)
    
    You can use this API in two ways:
    
    1. **Without Login (Default User)**: Just call the endpoints directly. Your chat history will be saved under "default_user".
    
    2. **With Login (Personalized)**: Click the "Authorize" button and login with:
       - Username: `demo` / Password: `demo`
       - Username: `admin` / Password: `admin123`
       - Username: `user1` / Password: `password123`
    
    When logged in, your chat history is isolated to your user account.
    
    ## Features
    - 💬 Chat with AI assistant
    - 📊 View chat history
    - 🔒 Optional authentication for personalized experience
    - 🗑️ Clear your chat history
    """,
    version="3.0.0",
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security schemes
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
security = HTTPBearer(auto_error=False)

# Global assistants dictionary (one per user)
assistants: Dict[str, SnowflakeAIAssistant] = {}

# Pydantic models
class Token(BaseModel):
    access_token: str
    token_type: str
    username: str

class User(BaseModel):
    username: str
    email: Optional[str] = None

class ChatRequest(BaseModel):
    message: str
    
class ChatResponse(BaseModel):
    response: str
    user: str
    timestamp: datetime
    success: bool
    error: Optional[str] = None

class ChatHistoryResponse(BaseModel):
    user: str
    messages: List[Dict]
    total_checkpoints: int

class StatusResponse(BaseModel):
    status: str
    user: str
    authenticated: bool
    snowflake_enabled: bool
    cosmos_db_enabled: bool

# Helper functions
def create_access_token(data: dict, expires_delta: timedelta = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Optional[str]:
    """Verify JWT token and return username"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        return username
    except jwt.PyJWTError:
        return None

def get_current_user(token: Optional[str] = Depends(oauth2_scheme)) -> str:
    """
    Get current user from token, or return 'default_user' if no token.
    This allows API to work with or without authentication.
    """
    if not token:
        return "default_user"
    
    username = verify_token(token)
    if username is None:
        return "default_user"
    
    return username

def get_or_create_assistant(user_id: str) -> SnowflakeAIAssistant:
    """Get or create assistant instance for user"""
    if user_id not in assistants:
        print(f"Creating new assistant for user: {user_id}")
        assistants[user_id] = SnowflakeAIAssistant(use_azure=True)
        # TODO: Update assistant to use user-specific thread_id
        assistants[user_id].thread_id = f"thread-{user_id}"
    return assistants[user_id]

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the default assistant on startup."""
    print("🚀 Starting Snowflake AI Assistant API with Authentication...")
    print(f"   Snowflake: {'✅ Enabled' if os.getenv('ENABLE_SNOWFLAKE', 'false').lower() == 'true' else '❌ Disabled'}")
    print(f"   Cosmos DB: {'✅ Enabled' if 'cosmos' in os.getenv('DATABASE_CONNECTION_STRING', '').lower() else '❌ Disabled'}")
    print("   Default user assistant will be created on first request")
    print("   User-specific assistants will be created on login")

# Authentication endpoints
@app.post("/token", response_model=Token, tags=["Authentication"])
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Login endpoint to get access token.
    
    Use these credentials to test:
    - username: `demo`, password: `demo`
    - username: `admin`, password: `admin123`
    - username: `user1`, password: `password123`
    """
    user = USERS_DB.get(form_data.username)
    if not user or user["password"] != form_data.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    
    return Token(
        access_token=access_token,
        token_type="bearer",
        username=user["username"]
    )

@app.get("/me", response_model=User, tags=["Authentication"])
async def get_current_user_info(current_user: str = Depends(get_current_user)):
    """Get current user information"""
    if current_user == "default_user":
        return User(username="default_user", email="anonymous@example.com")
    
    user_data = USERS_DB.get(current_user, {})
    return User(
        username=current_user,
        email=user_data.get("email", f"{current_user}@example.com")
    )

# Health check
@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "version": "3.0.0"
    }

# Status endpoint
@app.get("/status", response_model=StatusResponse, tags=["System"])
async def get_status(current_user: str = Depends(get_current_user)):
    """Get current system status and user info"""
    return StatusResponse(
        status="ready",
        user=current_user,
        authenticated=(current_user != "default_user"),
        snowflake_enabled=os.getenv('ENABLE_SNOWFLAKE', 'false').lower() == 'true',
        cosmos_db_enabled='cosmos' in os.getenv('DATABASE_CONNECTION_STRING', '').lower()
    )

# Chat endpoint
@app.post("/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(
    request: ChatRequest,
    current_user: str = Depends(get_current_user)
):
    """
    Send a message to the AI assistant.
    
    - If authenticated: Uses your personal chat history
    - If not authenticated: Uses default user chat history
    """
    try:
        assistant = get_or_create_assistant(current_user)
        response = assistant.chat(request.message)
        
        return ChatResponse(
            response=response,
            user=current_user,
            timestamp=datetime.now(),
            success=True
        )
    except Exception as e:
        return ChatResponse(
            response="",
            user=current_user,
            timestamp=datetime.now(),
            success=False,
            error=str(e)
        )

# Chat history endpoint
@app.get("/chat/history", tags=["Chat"])
async def get_chat_history(current_user: str = Depends(get_current_user)):
    """
    Get chat history for current user.
    
    Shows conversation messages from Cosmos DB if enabled,
    otherwise shows in-memory conversation.
    """
    try:
        assistant = get_or_create_assistant(current_user)
        history = assistant.get_conversation_history()
        
        messages = []
        for msg in history:
            msg_type = type(msg).__name__
            content = getattr(msg, 'content', str(msg))
            messages.append({
                "type": msg_type,
                "content": content[:500] if 'System' in msg_type else content,
                "timestamp": datetime.now().isoformat()
            })
        
        return {
            "user": current_user,
            "messages": messages,
            "total_messages": len(messages),
            "authenticated": current_user != "default_user"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Clear history endpoint
@app.delete("/chat/history", tags=["Chat"])
async def clear_chat_history(current_user: str = Depends(get_current_user)):
    """
    Clear chat history for current user.
    
    Creates a new conversation thread.
    """
    try:
        assistant = get_or_create_assistant(current_user)
        assistant.clear_memory()
        
        return {
            "message": "Chat history cleared",
            "user": current_user,
            "timestamp": datetime.now()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Quick test endpoints
@app.get("/quick-test", tags=["Examples"])
async def quick_test(current_user: str = Depends(get_current_user)):
    """Quick test endpoint - asks assistant to introduce itself"""
    try:
        assistant = get_or_create_assistant(current_user)
        response = assistant.chat("Hello! Please introduce yourself briefly.")
        
        return {
            "user": current_user,
            "query": "Hello! Please introduce yourself briefly.",
            "response": response,
            "timestamp": datetime.now()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 Snowflake AI Assistant API Server with Authentication")
    print("="*70)
    print("\n📚 Swagger Documentation: http://localhost:8080/docs")
    print("🔐 Login credentials:")
    print("   - demo / demo")
    print("   - admin / admin123")
    print("   - user1 / password123")
    print("\n💡 You can also use the API without logging in (uses default_user)")
    print("="*70 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info"
    )
