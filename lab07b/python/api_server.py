"""
FastAPI Server for Snowflake AI Assistant with LangGraph
Provides REST API endpoints for interacting with the LangGraph-powered AI assistant
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import os
import sys
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from snowflake_ai_assistant import SnowflakeAIAssistant

# Initialize FastAPI app
app = FastAPI(
    title="Snowflake AI Assistant API (LangGraph)",
    description="REST API for interacting with Snowflake AI Assistant using LangGraph and OpenAI",
    version="2.0.0"
)

# Enable CORS for cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # React dev server
        "http://localhost:5000",  # Flask dev server 
        "http://localhost:8080",  # Vue/other dev servers
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5000", 
        "http://127.0.0.1:8080",
        "*"  # Allow all origins for development
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Global assistant instance
assistant = None

# Pydantic models for request/response
class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    conversation_id: str
    timestamp: datetime
    success: bool
    error: Optional[str] = None

class StatusResponse(BaseModel):
    status: str
    assistant_initialized: bool
    snowflake_connected: bool
    openai_configured: bool

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the AI assistant on startup."""
    global assistant
    try:
        print("*** Initializing Snowflake AI Assistant...")
        assistant = SnowflakeAIAssistant(use_azure=True)
        print("*** Assistant initialized successfully!")
    except Exception as e:
        print(f"*** Failed to initialize assistant: {e}")
        assistant = None

# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now()
    )

# CORS preflight handler
@app.options("/{path:path}")
async def options_handler(path: str):
    """Handle CORS preflight requests"""
    return JSONResponse(
        content={"message": "OK"}, 
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )

# Status endpoint
@app.get("/status", response_model=StatusResponse)
async def get_status():
    """Get the current status of the assistant."""
    global assistant
    
    return StatusResponse(
        status="ready" if assistant else "not_ready",
        assistant_initialized=assistant is not None,
        snowflake_connected=True if assistant else False,  # Would need actual connection test
        openai_configured=True if assistant else False     # Would need actual API test
    )

# Main chat endpoint
@app.post("/chat", response_model=ChatResponse)
async def chat_with_assistant(request: ChatRequest):
    """Send a message to the AI assistant."""
    global assistant
    
    if not assistant:
        raise HTTPException(
            status_code=503, 
            detail="Assistant not initialized. Check your configuration and restart the server."
        )
    
    try:
        # Process the chat message
        response = assistant.chat(request.message)
        
        return ChatResponse(
            response=response,
            conversation_id=request.conversation_id or "default",
            timestamp=datetime.now(),
            success=True
        )
        
    except Exception as e:
        return ChatResponse(
            response="",
            conversation_id=request.conversation_id or "default",
            timestamp=datetime.now(),
            success=False,
            error=str(e)
        )

# Employee-specific endpoint for testing
@app.get("/employees")
async def get_employees():
    """Get employee list - equivalent to 'show me the employee list' query."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    try:
        response = assistant.chat("show me the employee list")
        return {
            "query": "show me the employee list",
            "response": response,
            "timestamp": datetime.now(),
            "success": True
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Schema inspection endpoint
@app.get("/schema/tables")
async def get_tables():
    """Get list of available tables in the database."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    try:
        response = assistant.chat("What tables do we have in the database?")
        return {
            "query": "What tables do we have in the database?",
            "response": response,
            "timestamp": datetime.now(),
            "success": True
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Sample data endpoint
@app.get("/data/sample")
async def get_sample_data():
    """Get sample data from available tables."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    try:
        response = assistant.chat("Show me the first 5 rows from any employee or customer table")
        return {
            "query": "Show me the first 5 rows from any employee or customer table",
            "response": response,
            "timestamp": datetime.now(),
            "success": True
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Clear conversation memory endpoint
@app.post("/memory/clear")
async def clear_memory():
    """Clear the conversation memory."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    try:
        assistant.clear_memory()
        return {
            "message": "Conversation memory cleared successfully",
            "timestamp": datetime.now(),
            "success": True
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Get conversation history endpoint
@app.get("/memory/history")
async def get_conversation_history():
    """Get the current conversation history."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    try:
        history = assistant.get_conversation_history()
        return {
            "history": history,
            "message_count": len(history),
            "timestamp": datetime.now(),
            "success": True
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Test endpoint with predefined queries
@app.get("/test/queries")
async def run_test_queries():
    """Run a set of predefined test queries."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    test_queries = [
        "show me the employee list",
        "What tables do we have in the database?",
        "Show me the first 5 rows from any employee or customer table"
    ]
    
    results = []
    
    for query in test_queries:
        try:
            response = assistant.chat(query)
            results.append({
                "query": query,
                "response": response,
                "success": True,
                "error": None
            })
        except Exception as e:
            results.append({
                "query": query,
                "response": "",
                "success": False,
                "error": str(e)
            })
    
    return {
        "test_results": results,
        "total_queries": len(test_queries),
        "successful_queries": sum(1 for r in results if r["success"]),
        "timestamp": datetime.now()
    }

# LangGraph-specific endpoints
@app.get("/graph/visualization")
async def get_agent_graph():
    """Generate and return the agent's graph visualization."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    try:
        graph_path = assistant.get_agent_graph_image("static/agent_graph.png")
        if graph_path:
            return {
                "message": "Agent graph generated successfully",
                "graph_path": graph_path,
                "timestamp": datetime.now(),
                "success": True
            }
        else:
            return {
                "message": "Failed to generate agent graph",
                "error": "Graph generation failed",
                "timestamp": datetime.now(),
                "success": False
            }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error generating agent graph: {str(e)}"
        )

@app.get("/graph/info")
async def get_graph_info():
    """Get information about the LangGraph agent structure."""
    global assistant
    
    if not assistant:
        raise HTTPException(status_code=503, detail="Assistant not initialized")
    
    return {
        "framework": "LangGraph",
        "agent_type": "React Agent with Tools",
        "features": [
            "State-based conversation management",
            "Persistent memory with checkpointing",
            "Graph-based execution flow",
            "Enhanced debugging and visualization",
            "Tool integration with Snowflake",
            "File processing capabilities"
        ],
        "tools": [tool.name for tool in assistant.tools],
        "memory_type": "MemorySaver with thread-based persistence",
        "thread_id": assistant.thread_id,
        "timestamp": datetime.now()
    }

if __name__ == "__main__":
    print("*** Starting Snowflake AI Assistant FastAPI Server with LangGraph...")
    print("*** Available endpoints:")
    print("  - GET  /health          - Health check")
    print("  - GET  /status          - Assistant status")
    print("  - POST /chat            - Send chat message")
    print("  - GET  /employees       - Get employee list")
    print("  - GET  /schema/tables   - Get database tables")
    print("  - GET  /data/sample     - Get sample data")
    print("  - GET  /test/queries    - Run test queries")
    print("  - POST /memory/clear    - Clear conversation")
    print("  - GET  /memory/history  - Get conversation history")
    print("  - GET  /graph/visualization - Generate agent graph image")
    print("  - GET  /graph/info      - Get LangGraph agent information")
    print()
    print("*** LangGraph Features:")
    print("  ✅ State-based conversation management")
    print("  ✅ Persistent memory with checkpointing")
    print("  ✅ Graph-based execution flow")
    print("  ✅ Enhanced debugging and visualization")
    print()
    print("*** Starting server on http://localhost:8080")
    print("*** API docs available at http://localhost:8080/docs")
    
    uvicorn.run(
        "api_server:app", 
        host="localhost", 
        port=8080, 
        reload=True,
        log_level="info"
    )