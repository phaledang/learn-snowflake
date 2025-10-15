#!/usr/bin/env python3
"""
Thread Management API for Lab 07c
RESTful API for managing conversation threads and state persistence
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from state_persistence import get_state_manager, ThreadMetadata
from thread_config import get_thread_config

# Pydantic models for API
class ThreadCreateRequest(BaseModel):
    title: Optional[str] = Field(None, description="Optional title for the thread")
    tags: Optional[List[str]] = Field(default_factory=list, description="Tags for categorizing the thread")

class ThreadUpdateRequest(BaseModel):
    title: Optional[str] = Field(None, description="Update thread title")
    summary: Optional[str] = Field(None, description="Update thread summary")
    tags: Optional[List[str]] = Field(None, description="Update thread tags")

class ThreadResponse(BaseModel):
    thread_id: str
    title: str
    summary: str
    created_at: str
    last_updated: str
    message_count: int
    tags: List[str]

class ThreadListResponse(BaseModel):
    threads: List[ThreadResponse]
    total: int
    has_persistence: bool
    database_type: str

class ConfigResponse(BaseModel):
    thread_management_enabled: bool
    database_type: str
    persistence_enabled: bool
    thread_prefix: str
    max_threads: int

# Initialize FastAPI app
app = FastAPI(
    title="Lab 07c Thread Management API",
    description="Advanced thread management for LangGraph conversations with multi-database support",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize state manager
state_manager = get_state_manager()
thread_config = get_thread_config()

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Lab 07c Thread Management API",
        "version": "1.0.0",
        "endpoints": {
            "config": "/config",
            "threads": "/threads",
            "create_thread": "/threads (POST)",
            "get_thread": "/threads/{thread_id}",
            "delete_thread": "/threads/{thread_id} (DELETE)",
            "cleanup": "/threads/cleanup (POST)"
        }
    }

@app.get("/config", response_model=ConfigResponse)
async def get_config():
    """Get thread management configuration"""
    config_info = thread_config.get_connection_info()
    
    return ConfigResponse(
        thread_management_enabled=thread_config.should_persist_threads(),
        database_type=config_info["database_type"],
        persistence_enabled=config_info["persistence_enabled"] == "True",
        thread_prefix=config_info["thread_prefix"],
        max_threads=int(config_info["max_threads"])
    )

@app.get("/threads", response_model=ThreadListResponse)
async def list_threads(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of threads to return"),
    include_empty: bool = Query(True, description="Include threads with no messages")
):
    """List all available threads"""
    try:
        threads = await state_manager.get_threads(limit)
        
        # Filter empty threads if requested
        if not include_empty:
            threads = [t for t in threads if t.message_count > 0]
        
        thread_responses = [
            ThreadResponse(
                thread_id=t.thread_id,
                title=t.title or f"Thread {t.thread_id[:8]}",
                summary=t.summary,
                created_at=t.created_at.isoformat(),
                last_updated=t.last_updated.isoformat(),
                message_count=t.message_count,
                tags=t.tags
            )
            for t in threads
        ]
        
        return ThreadListResponse(
            threads=thread_responses,
            total=len(thread_responses),
            has_persistence=thread_config.should_persist_threads(),
            database_type=thread_config.config.database_type.value
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list threads: {str(e)}")

@app.post("/threads", response_model=Dict[str, str])
async def create_thread(request: ThreadCreateRequest = Body(...)):
    """Create a new conversation thread"""
    try:
        # Generate new thread ID
        thread_id = state_manager.generate_thread_id()
        
        # Create initial thread metadata
        if thread_config.should_persist_threads():
            # Save empty checkpoint to create thread
            await state_manager.backend.save_checkpoint(thread_id, {
                "messages": [],
                "metadata": {
                    "title": request.title or f"Thread {thread_id[:8]}",
                    "tags": request.tags,
                    "created_at": datetime.now().isoformat()
                }
            })
        
        return {
            "thread_id": thread_id,
            "title": request.title or f"Thread {thread_id[:8]}",
            "status": "created",
            "persistence": "enabled" if thread_config.should_persist_threads() else "disabled"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create thread: {str(e)}")

@app.get("/threads/{thread_id}", response_model=Dict[str, Any])
async def get_thread(thread_id: str):
    """Get details for a specific thread"""
    try:
        summary = await state_manager.get_thread_summary(thread_id)
        
        if not summary:
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get thread: {str(e)}")

@app.patch("/threads/{thread_id}", response_model=Dict[str, str])
async def update_thread(thread_id: str, request: ThreadUpdateRequest = Body(...)):
    """Update thread metadata"""
    try:
        # Check if thread exists
        summary = await state_manager.get_thread_summary(thread_id)
        if not summary:
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
        
        # Update thread metadata (this would require extending the backend)
        # For now, return success message
        
        return {
            "thread_id": thread_id,
            "status": "updated",
            "message": "Thread metadata update requested (implementation pending)"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update thread: {str(e)}")

@app.delete("/threads/{thread_id}", response_model=Dict[str, str])
async def delete_thread(thread_id: str):
    """Delete a specific thread"""
    try:
        success = await state_manager.delete_thread(thread_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found or could not be deleted")
        
        return {
            "thread_id": thread_id,
            "status": "deleted",
            "message": "Thread and all associated data removed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete thread: {str(e)}")

@app.post("/threads/cleanup", response_model=Dict[str, Any])
async def cleanup_old_threads(
    days: int = Body(30, embed=True, description="Delete threads older than this many days")
):
    """Clean up threads older than specified days"""
    try:
        if days < 1:
            raise HTTPException(status_code=400, detail="Days must be at least 1")
        
        deleted_count = await state_manager.cleanup_old_threads(days)
        
        return {
            "status": "completed",
            "deleted_threads": deleted_count,
            "cutoff_days": days,
            "message": f"Cleaned up {deleted_count} threads older than {days} days"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cleanup threads: {str(e)}")

@app.get("/threads/export/{thread_id}")
async def export_thread(thread_id: str, format: str = Query("json", regex="^(json|txt)$")):
    """Export thread conversation history"""
    try:
        # Load thread checkpoint
        checkpoint = await state_manager.backend.load_checkpoint(thread_id)
        
        if not checkpoint:
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
        
        if format == "json":
            return JSONResponse(content=checkpoint)
        else:
            # Convert to text format
            messages = checkpoint.get("messages", [])
            text_content = f"Thread: {thread_id}\n" + "="*50 + "\n\n"
            
            for i, msg in enumerate(messages):
                text_content += f"Message {i+1}:\n{msg}\n\n"
            
            return JSONResponse(content={"thread_id": thread_id, "content": text_content})
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export thread: {str(e)}")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        threads = await state_manager.get_threads(1)
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database_type": thread_config.config.database_type.value,
            "persistence_enabled": thread_config.should_persist_threads(),
            "thread_count": len(threads) if threads else 0
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )

if __name__ == "__main__":
    import uvicorn
    
    print("Starting Lab 07c Thread Management API...")
    print(f"Database Type: {thread_config.config.database_type.value}")
    print(f"Persistence: {'Enabled' if thread_config.should_persist_threads() else 'Disabled'}")
    
    uvicorn.run(
        "thread_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )