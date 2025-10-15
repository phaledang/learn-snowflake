#!/usr/bin/env python3
"""
Advanced State Persistence Layer for Lab 07c
Multi-database support for LangGraph thread state management
"""

import asyncio
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, AsyncIterator, Union
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import MemorySaver
# Note: SQLite checkpointer may not be available in this LangGraph version
# from langgraph.checkpoint.sqlite import SqliteSaver

from thread_config import ThreadManagerConfig, DatabaseType, get_thread_config

@dataclass
class ThreadMetadata:
    """Metadata for a conversation thread"""
    thread_id: str
    created_at: datetime
    last_updated: datetime
    user_id: str = ""  # User who owns this thread
    title: str = ""
    summary: str = ""
    message_count: int = 0
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []

class StateBackend(ABC):
    """Abstract base class for state persistence backends"""
    
    @abstractmethod
    async def save_checkpoint(self, thread_id: str, checkpoint: Dict[str, Any], user_id: str = "") -> bool:
        """Save a checkpoint for a thread"""
        pass
    
    @abstractmethod
    async def load_checkpoint(self, thread_id: str, user_id: str = "") -> Optional[Dict[str, Any]]:
        """Load the latest checkpoint for a thread"""
        pass
    
    @abstractmethod
    async def list_threads(self, user_id: str = "", limit: int = 100) -> List[ThreadMetadata]:
        """List all available threads for a user"""
        pass
    
    @abstractmethod
    async def delete_thread(self, thread_id: str, user_id: str = "") -> bool:
        """Delete a thread and all its checkpoints"""
        pass
    
    @abstractmethod
    async def cleanup_old_threads(self, days: int = 30, user_id: str = "") -> int:
        """Clean up threads older than specified days"""
        pass

class SQLiteStateBackend(StateBackend):
    """SQLite implementation for state persistence"""
    
    def __init__(self, db_path: str, table_name: str = "langgraph_threads"):
        self.db_path = db_path
        self.table_name = table_name
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            # Create threads metadata table
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name}_metadata (
                    thread_id TEXT PRIMARY KEY,
                    user_id TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    title TEXT DEFAULT '',
                    summary TEXT DEFAULT '',
                    message_count INTEGER DEFAULT 0,
                    tags TEXT DEFAULT '[]'
                )
            """)
            
            # Create checkpoints table
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name}_checkpoints (
                    thread_id TEXT,
                    user_id TEXT DEFAULT '',
                    checkpoint_id TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    checkpoint_data TEXT,
                    PRIMARY KEY (thread_id, checkpoint_id),
                    FOREIGN KEY (thread_id) REFERENCES {self.table_name}_metadata(thread_id)
                )
            """)
            
            # Create index for user-based queries
            conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_user_id 
                ON {self.table_name}_metadata(user_id)
            """)
            
            conn.commit()
    
    async def save_checkpoint(self, thread_id: str, checkpoint: Dict[str, Any], user_id: str = "") -> bool:
        """Save checkpoint to SQLite"""
        try:
            checkpoint_id = str(datetime.now().timestamp())
            checkpoint_json = json.dumps(checkpoint)
            
            with sqlite3.connect(self.db_path) as conn:
                # Update or insert thread metadata
                conn.execute(f"""
                    INSERT OR REPLACE INTO {self.table_name}_metadata 
                    (thread_id, user_id, last_updated, message_count)
                    VALUES (?, ?, ?, COALESCE((SELECT message_count + 1 FROM {self.table_name}_metadata WHERE thread_id = ?), 1))
                """, (thread_id, user_id, datetime.now(), thread_id))
                
                # Save checkpoint
                conn.execute(f"""
                    INSERT INTO {self.table_name}_checkpoints 
                    (thread_id, user_id, checkpoint_id, checkpoint_data)
                    VALUES (?, ?, ?, ?)
                """, (thread_id, user_id, checkpoint_id, checkpoint_json))
                
                conn.commit()
            return True
        except Exception as e:
            logging.error(f"Failed to save checkpoint for {thread_id}: {e}")
            return False
    
    async def load_checkpoint(self, thread_id: str, user_id: str = "") -> Optional[Dict[str, Any]]:
        """Load latest checkpoint from SQLite"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Add user_id filter if provided
                if user_id:
                    cursor = conn.execute(f"""
                        SELECT checkpoint_data FROM {self.table_name}_checkpoints 
                        WHERE thread_id = ? AND user_id = ?
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """, (thread_id, user_id))
                else:
                    cursor = conn.execute(f"""
                        SELECT checkpoint_data FROM {self.table_name}_checkpoints 
                        WHERE thread_id = ? 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """, (thread_id,))
                
                row = cursor.fetchone()
                if row:
                    return json.loads(row[0])
                return None
        except Exception as e:
            logging.error(f"Failed to load checkpoint for {thread_id}: {e}")
            return None
    
    async def list_threads(self, user_id: str = "", limit: int = 100) -> List[ThreadMetadata]:
        """List threads from SQLite"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if user_id:
                    cursor = conn.execute(f"""
                        SELECT thread_id, user_id, created_at, last_updated, title, summary, message_count, tags
                        FROM {self.table_name}_metadata 
                        WHERE user_id = ?
                        ORDER BY last_updated DESC 
                        LIMIT ?
                    """, (user_id, limit))
                else:
                    cursor = conn.execute(f"""
                        SELECT thread_id, user_id, created_at, last_updated, title, summary, message_count, tags
                        FROM {self.table_name}_metadata 
                        ORDER BY last_updated DESC 
                        LIMIT ?
                    """, (limit,))
                
                threads = []
                for row in cursor.fetchall():
                    threads.append(ThreadMetadata(
                        thread_id=row[0],
                        user_id=row[1] or "",
                        created_at=datetime.fromisoformat(row[2]),
                        last_updated=datetime.fromisoformat(row[3]),
                        title=row[4] or f"Thread {row[0][:8]}",
                        summary=row[5],
                        message_count=row[6],
                        tags=json.loads(row[7]) if row[7] else []
                    ))
                return threads
        except Exception as e:
            logging.error(f"Failed to list threads: {e}")
            return []
    
    async def delete_thread(self, thread_id: str, user_id: str = "") -> bool:
        """Delete thread from SQLite"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if user_id:
                    # Only delete if user owns the thread
                    conn.execute(f"DELETE FROM {self.table_name}_checkpoints WHERE thread_id = ? AND user_id = ?", (thread_id, user_id))
                    conn.execute(f"DELETE FROM {self.table_name}_metadata WHERE thread_id = ? AND user_id = ?", (thread_id, user_id))
                else:
                    # Admin delete - no user restriction
                    conn.execute(f"DELETE FROM {self.table_name}_checkpoints WHERE thread_id = ?", (thread_id,))
                    conn.execute(f"DELETE FROM {self.table_name}_metadata WHERE thread_id = ?", (thread_id,))
                conn.commit()
            return True
        except Exception as e:
            logging.error(f"Failed to delete thread {thread_id}: {e}")
            return False
    
    async def cleanup_old_threads(self, days: int = 30, user_id: str = "") -> int:
        """Clean up old threads from SQLite"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            with sqlite3.connect(self.db_path) as conn:
                if user_id:
                    cursor = conn.execute(f"""
                        SELECT thread_id FROM {self.table_name}_metadata 
                        WHERE last_updated < ? AND user_id = ?
                    """, (cutoff_date, user_id))
                else:
                    cursor = conn.execute(f"""
                        SELECT thread_id FROM {self.table_name}_metadata 
                        WHERE last_updated < ?
                    """, (cutoff_date,))
                
                old_threads = [row[0] for row in cursor.fetchall()]
                
                for thread_id in old_threads:
                    await self.delete_thread(thread_id, user_id)
                
                return len(old_threads)
        except Exception as e:
            logging.error(f"Failed to cleanup old threads: {e}")
            return 0

class InMemoryStateBackend(StateBackend):
    """In-memory implementation for state persistence (fallback)"""
    
    def __init__(self):
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
        self.metadata: Dict[str, ThreadMetadata] = {}
    
    async def save_checkpoint(self, thread_id: str, checkpoint: Dict[str, Any], user_id: str = "") -> bool:
        """Save checkpoint to memory"""
        self.checkpoints[thread_id] = checkpoint
        
        if thread_id not in self.metadata:
            self.metadata[thread_id] = ThreadMetadata(
                thread_id=thread_id,
                user_id=user_id,
                created_at=datetime.now(),
                last_updated=datetime.now(),
                title=f"Thread {thread_id[:8]}"
            )
        else:
            self.metadata[thread_id].last_updated = datetime.now()
            self.metadata[thread_id].message_count += 1
            if user_id and not self.metadata[thread_id].user_id:
                self.metadata[thread_id].user_id = user_id
        
        return True
    
    async def load_checkpoint(self, thread_id: str, user_id: str = "") -> Optional[Dict[str, Any]]:
        """Load checkpoint from memory"""
        if user_id:
            # Check if user owns the thread
            meta = self.metadata.get(thread_id)
            if meta and meta.user_id != user_id:
                return None
        return self.checkpoints.get(thread_id)
    
    async def list_threads(self, user_id: str = "", limit: int = 100) -> List[ThreadMetadata]:
        """List threads from memory"""
        if user_id:
            threads = [meta for meta in self.metadata.values() if meta.user_id == user_id]
        else:
            threads = list(self.metadata.values())
        
        threads.sort(key=lambda x: x.last_updated, reverse=True)
        return threads[:limit]
    
    async def delete_thread(self, thread_id: str, user_id: str = "") -> bool:
        """Delete thread from memory"""
        if user_id:
            # Check if user owns the thread
            meta = self.metadata.get(thread_id)
            if meta and meta.user_id != user_id:
                return False
        
        self.checkpoints.pop(thread_id, None)
        self.metadata.pop(thread_id, None)
        return True
    
    async def cleanup_old_threads(self, days: int = 30, user_id: str = "") -> int:
        """Clean up old threads from memory"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        if user_id:
            old_threads = [
                tid for tid, meta in self.metadata.items()
                if meta.last_updated < cutoff_date and meta.user_id == user_id
            ]
        else:
            old_threads = [
                tid for tid, meta in self.metadata.items()
                if meta.last_updated < cutoff_date
            ]
        
        for thread_id in old_threads:
            await self.delete_thread(thread_id, user_id)
        
        return len(old_threads)

class ThreadStateManager:
    """High-level thread state management"""
    
    def __init__(self, config: ThreadManagerConfig):
        self.config = config
        self.backend = self._create_backend()
        self.checkpointer = self._create_checkpointer()
    
    def _create_backend(self) -> StateBackend:
        """Create appropriate backend based on configuration"""
        
        if self.config.config.database_type == DatabaseType.SQLITE:
            db_path = self.config.config.connection_string
            if not db_path.endswith('.db'):
                db_path += '.db'
            return SQLiteStateBackend(db_path, self.config.config.table_name)
        
        elif self.config.config.database_type == DatabaseType.IN_MEMORY:
            return InMemoryStateBackend()
        
        else:
            # For other database types, fall back to in-memory for now
            # Future implementations would add PostgreSQL, Azure SQL, Cosmos DB
            logging.warning(f"Database type {self.config.config.database_type} not yet implemented, using in-memory")
            return InMemoryStateBackend()
    
    def _create_checkpointer(self) -> BaseCheckpointSaver:
        """Create LangGraph checkpointer"""
        
        if self.config.config.database_type == DatabaseType.SQLITE:
            # Note: For now using MemorySaver as SQLite checkpointer may not be available
            # TODO: Implement proper SQLite persistence when available
            print(f"⚠️  SQLite persistence not yet available, using memory-based storage")
            return MemorySaver()
        
        else:
            return MemorySaver()
    
    async def get_threads(self, user_id: str = "", limit: int = 100) -> List[ThreadMetadata]:
        """Get list of available threads"""
        return await self.backend.list_threads(user_id, limit)
    
    async def delete_thread(self, thread_id: str, user_id: str = "") -> bool:
        """Delete a thread"""
        return await self.backend.delete_thread(thread_id, user_id)
    
    async def cleanup_old_threads(self, days: int = None, user_id: str = "") -> int:
        """Clean up old threads"""
        days = days or self.config.config.auto_cleanup_days
        return await self.backend.cleanup_old_threads(days, user_id)
    
    def get_checkpointer(self) -> BaseCheckpointSaver:
        """Get the LangGraph checkpointer"""
        return self.checkpointer
    
    def generate_thread_id(self, suffix: str = None, user_id: str = "") -> str:
        """Generate a new thread ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        thread_id = f"{self.config.get_thread_prefix()}-{timestamp}"
        if user_id:
            # Include user identifier in thread ID for uniqueness
            user_short = user_id.split('@')[0].replace('-', '')[:8]
            thread_id += f"-{user_short}"
        if suffix:
            thread_id += f"-{suffix}"
        return thread_id
    
    async def get_thread_summary(self, thread_id: str, user_id: str = "") -> Dict[str, Any]:
        """Get summary information for a thread"""
        threads = await self.get_threads(user_id)
        for thread in threads:
            if thread.thread_id == thread_id:
                return {
                    "thread_id": thread.thread_id,
                    "user_id": thread.user_id,
                    "title": thread.title,
                    "summary": thread.summary,
                    "created_at": thread.created_at.isoformat(),
                    "last_updated": thread.last_updated.isoformat(),
                    "message_count": thread.message_count,
                    "tags": thread.tags
                }
        return None

# Global state manager instance
_state_manager: Optional[ThreadStateManager] = None

def get_state_manager() -> ThreadStateManager:
    """Get the global state manager instance"""
    global _state_manager
    if _state_manager is None:
        _state_manager = ThreadStateManager(get_thread_config())
    return _state_manager

if __name__ == "__main__":
    # Test state manager
    async def test_state_manager():
        manager = get_state_manager()
        
        print("State Manager Configuration:")
        print(f"Database Type: {manager.config.config.database_type}")
        print(f"Persistence Enabled: {manager.config.should_persist_threads()}")
        
        # Test thread listing
        threads = await manager.get_threads()
        print(f"\nExisting threads: {len(threads)}")
        for thread in threads[:5]:  # Show first 5
            print(f"  - {thread.thread_id}: {thread.title} ({thread.message_count} messages)")
    
    asyncio.run(test_state_manager())