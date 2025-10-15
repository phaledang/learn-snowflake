#!/usr/bin/env python3
"""
Custom SQLite Checkpointer for Lab 07c
Persistent storage for LangGraph checkpoint data with user isolation
"""

import sqlite3
import json
import pickle
import logging
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Sequence, Tuple
from contextlib import contextmanager
from datetime import datetime, timedelta

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata, CheckpointTuple
from langgraph.checkpoint.serde.base import SerializerProtocol

logger = logging.getLogger(__name__)

class SQLiteCheckpointSaver(BaseCheckpointSaver):
    """Custom SQLite-based checkpoint saver with user isolation"""
    
    def __init__(
        self, 
        db_path: str, 
        serde: Optional[SerializerProtocol] = None,
        user_id: str = ""
    ):
        super().__init__(serde=serde)
        self.db_path = db_path
        self.user_id = user_id
        self._setup_database()
    
    def _setup_database(self):
        """Initialize the SQLite database with required tables"""
        with self._get_connection() as conn:
            # Create checkpoints table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    thread_id TEXT NOT NULL,
                    checkpoint_ns TEXT NOT NULL DEFAULT '',
                    checkpoint_id TEXT NOT NULL,
                    user_id TEXT NOT NULL DEFAULT '',
                    parent_checkpoint_id TEXT,
                    type TEXT,
                    checkpoint BLOB NOT NULL,
                    metadata TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
                )
            """)
            
            # Create writes table for checkpoint writes
            conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoint_writes (
                    thread_id TEXT NOT NULL,
                    checkpoint_ns TEXT NOT NULL DEFAULT '',
                    checkpoint_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    idx INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    type TEXT,
                    value BLOB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
                )
            """)
            
            # Create indices for better performance
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_checkpoints_thread_user 
                ON checkpoints(thread_id, user_id, created_at DESC)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_checkpoints_user 
                ON checkpoints(user_id, created_at DESC)
            """)
            
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """Get database connection with proper error handling"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_tuple(self, config: Dict[str, Any]) -> Optional[CheckpointTuple]:
        """Get a specific checkpoint tuple"""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"].get("checkpoint_id")
        
        with self._get_connection() as conn:
            if checkpoint_id:
                # Get specific checkpoint
                result = conn.execute("""
                    SELECT checkpoint, metadata, parent_checkpoint_id 
                    FROM checkpoints 
                    WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ? AND user_id = ?
                """, (thread_id, checkpoint_ns, checkpoint_id, self.user_id)).fetchone()
            else:
                # Get latest checkpoint
                result = conn.execute("""
                    SELECT checkpoint, metadata, parent_checkpoint_id, checkpoint_id
                    FROM checkpoints 
                    WHERE thread_id = ? AND checkpoint_ns = ? AND user_id = ?
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, (thread_id, checkpoint_ns, self.user_id)).fetchone()
            
            if result:
                checkpoint_data = pickle.loads(result['checkpoint'])
                metadata = json.loads(result['metadata'])
                
                return CheckpointTuple(
                    config=config,
                    checkpoint=checkpoint_data,
                    metadata=metadata,
                    parent_config={"configurable": {
                        "thread_id": thread_id,
                        "checkpoint_id": result['parent_checkpoint_id']
                    }} if result['parent_checkpoint_id'] else None
                )
        
        return None
    
    def list(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints with filtering"""
        
        query = """
            SELECT thread_id, checkpoint_ns, checkpoint_id, checkpoint, metadata, parent_checkpoint_id
            FROM checkpoints 
            WHERE user_id = ?
        """
        params = [self.user_id]
        
        if config and "configurable" in config:
            if "thread_id" in config["configurable"]:
                query += " AND thread_id = ?"
                params.append(config["configurable"]["thread_id"])
            if "checkpoint_ns" in config["configurable"]:
                query += " AND checkpoint_ns = ?"
                params.append(config["configurable"]["checkpoint_ns"])
        
        if before and "configurable" in before and "checkpoint_id" in before["configurable"]:
            query += " AND created_at < (SELECT created_at FROM checkpoints WHERE checkpoint_id = ?)"
            params.append(before["configurable"]["checkpoint_id"])
        
        query += " ORDER BY created_at DESC"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        with self._get_connection() as conn:
            for row in conn.execute(query, params):
                checkpoint_data = pickle.loads(row['checkpoint'])
                metadata = json.loads(row['metadata'])
                
                config_dict = {
                    "configurable": {
                        "thread_id": row['thread_id'],
                        "checkpoint_ns": row['checkpoint_ns'],
                        "checkpoint_id": row['checkpoint_id']
                    }
                }
                
                yield CheckpointTuple(
                    config=config_dict,
                    checkpoint=checkpoint_data,
                    metadata=metadata,
                    parent_config={"configurable": {
                        "thread_id": row['thread_id'],
                        "checkpoint_id": row['parent_checkpoint_id']
                    }} if row['parent_checkpoint_id'] else None
                )
    
    def put(
        self,
        config: Dict[str, Any],
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Save a checkpoint"""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = checkpoint["id"]
        parent_checkpoint_id = config["configurable"].get("checkpoint_id")
        
        with self._get_connection() as conn:
            # Save checkpoint
            conn.execute("""
                INSERT OR REPLACE INTO checkpoints 
                (thread_id, checkpoint_ns, checkpoint_id, user_id, parent_checkpoint_id, 
                 type, checkpoint, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                thread_id,
                checkpoint_ns, 
                checkpoint_id,
                self.user_id,
                parent_checkpoint_id,
                "checkpoint",
                pickle.dumps(checkpoint),
                json.dumps(metadata),
                datetime.now()
            ))
            
            conn.commit()
        
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id
            }
        }
    
    def put_writes(
        self,
        config: Dict[str, Any],
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
    ) -> None:
        """Save checkpoint writes"""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]
        
        with self._get_connection() as conn:
            for idx, (channel, value) in enumerate(writes):
                conn.execute("""
                    INSERT OR REPLACE INTO checkpoint_writes 
                    (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    idx,
                    channel,
                    "write",
                    pickle.dumps(value)
                ))
            
            conn.commit()
    
    def get_threads(self, limit: int = 100) -> Iterator[Dict[str, Any]]:
        """Get list of thread IDs for the current user"""
        with self._get_connection() as conn:
            for row in conn.execute("""
                SELECT DISTINCT thread_id, MAX(created_at) as last_updated
                FROM checkpoints 
                WHERE user_id = ?
                GROUP BY thread_id
                ORDER BY last_updated DESC
                LIMIT ?
            """, (self.user_id, limit)):
                yield {
                    "thread_id": row['thread_id'],
                    "last_updated": row['last_updated']
                }

    def get_thread_message_count(self, thread_id: str) -> int:
        """Get the message count for a specific thread"""
        with self._get_connection() as conn:
            # Count unique checkpoints for this thread (each checkpoint represents a turn in conversation)
            result = conn.execute("""
                SELECT COUNT(DISTINCT checkpoint_id) as message_count
                FROM checkpoints 
                WHERE thread_id = ? AND user_id = ?
            """, (thread_id, self.user_id)).fetchone()
            
            return result['message_count'] if result else 0

    def get_threads_with_counts(self, limit: int = 100) -> Iterator[Dict[str, Any]]:
        """Get list of thread IDs with message counts for the current user"""
        with self._get_connection() as conn:
            for row in conn.execute("""
                SELECT 
                    thread_id, 
                    MAX(created_at) as last_updated,
                    COUNT(DISTINCT checkpoint_id) as message_count
                FROM checkpoints 
                WHERE user_id = ?
                GROUP BY thread_id
                ORDER BY last_updated DESC
                LIMIT ?
            """, (self.user_id, limit)):
                yield {
                    "thread_id": row['thread_id'],
                    "last_updated": row['last_updated'],
                    "message_count": row['message_count']
                }
    
    def cleanup_old_checkpoints(self, days: int = 30):
        """Clean up old checkpoints"""
        with self._get_connection() as conn:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            conn.execute("""
                DELETE FROM checkpoint_writes 
                WHERE created_at < ?
            """, (cutoff_date,))
            
            conn.execute("""
                DELETE FROM checkpoints 
                WHERE created_at < ?
            """, (cutoff_date,))
            
            conn.commit()
            
            logger.info(f"Cleaned up checkpoints older than {days} days")