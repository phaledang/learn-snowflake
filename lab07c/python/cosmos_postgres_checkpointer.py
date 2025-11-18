#!/usr/bin/env python3
"""
Cosmos DB MongoDB Checkpointer following PostgreSQL 3-table pattern
Reference: https://github.com/langchain-ai/langgraph/blob/main/libs/checkpoint-postgres/langgraph/checkpoint/postgres/base.py

Collections:
- threads: Thread ownership and metadata (user_id, thread_id, thread_name, created_at, metadata)
- checkpoints: Main checkpoint metadata (like PostgreSQL checkpoints table)
- checkpoint_blobs: Channel values - large binary data (like PostgreSQL checkpoint_blobs table)
- checkpoint_writes: Pending writes for each checkpoint (like PostgreSQL checkpoint_writes table)
"""

import logging
import uuid
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple
from datetime import datetime

try:
    from pymongo import MongoClient
    from pymongo.errors import DuplicateKeyError
except ImportError:
    print("ERROR: pymongo not installed.")
    print("Install with: pip install pymongo")
    raise

from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

logger = logging.getLogger(__name__)


class CosmosDBPostgresStyleCheckpointer(BaseCheckpointSaver):
    """
    Cosmos DB MongoDB API checkpoint saver following PostgreSQL 3-table pattern.
    
    This implementation mirrors the PostgreSQL checkpoint structure with an additional
    threads collection for managing thread ownership:
    
    Collection 0: threads (Thread Management)
    - thread_id (primary key, GUID)
    - user_id (owner of the thread)
    - thread_name (auto-generated summary of first conversation, <255 chars)
    - created_at, updated_at
    - metadata (thread name, description, etc.)
    
    Collection 1: checkpoints
    - thread_id, checkpoint_ns, checkpoint_id (compound primary key)
    - parent_checkpoint_id
    - checkpoint (JSONB-like dict with channel_versions)
    - metadata
    
    Collection 2: checkpoint_blobs  
    - thread_id, checkpoint_ns, channel, version (compound primary key)
    - type, blob (serialized channel value)
    
    Collection 3: checkpoint_writes
    - thread_id, checkpoint_ns, checkpoint_id, task_id, idx (compound primary key)
    - channel, type, blob (serialized pending write)
    """
    
    def __init__(
        self,
        connection_string: str,
        database_name: str = "langgraph_db",
        serde: Optional[SerializerProtocol] = None,
        user_id: str = "default_user"
    ):
        """
        Initialize Cosmos DB checkpointer with PostgreSQL-style 3-collection structure
        
        Args:
            connection_string: MongoDB/Cosmos DB connection string
            database_name: Name of the database
            serde: Serializer protocol (default: JsonPlusSerializer)
            user_id: User ID for isolation (optional, for multi-user support)
        """
        # Use JsonPlusSerializer as default if no serializer provided
        if serde is None:
            serde = JsonPlusSerializer()
        
        super().__init__(serde=serde)
        self.user_id = user_id
        self.database_name = database_name
        
        # Initialize MongoDB client (connects to Cosmos DB MongoDB API)
        print("🔄 Initializing Cosmos DB checkpointer (PostgreSQL 3-collection pattern)...")
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        
        # 4 collections: threads management + PostgreSQL pattern (3 collections)
        self.threads = self.db["threads"]
        self.checkpoints = self.db["checkpoints"]
        self.checkpoint_blobs = self.db["checkpoint_blobs"]
        self.checkpoint_writes = self.db["checkpoint_writes"]
        
        # Setup indexes
        self._setup_indexes()
        
        print("✅ Cosmos DB checkpointer initialized successfully (PostgreSQL pattern)!")
        logger.info(f"Cosmos DB Checkpointer initialized with 4-collection structure for user: {user_id}")
    
    def _setup_indexes(self):
        """Create indexes matching PostgreSQL schema + threads collection"""
        try:
            # threads collection indexes
            self.threads.create_index([
                ("thread_id", 1)
            ], unique=True, name="threads_pk")
            
            self.threads.create_index([
                ("user_id", 1),
                ("created_at", -1)
            ], name="threads_user_id_idx")
            
            # checkpoints collection indexes
            # Primary key: (thread_id, checkpoint_ns, checkpoint_id)
            self.checkpoints.create_index([
                ("thread_id", 1),
                ("checkpoint_ns", 1),
                ("checkpoint_id", 1)
            ], unique=True, name="checkpoints_pk")
            
            # Index for thread queries
            self.checkpoints.create_index([
                ("thread_id", 1)
            ], name="checkpoints_thread_id_idx")
            
            # checkpoint_blobs collection indexes
            # Primary key: (thread_id, checkpoint_ns, channel, version)
            self.checkpoint_blobs.create_index([
                ("thread_id", 1),
                ("checkpoint_ns", 1),
                ("channel", 1),
                ("version", 1)
            ], unique=True, name="checkpoint_blobs_pk")
            
            self.checkpoint_blobs.create_index([
                ("thread_id", 1)
            ], name="checkpoint_blobs_thread_id_idx")
            
            # checkpoint_writes collection indexes
            # Primary key: (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
            self.checkpoint_writes.create_index([
                ("thread_id", 1),
                ("checkpoint_ns", 1),
                ("checkpoint_id", 1),
                ("task_id", 1),
                ("idx", 1)
            ], unique=True, name="checkpoint_writes_pk")
            
            self.checkpoint_writes.create_index([
                ("thread_id", 1)
            ], name="checkpoint_writes_thread_id_idx")
            
            logger.debug("Cosmos DB indexes created successfully")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    def _ensure_thread_exists(self, thread_id: str, user_id: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Ensure thread exists in threads collection
        Creates thread if it doesn't exist, updates user_id if changed
        
        Args:
            thread_id: Thread identifier
            user_id: User who owns this thread
            metadata: Optional thread metadata (name, description, etc.)
        """
        try:
            now = datetime.utcnow()
            
            # Try to find existing thread
            existing_thread = self.threads.find_one({"thread_id": thread_id})
            
            if existing_thread:
                # Update user_id if it changed (e.g., user logged in)
                if existing_thread.get("user_id") != user_id:
                    self.threads.update_one(
                        {"thread_id": thread_id},
                        {
                            "$set": {
                                "user_id": user_id,
                                "updated_at": now,
                                "updated_by": user_id
                            }
                        }
                    )
                    logger.info(f"Thread {thread_id} ownership transferred to user {user_id}")
                else:
                    # Just update timestamp
                    self.threads.update_one(
                        {"thread_id": thread_id},
                        {"$set": {"updated_at": now}}
                    )
            else:
                # Create new thread
                thread_doc = {
                    "thread_id": thread_id,
                    "user_id": user_id,
                    "thread_name": metadata.get("thread_name") if metadata else None,
                    "created_at": now,
                    "updated_at": now,
                    "metadata": metadata or {}
                }
                self.threads.insert_one(thread_doc)
                logger.info(f"Created new thread {thread_id} for user {user_id}")
        
        except Exception as e:
            logger.error(f"Error ensuring thread exists: {e}", exc_info=True)
    
    def get_thread_owner(self, thread_id: str) -> Optional[str]:
        """
        Get the owner (user_id) of a thread
        
        Args:
            thread_id: Thread identifier
        
        Returns:
            user_id of the thread owner, or None if thread doesn't exist
        """
        try:
            thread = self.threads.find_one({"thread_id": thread_id})
            return thread.get("user_id") if thread else None
        except Exception as e:
            logger.error(f"Error getting thread owner: {e}")
            return None
    
    def list_user_threads(self, user_id: str, assistant_id: Optional[str] = None, limit: Optional[int] = None) -> list:
        """
        List all threads owned by a specific user
        
        Args:
            user_id: User identifier
            assistant_id: Optional assistant identifier to filter by
            limit: Maximum number of threads to return
        
        Returns:
            List of thread documents
        """
        try:
            query = {"user_id": user_id}
            if assistant_id:
                query["assistant_id"] = assistant_id
            
            # Sort by _id descending (MongoDB ObjectId contains timestamp)
            # This avoids indexing issues with Cosmos DB
            cursor = self.threads.find(query).sort("_id", -1)
            
            if limit:
                cursor = cursor.limit(limit)
            
            return list(cursor)
        
        except Exception as e:
            logger.error(f"Error listing user threads: {e}")
            return []
    
    def delete_thread(self, thread_id: str, user_id: Optional[str] = None) -> bool:
        """
        Delete a thread and all its associated data
        
        Args:
            thread_id: Thread identifier
            user_id: Optional user_id for ownership verification
        
        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            # Verify ownership if user_id provided
            if user_id:
                owner = self.get_thread_owner(thread_id)
                if owner != user_id:
                    logger.warning(f"User {user_id} attempted to delete thread {thread_id} owned by {owner}")
                    return False
            
            # Delete from all collections
            self.threads.delete_one({"thread_id": thread_id})
            self.checkpoints.delete_many({"thread_id": thread_id})
            self.checkpoint_blobs.delete_many({"thread_id": thread_id})
            self.checkpoint_writes.delete_many({"thread_id": thread_id})
            
            logger.info(f"Deleted thread {thread_id} and all associated data")
            return True
        
        except Exception as e:
            logger.error(f"Error deleting thread: {e}")
            return False
    
    def create_new_thread(self, user_id: Optional[str] = None, assistant_id: Optional[str] = None) -> str:
        """
        Create a new thread with a GUID-based thread_id
        
        Args:
            user_id: Optional user identifier (defaults to self.user_id)
            assistant_id: Optional assistant identifier
        
        Returns:
            New thread_id (GUID string)
        """
        thread_id = str(uuid.uuid4())
        owner_id = user_id or self.user_id
        
        try:
            now = datetime.utcnow()
            thread_doc = {
                "thread_id": thread_id,
                "user_id": owner_id,
                "assistant_id": assistant_id,
                "thread_name": None,  # Will be set after first message
                "created_at": now,
                "updated_at": now,
                "metadata": {}
            }
            self.threads.insert_one(thread_doc)
            logger.info(f"Created new thread {thread_id} for user {owner_id} with assistant {assistant_id}")
            return thread_id
        
        except Exception as e:
            logger.error(f"Error creating new thread: {e}")
            raise
    
    def update_thread_name(self, thread_id: str, thread_name: str) -> bool:
        """
        Update the thread name
        
        Args:
            thread_id: Thread identifier
            thread_name: New thread name (<255 chars)
        
        Returns:
            True if updated successfully, False otherwise
        """
        try:
            # Truncate to 255 chars if needed
            thread_name = thread_name[:255] if thread_name else None
            
            result = self.threads.update_one(
                {"thread_id": thread_id},
                {
                    "$set": {
                        "thread_name": thread_name,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated thread {thread_id} name to: {thread_name}")
                return True
            return False
        
        except Exception as e:
            logger.error(f"Error updating thread name: {e}")
            return False
    
    def generate_thread_name(self, question: str, response: str, llm_client=None) -> str:
        """
        Generate a thread name from the first question and response
        
        Args:
            question: First user question
            response: Assistant's response
            llm_client: Optional LLM client for summarization (if None, uses simple truncation)
        
        Returns:
            Thread name (<255 chars)
        """
        try:
            if llm_client:
                # Use LLM to generate a concise summary
                prompt = f"""Summarize this conversation in a very short title (max 60 characters).
                
Question: {question}
Response: {response}

Generate only the title, no quotes or extra text."""
                
                try:
                    summary = llm_client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=30,
                        temperature=0.3
                    )
                    thread_name = summary.choices[0].message.content.strip()
                    # Remove quotes if present
                    thread_name = thread_name.strip('"\'')
                except Exception as llm_error:
                    logger.warning(f"LLM summarization failed: {llm_error}, using fallback")
                    thread_name = question[:60]
            else:
                # Simple fallback: use first question truncated
                thread_name = question[:60]
            
            # Ensure it's under 255 chars
            return thread_name[:255]
        
        except Exception as e:
            logger.error(f"Error generating thread name: {e}")
            return "New Conversation"
            return False
    
    def get_tuple(self, config: Dict[str, Any]) -> Optional[CheckpointTuple]:
        """
        Get a checkpoint tuple from Cosmos DB (joins data from 3 collections)
        
        This mimics PostgreSQL's SELECT with JOINs to reconstruct the full checkpoint
        from checkpoints, checkpoint_blobs, and checkpoint_writes collections.
        
        Args:
            config: Configuration with thread_id and optionally checkpoint_id
        
        Returns:
            CheckpointTuple or None if not found
        """
        thread_id = config.get("configurable", {}).get("thread_id")
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
        checkpoint_id = get_checkpoint_id(config)
        
        if not thread_id:
            return None
        
        try:
            # 1. Get checkpoint from checkpoints collection
            query = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns
            }
            
            if checkpoint_id:
                query["checkpoint_id"] = checkpoint_id
                checkpoint_doc = self.checkpoints.find_one(query)
            else:
                # Get latest checkpoint (sort by _id descending)
                checkpoint_doc = self.checkpoints.find_one(
                    query,
                    sort=[("_id", -1)]
                )
            
            if not checkpoint_doc:
                return None
            
            # 2. Get channel values from checkpoint_blobs collection
            # Join on thread_id, checkpoint_ns, and channel/version from channel_versions
            channel_versions = checkpoint_doc["checkpoint"].get("channel_versions", {})
            channel_values = {}
            
            for channel, version in channel_versions.items():
                blob_doc = self.checkpoint_blobs.find_one({
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "channel": channel,
                    "version": version
                })
                
                if blob_doc:
                    # Deserialize the blob
                    # NOTE: Both dumps_typed and loads_typed use (type, blob) order
                    blob_type = blob_doc["type"]
                    blob_data = blob_doc["blob"]
                    
                    # Ensure type is a string
                    if isinstance(blob_type, bytes):
                        blob_type = blob_type.decode('utf-8')
                    
                    # loads_typed expects (type, blob) order - same as dumps_typed
                    channel_values[channel] = self.serde.loads_typed(
                        (blob_type, blob_data)
                    )
            
            # 3. Get pending writes from checkpoint_writes collection
            # Join on thread_id, checkpoint_ns, checkpoint_id
            try:
                writes_docs = self.checkpoint_writes.find({
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_doc["checkpoint_id"]
                }).sort([("task_id", 1), ("idx", 1)])
                
                pending_writes = []
                for write_doc in writes_docs:
                    task_id = write_doc["task_id"]
                    channel = write_doc["channel"]
                    
                    # Ensure type is a string (Cosmos DB may return bytes)
                    write_type = write_doc["type"]
                    if isinstance(write_type, bytes):
                        write_type = write_type.decode('utf-8')
                    
                    # loads_typed expects (type, blob) order - same as dumps_typed
                    value = self.serde.loads_typed((write_type, write_doc["blob"]))
                    pending_writes.append((task_id, channel, value))
            except Exception as e:
                # Cosmos DB may not have composite index for sort
                # Fall back to empty pending writes (conversation history still works)
                # Silently handle this - it's a known Cosmos DB limitation
                pending_writes = []
            
            # 4. Reconstruct complete checkpoint
            checkpoint = checkpoint_doc["checkpoint"].copy()
            checkpoint["channel_values"] = channel_values
            
            # 5. Build config
            checkpoint_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_doc["checkpoint_id"]
                }
            }
            
            # 6. Build parent config
            parent_config = None
            if checkpoint_doc.get("parent_checkpoint_id"):
                parent_config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_doc["parent_checkpoint_id"]
                    }
                }
            
            return CheckpointTuple(
                config=checkpoint_config,
                checkpoint=checkpoint,
                metadata=checkpoint_doc.get("metadata", {}),
                parent_config=parent_config,
                pending_writes=pending_writes
            )
            
        except Exception as e:
            # Only log if it's not the known Cosmos DB composite index error
            error_msg = str(e)
            if "composite index" not in error_msg.lower():
                logger.error(f"Error getting checkpoint: {e}", exc_info=True)
            return None
    
    def list(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> Iterator[CheckpointTuple]:
        """
        List checkpoints from Cosmos DB
        
        Args:
            config: Configuration with optional thread_id
            filter: Metadata filter (matches metadata fields)
            before: Get checkpoints before this configuration
            limit: Maximum number of results
        
        Yields:
            CheckpointTuple objects
        """
        query = {}
        
        # Build query from config
        if config:
            thread_id = config.get("configurable", {}).get("thread_id")
            if thread_id:
                query["thread_id"] = thread_id
            
            checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns")
            if checkpoint_ns is not None:
                query["checkpoint_ns"] = checkpoint_ns
            
            checkpoint_id = get_checkpoint_id(config)
            if checkpoint_id:
                query["checkpoint_id"] = checkpoint_id
        
        # Add metadata filter (PostgreSQL uses @> operator, we use dot notation)
        if filter:
            for key, value in filter.items():
                query[f"metadata.{key}"] = value
        
        try:
            # Execute query (sort by _id descending for latest first)
            cursor = self.checkpoints.find(query).sort("_id", -1)
            
            if limit:
                cursor = cursor.limit(limit)
            
            for checkpoint_doc in cursor:
                # Build config for this checkpoint
                checkpoint_config = {
                    "configurable": {
                        "thread_id": checkpoint_doc["thread_id"],
                        "checkpoint_ns": checkpoint_doc["checkpoint_ns"],
                        "checkpoint_id": checkpoint_doc["checkpoint_id"]
                    }
                }
                
                # Use get_tuple to get full checkpoint with channel values and writes
                checkpoint_tuple = self.get_tuple(checkpoint_config)
                if checkpoint_tuple:
                    yield checkpoint_tuple
                    
        except Exception as e:
            logger.error(f"Error listing checkpoints: {e}", exc_info=True)
    
    def put(
        self,
        config: Dict[str, Any],
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save a checkpoint to Cosmos DB (PostgreSQL 3-collection pattern)
        
        This mimics PostgreSQL's INSERT/UPDATE to 3 tables:
        1. Upsert channel blobs to checkpoint_blobs
        2. Upsert checkpoint metadata to checkpoints
        
        Args:
            config: Configuration with thread_id
            checkpoint: Checkpoint object to save
            metadata: Checkpoint metadata
            new_versions: New version information for channels
        
        Returns:
            Updated configuration with checkpoint_id
        """
        thread_id = config.get("configurable", {}).get("thread_id")
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
        parent_checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        
        if not thread_id:
            raise ValueError("thread_id is required in config")
        
        checkpoint_id = checkpoint["id"]
        
        try:
            # 0. Ensure thread exists and is owned by current user
            self._ensure_thread_exists(thread_id, self.user_id, metadata)
            
            # 1. Save channel values to checkpoint_blobs collection
            channel_versions = {}
            for channel, value in checkpoint.get("channel_values", {}).items():
                version = new_versions.get(channel, checkpoint.get("channel_versions", {}).get(channel))
                if version:
                    channel_versions[channel] = version
                    
                    # Serialize the channel value
                    type_name, blob = self.serde.dumps_typed(value)
                    
                    # Upsert to checkpoint_blobs (ON CONFLICT DO NOTHING in PostgreSQL)
                    self.checkpoint_blobs.replace_one(
                        {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "channel": channel,
                            "version": version
                        },
                        {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "channel": channel,
                            "version": version,
                            "type": type_name,
                            "blob": blob
                        },
                        upsert=True
                    )
            
            # 2. Save checkpoint metadata to checkpoints collection
            # Remove channel_values and pending_writes (stored separately)
            checkpoint_without_values = checkpoint.copy()
            checkpoint_without_values.pop("channel_values", None)
            checkpoint_without_values.pop("pending_writes", None)
            checkpoint_without_values["channel_versions"] = channel_versions
            
            checkpoint_doc = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "parent_checkpoint_id": parent_checkpoint_id,
                "type": "checkpoint",
                "checkpoint": checkpoint_without_values,
                "metadata": metadata or {},
                "created_at": datetime.utcnow()
            }
            
            # Upsert to checkpoints (ON CONFLICT DO UPDATE in PostgreSQL)
            self.checkpoints.replace_one(
                {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id
                },
                checkpoint_doc,
                upsert=True
            )
            
            logger.debug(f"Saved checkpoint {checkpoint_id} for thread {thread_id}")
            
            # Auto-generate thread name on first checkpoint (if thread has no name)
            try:
                thread = self.threads.find_one({"thread_id": thread_id})
                if thread and not thread.get("thread_name"):
                    # Check if this is the first checkpoint with messages
                    messages_channel = checkpoint.get("channel_values", {}).get("messages", [])
                    if messages_channel and len(messages_channel) >= 2:
                        # Extract first question and response
                        question = None
                        response = None
                        
                        for msg in messages_channel:
                            msg_type = getattr(msg, '__class__', None)
                            if msg_type:
                                type_name = msg_type.__name__
                                if type_name == "HumanMessage" and not question:
                                    question = getattr(msg, 'content', '')
                                elif type_name == "AIMessage" and question and not response:
                                    response = getattr(msg, 'content', '')
                                    break
                        
                        if question and response:
                            # Generate thread name (simple truncation, LLM integration can be added later)
                            thread_name = self.generate_thread_name(question, response, llm_client=None)
                            self.update_thread_name(thread_id, thread_name)
                            logger.info(f"Auto-generated thread name: {thread_name}")
            except Exception as name_error:
                logger.warning(f"Failed to auto-generate thread name: {name_error}")
            
            # Return updated config
            return {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id
                }
            }
            
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}", exc_info=True)
            raise
    
    def put_writes(
        self,
        config: Dict[str, Any],
        writes: Sequence[Tuple[str, Any]],
        task_id: str
    ) -> None:
        """
        Save checkpoint writes to checkpoint_writes collection
        
        This mimics PostgreSQL's INSERT into checkpoint_writes table.
        
        Args:
            config: Configuration with thread_id and checkpoint_id
            writes: Sequence of (channel, value) tuples
            task_id: Task ID for the writes
        """
        thread_id = config.get("configurable", {}).get("thread_id")
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
        checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        
        if not thread_id or not checkpoint_id:
            raise ValueError("thread_id and checkpoint_id are required in config")
        
        try:
            for idx, (channel, value) in enumerate(writes):
                # Serialize the value
                type_name, blob = self.serde.dumps_typed(value)
                
                # Upsert to checkpoint_writes
                # (ON CONFLICT DO UPDATE in PostgreSQL)
                self.checkpoint_writes.replace_one(
                    {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                        "task_id": task_id,
                        "idx": idx
                    },
                    {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                        "task_id": task_id,
                        "idx": idx,
                        "channel": channel,
                        "type": type_name,
                        "blob": blob,
                        "created_at": datetime.utcnow()
                    },
                    upsert=True
                )
            
            logger.debug(f"Saved {len(writes)} writes for checkpoint {checkpoint_id}, task {task_id}")
            
        except Exception as e:
            logger.error(f"Error saving writes: {e}", exc_info=True)
            raise
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("Cosmos DB connection closed")
