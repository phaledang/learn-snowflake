#!/usr/bin/env python3
"""
MongoDB (Cosmos DB for MongoDB) Checkpointer for Lab 07c
Persistent storage for LangGraph checkpoint data using Cosmos DB MongoDB API
"""

import json
import logging
import pickle
import base64
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple
from datetime import datetime

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, DuplicateKeyError
except ImportError:
    print("ERROR: pymongo not installed.")
    print("Install with: pip install pymongo")
    raise

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata, CheckpointTuple
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

logger = logging.getLogger(__name__)


class MongoDBCheckpointSaver(BaseCheckpointSaver):
    """MongoDB/Cosmos DB MongoDB API-based checkpoint saver with user isolation"""
    
    def __init__(
        self,
        connection_string: str,
        database_name: str = "langgraph_db",
        collection_name: str = "checkpoints",
        serde: Optional[SerializerProtocol] = None,
        user_id: str = ""
    ):
        """
        Initialize MongoDB checkpoint saver
        
        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database
            collection_name: Name of the collection for checkpoints
            serde: Serializer protocol
            user_id: User ID for isolation
        """
        # Use JsonPlusSerializer as default if no serializer provided
        if serde is None:
            serde = JsonPlusSerializer()
        
        super().__init__(serde=serde)
        self.user_id = user_id
        self.database_name = database_name
        self.collection_name = collection_name
        
        # Initialize MongoDB client
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        
        # Create indexes
        self._setup_indexes()
        
        logger.info(f"MongoDB Checkpointer initialized for user: {user_id}")
    
    def _setup_indexes(self):
        """Create indexes for better query performance"""
        try:
            # Index on user_id and thread_id
            self.collection.create_index([
                ("user_id", 1),
                ("thread_id", 1),
                ("created_at", -1)
            ])
            
            # Index on checkpoint_id for point reads
            self.collection.create_index([
                ("user_id", 1),
                ("thread_id", 1),
                ("checkpoint_id", 1)
            ], unique=True)
            
            logger.debug("MongoDB indexes created successfully")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    def _serialize_checkpoint(self, checkpoint: Checkpoint) -> str:
        """Serialize checkpoint to base64-encoded string"""
        try:
            # Use serde's dumps_typed method for proper serialization
            serialized = self.serde.dumps_typed(checkpoint)
            # dumps_typed returns (bytes, type_string) tuple
            if isinstance(serialized, tuple):
                return base64.b64encode(serialized[0]).decode('utf-8')
            # Or it might return bytes directly
            elif isinstance(serialized, bytes):
                return base64.b64encode(serialized).decode('utf-8')
            else:
                # Fallback to pickle
                serialized = pickle.dumps(checkpoint)
                return base64.b64encode(serialized).decode('utf-8')
        except Exception:
            # Fallback to pickle if serde doesn't work
            serialized = pickle.dumps(checkpoint)
            return base64.b64encode(serialized).decode('utf-8')
    
    def _deserialize_checkpoint(self, data: str) -> Checkpoint:
        """Deserialize checkpoint from base64-encoded string"""
        try:
            decoded = base64.b64decode(data.encode('utf-8'))
            # Use serde's loads_typed method for proper deserialization
            return self.serde.loads_typed((decoded, "json"))
        except Exception:
            # Fallback to pickle if serde doesn't work
            decoded = base64.b64decode(data.encode('utf-8'))
            return pickle.loads(decoded)
    
    def _serialize_value(self, value: Any) -> str:
        """Serialize a value to base64-encoded pickle"""
        serialized = pickle.dumps(value)
        return base64.b64encode(serialized).decode('utf-8')
    
    def _deserialize_value(self, data: str) -> Any:
        """Deserialize a value from base64-encoded pickle"""
        decoded = base64.b64decode(data.encode('utf-8'))
        return pickle.loads(decoded)
    
    def get_tuple(self, config: Dict[str, Any]) -> Optional[CheckpointTuple]:
        """
        Get a checkpoint tuple from MongoDB
        
        Args:
            config: Configuration with thread_id and optionally checkpoint_id
        
        Returns:
            CheckpointTuple or None if not found
        """
        thread_id = config.get("configurable", {}).get("thread_id")
        checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
        
        if not thread_id:
            return None
        
        try:
            # Build query
            query = {
                "user_id": self.user_id,
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns
            }
            
            if checkpoint_id:
                # Point read
                query["checkpoint_id"] = checkpoint_id
                document = self.collection.find_one(query)
            else:
                # Get latest checkpoint
                document = self.collection.find_one(
                    query,
                    sort=[("created_at", -1)]
                )
            
            if document:
                return self._document_to_tuple(document)
            return None
                
        except Exception as e:
            logger.error(f"Error getting checkpoint: {e}")
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
        List checkpoints from MongoDB
        
        Args:
            config: Configuration with optional thread_id
            filter: Additional filters
            before: Get checkpoints before this configuration
            limit: Maximum number of results
        
        Yields:
            CheckpointTuple objects
        """
        thread_id = config.get("configurable", {}).get("thread_id") if config else None
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "") if config else ""
        
        # Build query
        query = {"user_id": self.user_id}
        
        if thread_id:
            query["thread_id"] = thread_id
        
        if checkpoint_ns is not None:
            query["checkpoint_ns"] = checkpoint_ns
        
        try:
            # Execute query
            cursor = self.collection.find(query).sort("created_at", -1)
            
            if limit:
                cursor = cursor.limit(limit)
            
            for document in cursor:
                yield self._document_to_tuple(document)
                
        except Exception as e:
            logger.error(f"Error listing checkpoints: {e}")
    
    def put(
        self,
        config: Dict[str, Any],
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save a checkpoint to MongoDB
        
        Args:
            config: Configuration with thread_id
            checkpoint: Checkpoint object to save
            metadata: Checkpoint metadata
            new_versions: New version information
        
        Returns:
            Updated configuration
        """
        thread_id = config.get("configurable", {}).get("thread_id")
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
        parent_checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        
        if not thread_id:
            raise ValueError("thread_id is required in config")
        
        checkpoint_id = checkpoint["id"]
        
        # Create document
        document = {
            "user_id": self.user_id,
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_checkpoint_id,
            "type": "checkpoint",
            "checkpoint_data": self._serialize_checkpoint(checkpoint),
            "metadata": metadata or {},
            "writes": [],  # Will be populated by put_writes
            "created_at": datetime.utcnow()
        }
        
        try:
            # Upsert document
            self.collection.replace_one(
                {
                    "user_id": self.user_id,
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id
                },
                document,
                upsert=True
            )
            
            logger.debug(f"Saved checkpoint {checkpoint_id} for thread {thread_id}")
            
            # Return updated config with checkpoint_id
            return {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id
                }
            }
            
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
            raise
    
    def put_writes(
        self,
        config: Dict[str, Any],
        writes: Sequence[Tuple[str, Any]],
        task_id: str
    ) -> None:
        """
        Save checkpoint writes (embedded in checkpoint document)
        
        Args:
            config: Configuration with thread_id and checkpoint_id
            writes: Sequence of (channel, value) tuples
            task_id: Task identifier
        """
        thread_id = config.get("configurable", {}).get("thread_id")
        checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
        
        if not thread_id or not checkpoint_id:
            return
        
        try:
            # Find and update document
            write_entries = []
            for idx, (channel, value) in enumerate(writes):
                write_entry = {
                    "task_id": task_id,
                    "idx": idx,
                    "channel": channel,
                    "type": "write",
                    "value": self._serialize_value(value),
                    "created_at": datetime.utcnow()
                }
                write_entries.append(write_entry)
            
            # Append writes using $push
            self.collection.update_one(
                {
                    "user_id": self.user_id,
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id
                },
                {
                    "$push": {
                        "writes": {"$each": write_entries}
                    }
                }
            )
            
            logger.debug(f"Saved {len(writes)} writes for checkpoint {checkpoint_id}")
            
        except Exception as e:
            logger.error(f"Error saving writes: {e}")
    
    def _document_to_tuple(self, document: Dict[str, Any]) -> CheckpointTuple:
        """Convert MongoDB document to CheckpointTuple"""
        checkpoint = self._deserialize_checkpoint(document["checkpoint_data"])
        metadata = document.get("metadata", {})
        
        parent_config = None
        if document.get("parent_checkpoint_id"):
            parent_config = {
                "configurable": {
                    "thread_id": document["thread_id"],
                    "checkpoint_ns": document.get("checkpoint_ns", ""),
                    "checkpoint_id": document["parent_checkpoint_id"]
                }
            }
        
        config = {
            "configurable": {
                "thread_id": document["thread_id"],
                "checkpoint_ns": document.get("checkpoint_ns", ""),
                "checkpoint_id": document["checkpoint_id"]
            }
        }
        
        return CheckpointTuple(
            config=config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config
        )
    
    async def aget_tuple(self, config: Dict[str, Any]) -> Optional[CheckpointTuple]:
        """Async version of get_tuple"""
        return self.get_tuple(config)
    
    async def aput(
        self,
        config: Dict[str, Any],
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Async version of put"""
        return self.put(config, checkpoint, metadata, new_versions)
    
    async def aput_writes(
        self,
        config: Dict[str, Any],
        writes: Sequence[Tuple[str, Any]],
        task_id: str
    ) -> None:
        """Async version of put_writes"""
        return self.put_writes(config, writes, task_id)
