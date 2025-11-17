#!/usr/bin/env python3
"""
Cosmos DB Checkpointer for Lab 07c
Persistent storage for LangGraph checkpoint data using Azure Cosmos DB
"""

import json
import logging
import pickle
import base64
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple
from datetime import datetime
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.identity import DefaultAzureCredential

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata, CheckpointTuple
from langgraph.checkpoint.serde.base import SerializerProtocol

logger = logging.getLogger(__name__)


class CosmosDBCheckpointSaver(BaseCheckpointSaver):
    """Azure Cosmos DB-based checkpoint saver with user isolation"""
    
    def __init__(
        self,
        endpoint: str,
        database_name: str = "langgraph_db",
        container_name: str = "checkpoints",
        key: Optional[str] = None,
        credential: Optional[Any] = None,
        serde: Optional[SerializerProtocol] = None,
        user_id: str = "",
        use_hierarchical_partition: bool = True
    ):
        """
        Initialize Cosmos DB checkpoint saver
        
        Args:
            endpoint: Cosmos DB endpoint URL
            database_name: Name of the database
            container_name: Name of the container for checkpoints
            key: Account key (or use credential)
            credential: Azure credential (e.g., DefaultAzureCredential)
            serde: Serializer protocol
            user_id: User ID for isolation
            use_hierarchical_partition: Use hierarchical partition keys
        """
        super().__init__(serde=serde)
        self.user_id = user_id
        self.database_name = database_name
        self.container_name = container_name
        self.use_hierarchical_partition = use_hierarchical_partition
        
        # Initialize Cosmos client
        if credential:
            self.client = CosmosClient(endpoint, credential=credential)
        elif key:
            self.client = CosmosClient(endpoint, credential=key)
        else:
            # Try DefaultAzureCredential
            self.client = CosmosClient(endpoint, credential=DefaultAzureCredential())
        
        self.database = self.client.get_database_client(database_name)
        self.container = self.database.get_container_client(container_name)
        
        logger.info(f"Cosmos DB Checkpointer initialized for user: {user_id}")
    
    def _serialize_checkpoint(self, checkpoint: Checkpoint) -> str:
        """Serialize checkpoint to base64-encoded pickle"""
        serialized = self.serde.dumps(checkpoint)
        return base64.b64encode(serialized).decode('utf-8')
    
    def _deserialize_checkpoint(self, data: str) -> Checkpoint:
        """Deserialize checkpoint from base64-encoded pickle"""
        decoded = base64.b64decode(data.encode('utf-8'))
        return self.serde.loads(decoded)
    
    def _create_document_id(self, thread_id: str, checkpoint_id: str, checkpoint_ns: str = "") -> str:
        """Create unique document ID"""
        if checkpoint_ns:
            return f"{thread_id}_{checkpoint_ns}_{checkpoint_id}"
        return f"{thread_id}_{checkpoint_id}"
    
    def _get_partition_key(self, thread_id: str, checkpoint_ns: str = "") -> Any:
        """Get partition key value based on configuration"""
        if self.use_hierarchical_partition:
            # Hierarchical: [user_id, thread_id, checkpoint_ns]
            return [self.user_id, thread_id, checkpoint_ns or ""]
        else:
            # Simple: user_id only
            return self.user_id
    
    def get_tuple(self, config: Dict[str, Any]) -> Optional[CheckpointTuple]:
        """
        Get a checkpoint tuple from Cosmos DB
        
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
            if checkpoint_id:
                # Point read - most efficient
                doc_id = self._create_document_id(thread_id, checkpoint_id, checkpoint_ns)
                partition_key = self._get_partition_key(thread_id, checkpoint_ns)
                
                try:
                    response = self.container.read_item(
                        item=doc_id,
                        partition_key=partition_key
                    )
                    return self._document_to_tuple(response)
                except exceptions.CosmosResourceNotFoundError:
                    return None
            else:
                # Query for latest checkpoint
                query = """
                SELECT TOP 1 * FROM c 
                WHERE c.user_id = @user_id 
                AND c.thread_id = @thread_id 
                AND c.checkpoint_ns = @checkpoint_ns
                ORDER BY c.created_at DESC
                """
                parameters = [
                    {"name": "@user_id", "value": self.user_id},
                    {"name": "@thread_id", "value": thread_id},
                    {"name": "@checkpoint_ns", "value": checkpoint_ns}
                ]
                
                items = list(self.container.query_items(
                    query=query,
                    parameters=parameters,
                    partition_key=self._get_partition_key(thread_id, checkpoint_ns)
                ))
                
                if items:
                    return self._document_to_tuple(items[0])
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
        List checkpoints from Cosmos DB
        
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
        query_parts = ["SELECT * FROM c WHERE c.user_id = @user_id"]
        parameters = [{"name": "@user_id", "value": self.user_id}]
        
        if thread_id:
            query_parts.append("AND c.thread_id = @thread_id")
            parameters.append({"name": "@thread_id", "value": thread_id})
        
        if checkpoint_ns is not None:
            query_parts.append("AND c.checkpoint_ns = @checkpoint_ns")
            parameters.append({"name": "@checkpoint_ns", "value": checkpoint_ns})
        
        query_parts.append("ORDER BY c.created_at DESC")
        
        query = " ".join(query_parts)
        
        try:
            # Execute query
            partition_key = self._get_partition_key(thread_id, checkpoint_ns) if thread_id else None
            
            items = self.container.query_items(
                query=query,
                parameters=parameters,
                partition_key=partition_key,
                max_item_count=limit
            )
            
            count = 0
            for item in items:
                if limit and count >= limit:
                    break
                yield self._document_to_tuple(item)
                count += 1
                
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
        Save a checkpoint to Cosmos DB
        
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
        doc_id = self._create_document_id(thread_id, checkpoint_id, checkpoint_ns)
        
        # Create document
        document = {
            "id": doc_id,
            "user_id": self.user_id,
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": parent_checkpoint_id,
            "type": "checkpoint",
            "checkpoint_data": self._serialize_checkpoint(checkpoint),
            "metadata": metadata or {},
            "writes": [],  # Will be populated by put_writes
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        
        try:
            # Upsert document
            partition_key = self._get_partition_key(thread_id, checkpoint_ns)
            self.container.upsert_item(
                body=document,
                partition_key=partition_key
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
        
        doc_id = self._create_document_id(thread_id, checkpoint_id, checkpoint_ns)
        partition_key = self._get_partition_key(thread_id, checkpoint_ns)
        
        try:
            # Read existing document
            document = self.container.read_item(
                item=doc_id,
                partition_key=partition_key
            )
            
            # Append writes
            if "writes" not in document:
                document["writes"] = []
            
            for idx, (channel, value) in enumerate(writes):
                write_entry = {
                    "task_id": task_id,
                    "idx": idx,
                    "channel": channel,
                    "type": "write",
                    "value": self._serialize_value(value),
                    "created_at": datetime.utcnow().isoformat() + "Z"
                }
                document["writes"].append(write_entry)
            
            # Update document
            self.container.replace_item(
                item=doc_id,
                body=document,
                partition_key=partition_key
            )
            
            logger.debug(f"Saved {len(writes)} writes for checkpoint {checkpoint_id}")
            
        except Exception as e:
            logger.error(f"Error saving writes: {e}")
    
    def _serialize_value(self, value: Any) -> str:
        """Serialize a value to base64-encoded pickle"""
        serialized = pickle.dumps(value)
        return base64.b64encode(serialized).decode('utf-8')
    
    def _deserialize_value(self, data: str) -> Any:
        """Deserialize a value from base64-encoded pickle"""
        decoded = base64.b64decode(data.encode('utf-8'))
        return pickle.loads(decoded)
    
    def _document_to_tuple(self, document: Dict[str, Any]) -> CheckpointTuple:
        """Convert Cosmos DB document to CheckpointTuple"""
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
        # For now, use sync implementation
        # TODO: Implement with async Cosmos client
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
