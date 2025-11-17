#!/usr/bin/env python3
"""
Migrate LangGraph checkpoints from SQLite to Azure Cosmos DB
"""

import sqlite3
import pickle
import base64
import json
from typing import List, Dict, Any
from datetime import datetime
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.identity import DefaultAzureCredential
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLiteToCosmosDBMigrator:
    """Migrates checkpoint data from SQLite to Cosmos DB"""
    
    def __init__(
        self,
        sqlite_path: str,
        cosmos_endpoint: str,
        cosmos_key: str = None,
        database_name: str = "langgraph_db",
        container_name: str = "checkpoints",
        use_hierarchical_partition: bool = True
    ):
        self.sqlite_path = sqlite_path
        self.database_name = database_name
        self.container_name = container_name
        self.use_hierarchical_partition = use_hierarchical_partition
        
        # Initialize Cosmos client
        if cosmos_key:
            self.cosmos_client = CosmosClient(cosmos_endpoint, credential=cosmos_key)
        else:
            self.cosmos_client = CosmosClient(cosmos_endpoint, credential=DefaultAzureCredential())
        
        self.database = self.cosmos_client.get_database_client(database_name)
        self.container = self.database.get_container_client(container_name)
    
    def migrate(self, user_id: str = "", dry_run: bool = False) -> Dict[str, int]:
        """
        Migrate all checkpoints from SQLite to Cosmos DB
        
        Args:
            user_id: User ID to assign to migrated checkpoints
            dry_run: If True, only print what would be migrated
        
        Returns:
            Statistics about the migration
        """
        stats = {
            "checkpoints_migrated": 0,
            "writes_migrated": 0,
            "errors": 0
        }
        
        # Connect to SQLite
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Get all checkpoints
            cursor.execute("""
                SELECT * FROM checkpoints 
                ORDER BY thread_id, created_at
            """)
            checkpoints = cursor.fetchall()
            
            logger.info(f"Found {len(checkpoints)} checkpoints to migrate")
            
            # Group checkpoints by document
            documents = {}
            
            for checkpoint_row in checkpoints:
                thread_id = checkpoint_row["thread_id"]
                checkpoint_id = checkpoint_row["checkpoint_id"]
                checkpoint_ns = checkpoint_row["checkpoint_ns"] or ""
                
                # Create document ID
                doc_id = self._create_document_id(thread_id, checkpoint_id, checkpoint_ns)
                
                # Get associated writes
                cursor.execute("""
                    SELECT * FROM checkpoint_writes 
                    WHERE thread_id = ? AND checkpoint_id = ?
                    ORDER BY idx
                """, (thread_id, checkpoint_id))
                writes = cursor.fetchall()
                
                # Create document
                document = self._create_document(
                    checkpoint_row, 
                    writes, 
                    user_id or checkpoint_row.get("user_id", "")
                )
                
                documents[doc_id] = document
                
                if dry_run:
                    logger.info(f"Would migrate: {doc_id}")
                    logger.debug(f"  Thread: {thread_id}")
                    logger.debug(f"  Checkpoint: {checkpoint_id}")
                    logger.debug(f"  Writes: {len(writes)}")
            
            # Migrate to Cosmos DB
            if not dry_run:
                for doc_id, document in documents.items():
                    try:
                        partition_key = self._get_partition_key(
                            document["thread_id"],
                            document.get("checkpoint_ns", ""),
                            document["user_id"]
                        )
                        
                        self.container.upsert_item(
                            body=document,
                            partition_key=partition_key
                        )
                        
                        stats["checkpoints_migrated"] += 1
                        stats["writes_migrated"] += len(document.get("writes", []))
                        
                        logger.info(f"Migrated: {doc_id}")
                        
                    except Exception as e:
                        logger.error(f"Error migrating {doc_id}: {e}")
                        stats["errors"] += 1
            else:
                stats["checkpoints_migrated"] = len(documents)
                stats["writes_migrated"] = sum(len(d.get("writes", [])) for d in documents.values())
            
        finally:
            conn.close()
        
        return stats
    
    def _create_document_id(self, thread_id: str, checkpoint_id: str, checkpoint_ns: str = "") -> str:
        """Create unique document ID"""
        if checkpoint_ns:
            return f"{thread_id}_{checkpoint_ns}_{checkpoint_id}"
        return f"{thread_id}_{checkpoint_id}"
    
    def _get_partition_key(self, thread_id: str, checkpoint_ns: str, user_id: str) -> Any:
        """Get partition key value"""
        if self.use_hierarchical_partition:
            return [user_id, thread_id, checkpoint_ns or ""]
        else:
            return user_id
    
    def _create_document(
        self, 
        checkpoint_row: sqlite3.Row, 
        writes: List[sqlite3.Row],
        user_id: str
    ) -> Dict[str, Any]:
        """Create Cosmos DB document from SQLite rows"""
        
        thread_id = checkpoint_row["thread_id"]
        checkpoint_id = checkpoint_row["checkpoint_id"]
        checkpoint_ns = checkpoint_row["checkpoint_ns"] or ""
        
        # Serialize checkpoint blob to base64
        checkpoint_blob = checkpoint_row["checkpoint"]
        checkpoint_data = base64.b64encode(checkpoint_blob).decode('utf-8')
        
        # Parse metadata
        metadata = {}
        if checkpoint_row["metadata"]:
            try:
                metadata = json.loads(checkpoint_row["metadata"])
            except:
                metadata = {}
        
        # Create document
        document = {
            "id": self._create_document_id(thread_id, checkpoint_id, checkpoint_ns),
            "user_id": user_id,
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": checkpoint_row.get("parent_checkpoint_id"),
            "type": checkpoint_row.get("type", "checkpoint"),
            "checkpoint_data": checkpoint_data,
            "metadata": metadata,
            "writes": [],
            "created_at": checkpoint_row.get("created_at") or datetime.utcnow().isoformat() + "Z"
        }
        
        # Add writes
        for write in writes:
            write_entry = {
                "task_id": write["task_id"],
                "idx": write["idx"],
                "channel": write["channel"],
                "type": write.get("type", "write"),
                "value": base64.b64encode(write["value"]).decode('utf-8') if write["value"] else None,
                "created_at": write.get("created_at") or datetime.utcnow().isoformat() + "Z"
            }
            document["writes"].append(write_entry)
        
        return document
    
    def verify_migration(self, user_id: str = "") -> Dict[str, Any]:
        """
        Verify that migration was successful
        
        Returns:
            Verification results
        """
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        
        # Count SQLite records
        cursor.execute("SELECT COUNT(*) FROM checkpoints")
        sqlite_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM checkpoint_writes")
        sqlite_writes_count = cursor.fetchone()[0]
        
        conn.close()
        
        # Count Cosmos DB documents
        query = "SELECT VALUE COUNT(1) FROM c"
        if user_id:
            query = "SELECT VALUE COUNT(1) FROM c WHERE c.user_id = @user_id"
            parameters = [{"name": "@user_id", "value": user_id}]
            cosmos_count = list(self.container.query_items(query=query, parameters=parameters))[0]
        else:
            cosmos_count = list(self.container.query_items(query=query))[0]
        
        # Count total writes in Cosmos
        query = "SELECT SUM(ARRAY_LENGTH(c.writes)) as total_writes FROM c"
        if user_id:
            query = "SELECT SUM(ARRAY_LENGTH(c.writes)) as total_writes FROM c WHERE c.user_id = @user_id"
            parameters = [{"name": "@user_id", "value": user_id}]
            result = list(self.container.query_items(query=query, parameters=parameters))
        else:
            result = list(self.container.query_items(query=query))
        
        cosmos_writes_count = result[0]["total_writes"] if result and result[0].get("total_writes") else 0
        
        return {
            "sqlite_checkpoints": sqlite_count,
            "cosmos_checkpoints": cosmos_count,
            "sqlite_writes": sqlite_writes_count,
            "cosmos_writes": cosmos_writes_count,
            "checkpoints_match": sqlite_count == cosmos_count,
            "writes_match": sqlite_writes_count == cosmos_writes_count
        }


def main():
    parser = argparse.ArgumentParser(description="Migrate LangGraph checkpoints from SQLite to Cosmos DB")
    parser.add_argument("--sqlite-path", required=True, help="Path to SQLite database")
    parser.add_argument("--cosmos-endpoint", required=True, help="Cosmos DB endpoint URL")
    parser.add_argument("--cosmos-key", help="Cosmos DB account key (or use Azure identity)")
    parser.add_argument("--database", default="langgraph_db", help="Cosmos DB database name")
    parser.add_argument("--container", default="checkpoints", help="Cosmos DB container name")
    parser.add_argument("--user-id", default="", help="User ID to assign to migrated checkpoints")
    parser.add_argument("--dry-run", action="store_true", help="Simulate migration without writing")
    parser.add_argument("--verify", action="store_true", help="Verify migration after completion")
    parser.add_argument("--hierarchical-partition", action="store_true", default=True, 
                       help="Use hierarchical partition keys")
    
    args = parser.parse_args()
    
    # Create migrator
    migrator = SQLiteToCosmosDBMigrator(
        sqlite_path=args.sqlite_path,
        cosmos_endpoint=args.cosmos_endpoint,
        cosmos_key=args.cosmos_key,
        database_name=args.database,
        container_name=args.container,
        use_hierarchical_partition=args.hierarchical_partition
    )
    
    # Run migration
    logger.info("Starting migration...")
    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be written")
    
    stats = migrator.migrate(user_id=args.user_id, dry_run=args.dry_run)
    
    # Print results
    logger.info("\n" + "="*50)
    logger.info("Migration Complete!")
    logger.info("="*50)
    logger.info(f"Checkpoints migrated: {stats['checkpoints_migrated']}")
    logger.info(f"Writes migrated: {stats['writes_migrated']}")
    logger.info(f"Errors: {stats['errors']}")
    
    # Verify if requested
    if args.verify and not args.dry_run:
        logger.info("\nVerifying migration...")
        verification = migrator.verify_migration(user_id=args.user_id)
        logger.info("\n" + "="*50)
        logger.info("Verification Results")
        logger.info("="*50)
        logger.info(f"SQLite checkpoints: {verification['sqlite_checkpoints']}")
        logger.info(f"Cosmos checkpoints: {verification['cosmos_checkpoints']}")
        logger.info(f"Match: {'✓' if verification['checkpoints_match'] else '✗'}")
        logger.info(f"\nSQLite writes: {verification['sqlite_writes']}")
        logger.info(f"Cosmos writes: {verification['cosmos_writes']}")
        logger.info(f"Match: {'✓' if verification['writes_match'] else '✗'}")


if __name__ == "__main__":
    main()
