#!/usr/bin/env python3
"""
Thread Management Configuration for Lab 07c
Advanced state persistence across multiple database types
"""

import os
from typing import Dict, Any, Optional, List, Union
from enum import Enum
from dataclasses import dataclass
from urllib.parse import urlparse
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('../.env')

class DatabaseType(Enum):
    """Supported database types for thread state management"""
    SQLITE = "sqlite"
    SQL_AZURE = "sql_azure"
    POSTGRESQL = "postgresql"
    COSMOS_DB = "cosmos_db"
    IN_MEMORY = "in_memory"

@dataclass
class ThreadConfig:
    """Configuration for thread management"""
    database_type: DatabaseType
    connection_string: str
    table_name: str = "langgraph_threads"
    thread_id_prefix: str = "snowflake-assistant"
    max_threads: int = 1000
    auto_cleanup_days: int = 30
    enable_encryption: bool = False
    require_authentication: bool = True
    user_isolation: bool = True

class ThreadManagerConfig:
    """Central configuration manager for thread persistence"""
    
    def __init__(self):
        self.config = self._load_config()
        
    def _load_config(self) -> ThreadConfig:
        """Load configuration from environment variables"""
        
        # Get thread management setting
        thread_manage_connection = os.getenv("THREAD_MANAGE_CONNECTION", "false").lower()
        
        if thread_manage_connection != "true":
            return ThreadConfig(
                database_type=DatabaseType.IN_MEMORY,
                connection_string="memory://",
                thread_id_prefix="temp-session"
            )
        
        # Detect database type from connection string
        connection_string = os.getenv("DATABASE_CONNECTION_STRING", "")
        database_type = self._detect_database_type(connection_string)
        
        return ThreadConfig(
            database_type=database_type,
            connection_string=connection_string,
            table_name=os.getenv("THREAD_TABLE_NAME", "langgraph_threads"),
            thread_id_prefix=os.getenv("THREAD_ID_PREFIX", "snowflake-assistant"),
            max_threads=int(os.getenv("MAX_THREADS", "1000")),
            auto_cleanup_days=int(os.getenv("AUTO_CLEANUP_DAYS", "30")),
            enable_encryption=os.getenv("ENABLE_ENCRYPTION", "false").lower() == "true",
            require_authentication=os.getenv("THREAD_REQUIRE_AUTHENTICATION", "false").lower() == "true",
            user_isolation=os.getenv("THREAD_USER_ISOLATION", "false").lower() == "true"
        )
    
    def _detect_database_type(self, connection_string: str) -> DatabaseType:
        """Detect database type from connection string"""
        
        if not connection_string:
            return DatabaseType.IN_MEMORY
            
        connection_lower = connection_string.lower()
        
        # SQLite detection
        if connection_string.endswith('.db') or connection_string.endswith('.sqlite') or 'sqlite' in connection_lower:
            return DatabaseType.SQLITE
            
        # Azure SQL detection
        if 'database.windows.net' in connection_lower or 'sql.azuresynapse.net' in connection_lower:
            return DatabaseType.SQL_AZURE
            
        # PostgreSQL detection  
        if 'postgresql://' in connection_lower or 'postgres://' in connection_lower:
            return DatabaseType.POSTGRESQL
            
        # Cosmos DB detection
        if 'documents.azure.com' in connection_lower or 'cosmos' in connection_lower:
            return DatabaseType.COSMOS_DB
            
        # Default to SQLite for file-based connections
        return DatabaseType.SQLITE
    
    def get_checkpointer_config(self) -> Dict[str, Any]:
        """Get configuration for LangGraph checkpointer"""
        
        config = {
            "database_type": self.config.database_type.value,
            "connection_string": self.config.connection_string,
            "table_name": self.config.table_name
        }
        
        if self.config.database_type == DatabaseType.IN_MEMORY:
            config["type"] = "memory"
        else:
            config["type"] = "persistent"
            config["encryption"] = self.config.enable_encryption
            
        return config
    
    def get_thread_prefix(self) -> str:
        """Get thread ID prefix"""
        return self.config.thread_id_prefix
    
    def should_persist_threads(self) -> bool:
        """Check if threads should be persisted"""
        return self.config.database_type != DatabaseType.IN_MEMORY
    
    def get_connection_info(self) -> Dict[str, str]:
        """Get connection information for debugging"""
        return {
            "database_type": self.config.database_type.value,
            "table_name": self.config.table_name,
            "thread_prefix": self.config.thread_id_prefix,
            "persistence_enabled": str(self.should_persist_threads()),
            "max_threads": str(self.config.max_threads)
        }

# Global configuration instance
thread_config = ThreadManagerConfig()

def get_thread_config() -> ThreadManagerConfig:
    """Get the global thread configuration"""
    return thread_config

# Example environment configurations for different databases

EXAMPLE_CONFIGS = {
    "sqlite": """
# SQLite Configuration (Local Development)
THREAD_MANAGE_CONNECTION=true
DATABASE_CONNECTION_STRING=./threads.db
THREAD_TABLE_NAME=langgraph_threads
THREAD_ID_PREFIX=dev-assistant
""",
    
    "sql_azure": """
# Azure SQL Configuration
THREAD_MANAGE_CONNECTION=true
DATABASE_CONNECTION_STRING=Server=myserver.database.windows.net;Database=mydb;Authentication=ActiveDirectoryDefault;
THREAD_TABLE_NAME=langgraph_threads
THREAD_ID_PREFIX=prod-assistant
ENABLE_ENCRYPTION=true
""",
    
    "postgresql": """
# PostgreSQL Configuration
THREAD_MANAGE_CONNECTION=true
DATABASE_CONNECTION_STRING=postgresql://user:password@localhost:5432/threads_db
THREAD_TABLE_NAME=langgraph_threads
THREAD_ID_PREFIX=pg-assistant
""",
    
    "cosmos_db": """
# Azure Cosmos DB Configuration
THREAD_MANAGE_CONNECTION=true
DATABASE_CONNECTION_STRING=AccountEndpoint=https://myaccount.documents.azure.com:443/;AccountKey=mykey;
THREAD_TABLE_NAME=threads
THREAD_ID_PREFIX=cosmos-assistant
ENABLE_ENCRYPTION=true
"""
}

def generate_example_env(db_type: str) -> str:
    """Generate example .env configuration for a database type"""
    return EXAMPLE_CONFIGS.get(db_type, EXAMPLE_CONFIGS["sqlite"])

if __name__ == "__main__":
    # Test configuration loading
    config = get_thread_config()
    print("Thread Management Configuration:")
    print(json.dumps(config.get_connection_info(), indent=2))
    print(f"\nCheckpointer Config:")
    print(json.dumps(config.get_checkpointer_config(), indent=2))