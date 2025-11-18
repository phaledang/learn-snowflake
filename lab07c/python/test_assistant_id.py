"""
Test assistant_id functionality in thread management
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from cosmos_postgres_checkpointer import CosmosDBPostgresStyleCheckpointer

# Load environment
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def test_assistant_id():
    """Test creating and listing threads with assistant_id"""
    connection_string = os.getenv('DATABASE_CONNECTION_STRING')
    if not connection_string:
        print("❌ DATABASE_CONNECTION_STRING not found in .env")
        return
    
    print("\n🧪 Testing Assistant ID Functionality")
    print("=" * 70)
    
    try:
        # Initialize checkpointer
        checkpointer = CosmosDBPostgresStyleCheckpointer(
            connection_string=connection_string,
            user_id="test_user_assistant_id"
        )
        
        print("✅ Checkpointer initialized\n")
        
        # Create threads with different assistant IDs
        print("📝 Creating test threads...")
        
        thread1 = checkpointer.create_new_thread(
            user_id="test_user_assistant_id",
            assistant_id="simple_chat_001"
        )
        print(f"  ✅ Created thread for simple_chat_001: {thread1[:8]}...")
        
        thread2 = checkpointer.create_new_thread(
            user_id="test_user_assistant_id",
            assistant_id="simple_chat_001"
        )
        print(f"  ✅ Created thread for simple_chat_001: {thread2[:8]}...")
        
        thread3 = checkpointer.create_new_thread(
            user_id="test_user_assistant_id",
            assistant_id="other_assistant_002"
        )
        print(f"  ✅ Created thread for other_assistant_002: {thread3[:8]}...")
        
        # List all threads for user
        print("\n📋 Listing ALL threads for user:")
        all_threads = checkpointer.list_user_threads("test_user_assistant_id")
        print(f"  Total: {len(all_threads)} threads")
        for thread in all_threads:
            print(f"    - {thread['thread_id'][:8]}... | assistant: {thread.get('assistant_id', 'none')}")
        
        # List threads filtered by assistant_id
        print("\n📋 Listing threads for simple_chat_001 only:")
        filtered_threads = checkpointer.list_user_threads(
            "test_user_assistant_id",
            assistant_id="simple_chat_001"
        )
        print(f"  Total: {len(filtered_threads)} threads")
        for thread in filtered_threads:
            print(f"    - {thread['thread_id'][:8]}... | assistant: {thread.get('assistant_id', 'none')}")
        
        # Cleanup
        print("\n🧹 Cleaning up test threads...")
        from pymongo import MongoClient
        client = MongoClient(connection_string)
        db = client["langgraph_db"]
        result = db["threads"].delete_many({"user_id": "test_user_assistant_id"})
        print(f"  ✅ Deleted {result.deleted_count} test threads")
        client.close()
        
        print("\n" + "=" * 70)
        print("✅ All tests passed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_assistant_id()
