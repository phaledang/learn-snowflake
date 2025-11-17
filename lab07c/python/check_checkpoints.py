"""Quick script to check what's in the checkpoints collection"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

# Load environment
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

db_connection = os.getenv('DATABASE_CONNECTION_STRING')
if not db_connection:
    print("❌ No DATABASE_CONNECTION_STRING configured")
    exit(1)

print("🔍 Connecting to Cosmos DB...\n")
client = MongoClient(db_connection)
db = client["langgraph_db"]
collection = db["checkpoints"]

# Count total checkpoints
total = collection.count_documents({})
print(f"📊 Total checkpoints: {total}\n")

# Get the most recent 5 checkpoints
print("📝 Recent checkpoints:")
print("=" * 80)

recent = list(collection.find().sort("_id", -1).limit(5))

for idx, doc in enumerate(recent, 1):
    print(f"\n{idx}. Checkpoint:")
    print(f"   Thread ID: {doc.get('thread_id', 'N/A')}")
    print(f"   User ID: {doc.get('user_id', 'N/A')}")
    print(f"   Checkpoint ID: {doc.get('checkpoint_id', 'N/A')}")
    print(f"   Created: {doc.get('created_at', 'N/A')}")
    print(f"   Has checkpoint data: {bool(doc.get('checkpoint_data'))}")
    
    # Try to decode and check for messages
    if 'checkpoint_data' in doc:
        try:
            import base64
            from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
            
            serializer = JsonPlusSerializer()
            decoded = base64.b64decode(doc['checkpoint_data'])
            data = serializer.loads_typed((decoded, "json"))
            
            if isinstance(data, dict):
                channel_values = data.get('channel_values', {})
                messages = channel_values.get('messages', [])
                print(f"   Messages count: {len(messages)}")
                
                # Show first 2 messages
                for msg_idx, msg in enumerate(messages[:2], 1):
                    msg_type = msg.get('type', 'unknown')
                    content = msg.get('content', '')[:50]  # First 50 chars
                    print(f"      {msg_idx}. {msg_type}: {content}...")
        except Exception as e:
            print(f"   ⚠️ Error decoding: {e}")

print("\n" + "=" * 80)
client.close()
print("\n✅ Done!")
