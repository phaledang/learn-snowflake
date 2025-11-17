"""
View Chat History from Cosmos DB
Displays conversation history stored in Cosmos DB MongoDB API
"""
import os
import sys
import base64
import pickle
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

# Load environment from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def decode_checkpoint(data_str):
    """Decode base64-encoded checkpoint data"""
    try:
        # Decode from base64
        decoded = base64.b64decode(data_str.encode('utf-8'))
        
        # Try JsonPlusSerializer first
        try:
            serde = JsonPlusSerializer()
            checkpoint = serde.loads_typed((decoded, "json"))
            return checkpoint
        except:
            # Fallback to pickle
            return pickle.loads(decoded)
    except Exception as e:
        return f"Error decoding: {e}"

def view_history(thread_id="snowflake-assistant-session", user_id="default_user"):
    """View chat history for a specific thread"""
    
    connection_string = os.getenv('DATABASE_CONNECTION_STRING')
    if not connection_string:
        print("❌ DATABASE_CONNECTION_STRING not found in .env")
        return
    
    print(f"\n🔍 Viewing Chat History")
    print("=" * 70)
    print(f"Thread ID: {thread_id}")
    print(f"User ID: {user_id}")
    print("=" * 70)
    
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client["langgraph_db"]
        collection = db["checkpoints"]
        
        # Query for checkpoints
        query = {
            "user_id": user_id,
            "thread_id": thread_id
        }
        
        documents = list(collection.find(query).sort("created_at", 1))
        
        if not documents:
            print("\n📭 No chat history found for this thread.")
            return
        
        print(f"\n📚 Found {len(documents)} checkpoint(s)\n")
        
        for idx, doc in enumerate(documents, 1):
            print(f"\n{'='*70}")
            print(f"Checkpoint #{idx}")
            print(f"{'='*70}")
            print(f"Checkpoint ID: {doc.get('checkpoint_id')}")
            print(f"Created At: {doc.get('created_at')}")
            print(f"Namespace: {doc.get('checkpoint_ns', 'default')}")
            
            # Decode checkpoint data
            checkpoint_data = decode_checkpoint(doc.get('checkpoint_data', ''))
            
            if isinstance(checkpoint_data, dict):
                # Extract messages
                messages = checkpoint_data.get('channel_values', {}).get('messages', [])
                
                if messages:
                    print(f"\n💬 Messages ({len(messages)}):")
                    print("-" * 70)
                    
                    for msg_idx, msg in enumerate(messages, 1):
                        msg_type = type(msg).__name__
                        content = getattr(msg, 'content', str(msg))
                        
                        # Format based on message type
                        if 'Human' in msg_type:
                            print(f"\n👤 User (Message {msg_idx}):")
                            print(f"   {content}")
                        elif 'AI' in msg_type:
                            print(f"\n🤖 Assistant (Message {msg_idx}):")
                            print(f"   {content}")
                        elif 'System' in msg_type:
                            print(f"\n⚙️  System (Message {msg_idx}):")
                            print(f"   {content[:200]}..." if len(content) > 200 else f"   {content}")
                        else:
                            print(f"\n📝 {msg_type} (Message {msg_idx}):")
                            print(f"   {content}")
                else:
                    print("\n📭 No messages in this checkpoint")
            else:
                print(f"\n⚠️  Checkpoint data: {checkpoint_data}")
            
            # Show metadata if available
            metadata = doc.get('metadata', {})
            if metadata:
                print(f"\n📊 Metadata:")
                for key, value in metadata.items():
                    print(f"   {key}: {value}")
        
        print(f"\n{'='*70}\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'client' in locals():
            client.close()

def list_threads():
    """List all available threads"""
    connection_string = os.getenv('DATABASE_CONNECTION_STRING')
    if not connection_string:
        print("❌ DATABASE_CONNECTION_STRING not found in .env")
        return
    
    try:
        client = MongoClient(connection_string)
        db = client["langgraph_db"]
        collection = db["checkpoints"]
        
        # Get distinct thread_ids
        threads = collection.distinct("thread_id")
        
        print(f"\n📋 Available Threads ({len(threads)}):")
        print("=" * 70)
        
        for thread_id in threads:
            # Count checkpoints for this thread
            count = collection.count_documents({"thread_id": thread_id})
            # Get latest checkpoint
            latest = collection.find_one(
                {"thread_id": thread_id},
                sort=[("created_at", -1)]
            )
            latest_date = latest.get('created_at') if latest else None
            
            print(f"\n🔖 Thread: {thread_id}")
            print(f"   Checkpoints: {count}")
            if latest_date:
                print(f"   Last Updated: {latest_date}")
        
        print(f"\n{'='*70}\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="View chat history from Cosmos DB")
    parser.add_argument('--list', action='store_true', help='List all available threads')
    parser.add_argument('--thread', type=str, default='snowflake-assistant-session', 
                       help='Thread ID to view (default: snowflake-assistant-session)')
    parser.add_argument('--user', type=str, default='default_user',
                       help='User ID (default: default_user)')
    
    args = parser.parse_args()
    
    if args.list:
        list_threads()
    else:
        view_history(thread_id=args.thread, user_id=args.user)
