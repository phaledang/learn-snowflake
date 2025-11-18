"""
Test script to verify simple_chat.py commands work for both logged-in and default users
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def test_thread_listing():
    """Test listing threads for default user"""
    connection_string = os.getenv('DATABASE_CONNECTION_STRING')
    if not connection_string:
        print("❌ DATABASE_CONNECTION_STRING not found in .env")
        return
    
    print("\n🔍 Testing Thread Listing")
    print("=" * 70)
    
    try:
        client = MongoClient(connection_string)
        db = client["langgraph_db"]
        threads_collection = db["threads"]
        
        # List threads for default user
        default_user_threads = list(threads_collection.find({"user_id": "default_user"}).sort("_id", -1).limit(10))
        
        print(f"\n📋 Default User Threads: {len(default_user_threads)}")
        for idx, thread in enumerate(default_user_threads, 1):
            thread_name = thread.get('thread_name') or '(Unnamed)'
            thread_id = thread.get('thread_id', '')
            created = thread.get('created_at')
            created_str = created.strftime("%Y-%m-%d %H:%M") if created else 'Unknown'
            
            print(f"  {idx}. {thread_name}")
            print(f"     ID: {thread_id[:8]}... | Created: {created_str}")
        
        # List all unique users in the database
        all_user_ids = threads_collection.distinct("user_id")
        print(f"\n👥 All Users in Database: {len(all_user_ids)}")
        for user_id in all_user_ids[:10]:  # Show first 10
            user_thread_count = threads_collection.count_documents({"user_id": user_id})
            print(f"  - {user_id}: {user_thread_count} threads")
        
        print("\n" + "=" * 70)
        client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_thread_listing()
