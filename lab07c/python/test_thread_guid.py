#!/usr/bin/env python3
"""
Test GUID-based thread creation and automatic thread naming
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

from cosmos_postgres_checkpointer import CosmosDBPostgresStyleCheckpointer

def test_guid_thread_creation():
    """Test creating threads with GUID IDs"""
    print("\n" + "="*80)
    print("TEST: GUID-based Thread Creation")
    print("="*80)
    
    connection_string = os.getenv('DATABASE_CONNECTION_STRING')
    if not connection_string:
        print("❌ DATABASE_CONNECTION_STRING not found in .env")
        return False
    
    try:
        # Initialize checkpointer
        checkpointer = CosmosDBPostgresStyleCheckpointer(
            connection_string=connection_string,
            database_name="langgraph_db",
            user_id="test_user_guid@example.com"
        )
        
        # Test 1: Create new thread with GUID
        print("\n📝 Test 1: Creating new thread with GUID...")
        thread_id_1 = checkpointer.create_new_thread()
        print(f"✅ Created thread: {thread_id_1}")
        
        # Verify it's a valid UUID format (36 chars with dashes)
        if len(thread_id_1) == 36 and thread_id_1.count('-') == 4:
            print("✅ Thread ID is valid GUID format")
        else:
            print(f"❌ Thread ID format invalid: {thread_id_1}")
            return False
        
        # Test 2: Create another thread
        print("\n📝 Test 2: Creating second thread...")
        thread_id_2 = checkpointer.create_new_thread()
        print(f"✅ Created thread: {thread_id_2}")
        
        # Verify they're different
        if thread_id_1 != thread_id_2:
            print("✅ Thread IDs are unique")
        else:
            print("❌ Thread IDs are not unique!")
            return False
        
        # Test 3: Verify threads in database
        print("\n📝 Test 3: Verifying threads in database...")
        threads = checkpointer.list_user_threads("test_user_guid@example.com", limit=10)
        print(f"✅ Found {len(threads)} threads for user")
        
        thread_ids = [t['thread_id'] for t in threads]
        if thread_id_1 in thread_ids and thread_id_2 in thread_ids:
            print("✅ Both threads found in database")
        else:
            print("❌ Threads not found in database")
            return False
        
        # Test 4: Update thread name
        print("\n📝 Test 4: Updating thread name...")
        test_name = "Test Conversation About Python"
        success = checkpointer.update_thread_name(thread_id_1, test_name)
        if success:
            print(f"✅ Updated thread name to: {test_name}")
        else:
            print("❌ Failed to update thread name")
            return False
        
        # Verify the name was saved
        thread = checkpointer.threads.find_one({"thread_id": thread_id_1})
        if thread and thread.get('thread_name') == test_name:
            print("✅ Thread name verified in database")
        else:
            print(f"❌ Thread name not saved correctly. Got: {thread.get('thread_name') if thread else 'None'}")
            return False
        
        # Test 5: Test thread name generation
        print("\n📝 Test 5: Testing thread name generation...")
        question = "What is the capital of France and what's its population?"
        response = "Paris is the capital of France. It has a population of approximately 2.2 million people in the city proper."
        
        generated_name = checkpointer.generate_thread_name(question, response, llm_client=None)
        print(f"✅ Generated name: {generated_name}")
        
        if len(generated_name) <= 255:
            print("✅ Generated name is within 255 character limit")
        else:
            print(f"❌ Generated name too long: {len(generated_name)} chars")
            return False
        
        # Test 6: Test long name truncation
        print("\n📝 Test 6: Testing long name truncation...")
        long_name = "A" * 300  # 300 characters
        success = checkpointer.update_thread_name(thread_id_2, long_name)
        
        thread = checkpointer.threads.find_one({"thread_id": thread_id_2})
        saved_name = thread.get('thread_name') if thread else None
        
        if saved_name and len(saved_name) == 255:
            print(f"✅ Long name truncated to 255 chars")
        else:
            print(f"❌ Truncation failed. Length: {len(saved_name) if saved_name else 0}")
            return False
        
        # Test 7: List threads with names
        print("\n📝 Test 7: Listing threads with names...")
        threads = checkpointer.list_user_threads("test_user_guid@example.com", limit=10)
        
        print("\n📋 User's Threads:")
        print("-" * 80)
        for thread in threads:
            thread_name = thread.get('thread_name') or '(Unnamed)'
            thread_id = thread.get('thread_id', '')
            created = thread.get('created_at')
            print(f"  - {thread_name}")
            print(f"    ID: {thread_id}")
            print(f"    Created: {created}")
            print()
        
        # Cleanup
        print("\n🧹 Cleaning up test data...")
        checkpointer.delete_thread(thread_id_1, "test_user_guid@example.com")
        checkpointer.delete_thread(thread_id_2, "test_user_guid@example.com")
        print("✅ Cleanup complete")
        
        print("\n" + "="*80)
        print("✅ ALL TESTS PASSED")
        print("="*80)
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_guid_thread_creation()
    sys.exit(0 if success else 1)
