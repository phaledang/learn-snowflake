"""
Test conversation history persistence
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from snowflake_ai_assistant import SnowflakeAIAssistant

# Load environment
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def test_conversation_history():
    """Test that conversation history is maintained across multiple messages"""
    print("\n🧪 Testing Conversation History")
    print("=" * 70)
    
    try:
        # Initialize assistant
        assistant = SnowflakeAIAssistant(use_azure=True)
        
        # Create a unique thread for this test
        test_thread_id = assistant.memory.create_new_thread(
            user_id="test_history_user",
            assistant_id="test_assistant"
        )
        print(f"✅ Created test thread: {test_thread_id[:8]}...\n")
        
        # First message - tell a story
        print("📤 Message 1: Asking for a story about a robot...")
        response1 = assistant.chat(
            "Tell me a very short story about a robot named Bob in 2 sentences.",
            thread_id=test_thread_id
        )
        print(f"🤖 Response 1: {response1}\n")
        
        # Second message - ask to summarize
        print("📤 Message 2: Asking to summarize the story...")
        response2 = assistant.chat(
            "Can you summarize the story you just told me?",
            thread_id=test_thread_id
        )
        print(f"🤖 Response 2: {response2}\n")
        
        # Check if the assistant remembers
        if "bob" in response2.lower() or "robot" in response2.lower():
            print("✅ SUCCESS: Assistant remembered the previous story!")
        else:
            print("❌ FAILED: Assistant did not remember the previous story")
            print(f"   Expected mention of 'Bob' or 'robot' in summary")
        
        # Third message - ask about a specific detail
        print("\n📤 Message 3: Asking about the robot's name...")
        response3 = assistant.chat(
            "What was the robot's name in the story?",
            thread_id=test_thread_id
        )
        print(f"🤖 Response 3: {response3}\n")
        
        if "bob" in response3.lower():
            print("✅ SUCCESS: Assistant remembered the robot's name!")
        else:
            print("❌ FAILED: Assistant did not remember the robot's name")
        
        # Cleanup
        print("\n🧹 Cleaning up...")
        from pymongo import MongoClient
        connection_string = os.getenv('DATABASE_CONNECTION_STRING')
        client = MongoClient(connection_string)
        db = client["langgraph_db"]
        
        # Delete test thread
        db["threads"].delete_one({"thread_id": test_thread_id})
        db["checkpoints"].delete_many({"thread_id": test_thread_id})
        db["checkpoint_blobs"].delete_many({"thread_id": test_thread_id})
        db["checkpoint_writes"].delete_many({"thread_id": test_thread_id})
        
        print(f"✅ Cleaned up test thread {test_thread_id[:8]}...")
        client.close()
        
        print("\n" + "=" * 70)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_conversation_history()
