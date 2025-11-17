"""
Simple chat interface with Cosmos DB persistence
Works with or without Snowflake connection
Automatically uses Azure CLI user if logged in
"""
import sys
import warnings
import os
from pathlib import Path
import subprocess
import json
import atexit

# Suppress LangSmith UUID warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')
warnings.filterwarnings('ignore', message='.*LangSmith now uses UUID v7.*')

from snowflake_ai_assistant import SnowflakeAIAssistant
from dotenv import load_dotenv

# Load environment from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def get_azure_user():
    """Get current Azure CLI user if logged in"""
    try:
        result = subprocess.run(
            ['az', 'ad', 'signed-in-user', 'show'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            user_info = json.loads(result.stdout)
            return {
                'user_id': user_info.get('id'),
                'email': user_info.get('userPrincipalName') or user_info.get('mail'),
                'name': user_info.get('displayName')
            }
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        pass
    return None

# Check configuration
enable_snowflake = os.getenv('ENABLE_SNOWFLAKE', 'false').lower() == 'true'
db_connection = os.getenv('DATABASE_CONNECTION_STRING', '')

# Try to get Azure user
azure_user = get_azure_user()

print("🤖 AI Assistant Chat with Cosmos DB History")
print("=" * 60)
print(f"Snowflake: {'✅ Enabled' if enable_snowflake else '❌ Disabled'}")
print(f"History: {'✅ Cosmos DB' if 'cosmos' in db_connection.lower() else '📝 In-Memory'}")
if azure_user:
    print(f"User: ✅ {azure_user['name']} ({azure_user['email']})")
    print(f"User ID: {azure_user['user_id']}")
else:
    print("User: 📝 Default user (not logged in with 'az login')")
print("=" * 60)
print("Type 'quit' to exit, 'clear' to clear history\n")

# Initialize assistant with user context
try:
    assistant = SnowflakeAIAssistant(use_azure=True)
    
    # Register cleanup function to close MongoDB connection on exit
    def cleanup():
        try:
            if hasattr(assistant, 'memory') and hasattr(assistant.memory, 'client'):
                assistant.memory.client.close()
        except:
            pass
    
    atexit.register(cleanup)
    
    # Store user context that will be injected into messages
    user_context_message = None
    
    # If we have an Azure user, set user context
    if azure_user:
        # Use user_id as prefix for thread isolation
        assistant.user_id = azure_user['user_id']
        assistant.user_name = azure_user['name']
        assistant.user_email = azure_user['email']
        
        # Create a user context message that will be prepended to queries
        user_context_message = f"[User Context: You are talking to {azure_user['name']} ({azure_user['email']}). When asked 'who am I', 'what's my name', or similar questions, refer to this information.]"
    
    print("✅ Assistant ready!\n")
except Exception as e:
    print(f"❌ Error initializing assistant: {e}\n")
    exit(1)

def search_chat_history(limit=20):
    """Search and display recent chat history"""
    client = None
    try:
        db_connection = os.getenv('DATABASE_CONNECTION_STRING')
        if not db_connection:
            print("❌ No database connection configured\n")
            return
        
        from pymongo import MongoClient
        import base64
        import pickle
        from datetime import datetime, timedelta
        client = MongoClient(db_connection)
        db = client["langgraph_db"]
        collection = db["checkpoints"]
        
        # Build query - search all recent checkpoints
        # Don't filter by user_id yet since checkpoints may have been saved before user context was added
        query = {}
        
        # Get recent checkpoints (more than we need since we'll extract messages)
        # We'll stop processing once we have enough unique messages
        all_checkpoints = list(collection.find(query).sort("_id", -1).limit(100))
        
        print(f"🔍 Found {len(all_checkpoints)} checkpoints in database")
        
        messages = []
        seen_content = set()  # Track unique messages
        decode_errors = 0
        no_channel_values = 0
        no_messages = 0
        
        for idx, checkpoint in enumerate(all_checkpoints):
            # Stop if we have enough unique messages
            if len(messages) >= limit:
                break
                
            try:
                if 'checkpoint_data' not in checkpoint:
                    print(f"⚠️  Checkpoint {idx}: No 'checkpoint_data' field")
                    continue
                
                # Base64 decode then unpickle
                decoded = base64.b64decode(checkpoint['checkpoint_data'])
                data = pickle.loads(decoded)
                
                if not isinstance(data, dict):
                    print(f"⚠️  Checkpoint {idx}: Data is not a dict, type={type(data)}")
                    continue
                
                if 'channel_values' not in data:
                    no_channel_values += 1
                    print(f"⚠️  Checkpoint {idx}: No 'channel_values', keys={list(data.keys())}")
                    continue
                
                channel_values = data['channel_values']
                if 'messages' not in channel_values:
                    no_messages += 1
                    print(f"⚠️  Checkpoint {idx}: No 'messages' in channel_values, keys={list(channel_values.keys())}")
                    continue
                
                checkpoint_time = checkpoint.get('created_at', datetime.now())
                
                # Messages are Pydantic objects, not dicts
                for msg in channel_values['messages']:
                    # Skip if we have enough messages
                    if len(messages) >= limit:
                        break
                        
                    msg_type = msg.type if hasattr(msg, 'type') else 'unknown'
                    content = msg.content if hasattr(msg, 'content') else ''
                    timestamp = checkpoint.get('created_at', datetime.now())
                    
                    # Only add if not a duplicate (based on content)
                    content_str = str(content)
                    if content_str not in seen_content:
                        seen_content.add(content_str)
                        messages.append((timestamp, msg_type, content))
                    
            except Exception as e:
                decode_errors += 1
                print(f"⚠️  Checkpoint {idx}: Error - {e}")
        
        # Print summary
        print(f"\n📊 Summary:")
        print(f"   - Decode errors: {decode_errors}")
        print(f"   - Missing channel_values: {no_channel_values}")
        print(f"   - Missing messages: {no_messages}")
        print(f"   - Total messages found: {len(messages)}\n")
        
        if not messages:
            print("📭 No messages found\n")
            print("💡 Tip: Make sure you've had a conversation in this session\n")
            return
        
        print(f"\n📜 Chat History ({len(messages)} recent messages):")
        print("=" * 60)
        
        # Sort by timestamp (oldest first)
        messages.sort(key=lambda x: x[0] if isinstance(x[0], datetime) else datetime.now())
        
        for timestamp, msg_type, content in messages:
            time_str = timestamp.strftime("%H:%M:%S") if isinstance(timestamp, datetime) else str(timestamp)
            if msg_type == 'human':
                print(f"[{time_str}] 👤 You: {content}")
            elif msg_type == 'ai':
                print(f"[{time_str}] 🤖 Assistant: {content}")
        
        print("=" * 60 + "\n")
        
    except Exception as e:
        print(f"❌ Error searching history: {e}\n")
    finally:
        if client:
            client.close()

# Chat loop
print("💡 Commands: 'quit' to exit, 'clear' to clear history, 'history' to show today's messages\n")

while True:
    try:
        user_input = input("You: ").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("\n👋 Goodbye!")
            break
        
        if user_input.lower() == 'clear':
            assistant.clear_memory()
            print("🗑️  Conversation history cleared!\n")
            continue
        
        if user_input.lower() in ['history', 'show history', 'show me history', 'show me today history']:
            search_chat_history(limit=20)
            continue
            
        if not user_input:
            continue
        
        # Prepend user context to the message if available
        if user_context_message:
            full_message = f"{user_context_message}\n\nUser Question: {user_input}"
        else:
            full_message = user_input
            
        # Get response
        response = assistant.chat(full_message)
        print(f"\nAssistant: {response}\n")
        
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        break
    except Exception as e:
        print(f"\n❌ Error: {e}\n")

# Cleanup: Close MongoDB connection if it exists
try:
    if hasattr(assistant, 'memory') and hasattr(assistant.memory, 'client'):
        assistant.memory.client.close()
except:
    pass
