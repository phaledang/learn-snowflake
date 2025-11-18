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
            timeout=5,
            shell=True  # Required on Windows to find az command
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
    
    # If we have an Azure user, set user context
    if azure_user:
        # Use user_id as prefix for thread isolation
        assistant.user_id = azure_user['user_id']
        assistant.user_name = azure_user['name']
        assistant.user_email = azure_user['email']
    
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
current_thread_id = None  # Will be set to GUID on first message or when user creates new thread
ASSISTANT_ID = "simple_chat_001"  # Assistant identifier for this chat interface

def list_user_threads():
    """List all threads for the current user"""
    try:
        if not hasattr(assistant, 'memory'):
            print("❌ No memory configured\n")
            return
        
        # Use the user_id from assistant (which defaults to 'default_user' if not logged in)
        user_id = getattr(assistant, 'user_id', 'default_user')
        threads = assistant.memory.list_user_threads(user_id, assistant_id=ASSISTANT_ID, limit=20)
        
        if not threads:
            print("� No conversation threads found\n")
            return
        
        print(f"\n📋 Your Conversation Threads ({len(threads)}):")
        print("=" * 80)
        
        for idx, thread in enumerate(threads, 1):
            thread_name = thread.get('thread_name') or '(Unnamed)'
            thread_id = thread.get('thread_id', '')
            created = thread.get('created_at')
            created_str = created.strftime("%Y-%m-%d %H:%M") if created else 'Unknown'
            
            # Mark current thread
            marker = "👉" if thread_id == current_thread_id else "  "
            
            print(f"{marker} {idx}. {thread_name}")
            print(f"     ID: {thread_id[:8]}... | Created: {created_str}")
        
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"❌ Error listing threads: {e}\n")

def create_new_thread():
    """Create a new thread with GUID"""
    global current_thread_id
    try:
        if not hasattr(assistant, 'memory'):
            print("❌ No memory configured\n")
            return None
        
        new_thread_id = assistant.memory.create_new_thread(assistant.user_id, assistant_id=ASSISTANT_ID)
        print(f"✅ Created new thread: {new_thread_id[:8]}...\n")
        return new_thread_id
        
    except Exception as e:
        print(f"❌ Error creating thread: {e}\n")
        return None

def view_thread_history(thread_id=None):
    """View message history for a specific thread or the current thread"""
    try:
        if not hasattr(assistant, 'memory'):
            print("❌ No memory configured\n")
            return
        
        # Use current thread if not specified
        target_thread_id = thread_id or current_thread_id
        
        if not target_thread_id:
            print("❌ No thread specified and no active thread. Please start a conversation or select a thread.\n")
            return
        
        # Get the database connection
        db_connection = os.getenv('DATABASE_CONNECTION_STRING')
        if not db_connection:
            print("❌ No database connection configured\n")
            return
        
        from pymongo import MongoClient
        import base64
        import pickle
        from datetime import datetime
        
        client = None
        try:
            client = MongoClient(db_connection)
            db = client["langgraph_db"]
            collection = db["checkpoints"]
            
            # Query for this specific thread
            query = {"thread_id": target_thread_id}
            checkpoints = list(collection.find(query).sort("created_at", 1))
            
            if not checkpoints:
                print(f"📭 No history found for thread {target_thread_id[:8]}...\n")
                return
            
            # Extract all messages from checkpoints
            all_messages = []
            seen_content = set()
            
            for checkpoint in checkpoints:
                try:
                    if 'checkpoint_data' not in checkpoint:
                        continue
                    
                    # Base64 decode then unpickle
                    decoded = base64.b64decode(checkpoint['checkpoint_data'])
                    data = pickle.loads(decoded)
                    
                    if not isinstance(data, dict):
                        continue
                    
                    channel_values = data.get('channel_values', {})
                    messages = channel_values.get('messages', [])
                    
                    checkpoint_time = checkpoint.get('created_at', datetime.now())
                    
                    # Extract messages (they are Pydantic objects)
                    for msg in messages:
                        msg_type = msg.type if hasattr(msg, 'type') else 'unknown'
                        content = msg.content if hasattr(msg, 'content') else ''
                        
                        # Only add unique messages
                        content_str = str(content)
                        if content_str and content_str not in seen_content:
                            seen_content.add(content_str)
                            all_messages.append((checkpoint_time, msg_type, content))
                
                except Exception as e:
                    continue
            
            if not all_messages:
                print(f"📭 No messages found in thread {target_thread_id[:8]}...\n")
                return
            
            # Sort by timestamp
            all_messages.sort(key=lambda x: x[0] if isinstance(x[0], datetime) else datetime.now())
            
            # Get thread info
            threads_collection = db["threads"]
            thread_info = threads_collection.find_one({"thread_id": target_thread_id})
            thread_name = thread_info.get('thread_name', '(Unnamed)') if thread_info else '(Unnamed)'
            thread_assistant_id = thread_info.get('assistant_id', 'unknown') if thread_info else 'unknown'
            
            print(f"\n💬 Thread History: {thread_name}")
            print(f"Thread ID: {target_thread_id[:8]}... | Assistant: {thread_assistant_id}")
            print("=" * 70)
            
            for timestamp, msg_type, content in all_messages:
                time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(timestamp, datetime) else str(timestamp)
                
                if msg_type == 'human':
                    print(f"\n[{time_str}] 👤 You:")
                    print(f"  {content}")
                elif msg_type == 'ai':
                    print(f"\n[{time_str}] 🤖 Assistant:")
                    print(f"  {content}")
            
            print("\n" + "=" * 70 + "\n")
            
        finally:
            if client:
                client.close()
        
    except Exception as e:
        print(f"❌ Error viewing thread history: {e}\n")

def select_thread():
    """Allow user to select a thread"""
    global current_thread_id
    try:
        if not hasattr(assistant, 'memory'):
            print("❌ No memory configured\n")
            return
        
        # Use the user_id from assistant (which defaults to 'default_user' if not logged in)
        user_id = getattr(assistant, 'user_id', 'default_user')
        threads = assistant.memory.list_user_threads(user_id, assistant_id=ASSISTANT_ID, limit=20)
        
        if not threads:
            print("📭 No threads found. A new thread will be created automatically.\n")
            return
        
        user_label = "Your" if azure_user else "Default User's"
        print(f"\n📋 Select a Thread ({user_label}):")
        print("=" * 80)
        
        for idx, thread in enumerate(threads, 1):
            thread_name = thread.get('thread_name') or '(Unnamed)'
            thread_id = thread.get('thread_id', '')
            created = thread.get('created_at')
            created_str = created.strftime("%Y-%m-%d %H:%M") if created else 'Unknown'
            
            print(f"  {idx}. {thread_name}")
            print(f"     ID: {thread_id[:8]}... | Created: {created_str}")
        
        print("=" * 80)
        
        choice = input(f"\nEnter thread number (1-{len(threads)}) or press Enter to create new: ").strip()
        
        if not choice:
            new_thread_id = create_new_thread()
            if new_thread_id:
                current_thread_id = new_thread_id
            return
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(threads):
                current_thread_id = threads[idx]['thread_id']
                thread_name = threads[idx].get('thread_name') or '(Unnamed)'
                print(f"✅ Switched to thread: {thread_name}\n")
            else:
                print(f"❌ Invalid selection. Please enter 1-{len(threads)}\n")
        except ValueError:
            print("❌ Invalid input. Please enter a number.\n")
        
    except Exception as e:
        print(f"❌ Error selecting thread: {e}\n")

print("💡 Commands:")
print("  - 'quit' to exit")
print("  - 'new' to start a new conversation thread")
print("  - 'threads' to list all your threads")
print("  - 'switch' to switch to a different thread")
print("  - 'view' to view history of current thread")
print("  - 'clear' to clear current thread history")
print("  - 'history' to show recent messages\n")

while True:
    try:
        user_input = input("You: ").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("\n👋 Goodbye!")
            break
        
        if user_input.lower() in ['new', 'new thread', 'new conversation']:
            new_thread_id = create_new_thread()
            if new_thread_id:
                current_thread_id = new_thread_id
            continue
        
        if user_input.lower() in ['threads', 'list threads', 'show threads']:
            list_user_threads()
            continue
        
        if user_input.lower() in ['switch', 'switch thread', 'select']:
            select_thread()
            continue
        
        if user_input.lower() in ['view', 'view thread', 'view history']:
            view_thread_history()
            continue
        
        if user_input.lower() == 'clear':
            if current_thread_id:
                # Clear just sets a new thread - the old one is preserved
                print("� Creating new thread (old thread preserved)...\n")
                new_thread_id = create_new_thread()
                if new_thread_id:
                    current_thread_id = new_thread_id
            else:
                print("🗑️  No active thread to clear!\n")
            continue
        
        if user_input.lower() in ['history', 'show history', 'show me history', 'show me today history']:
            search_chat_history(limit=20)
            continue
            
        if not user_input:
            continue
        
        # Auto-create thread on first message if not set
        if not current_thread_id:
            current_thread_id = create_new_thread()
            if not current_thread_id:
                print("❌ Failed to create thread. Please try again.\n")
                continue
        
        # Get response with specific thread_id
        response = assistant.chat(user_input, thread_id=current_thread_id)
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
