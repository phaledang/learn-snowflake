"""Simple test to see if we can read checkpoint messages"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
import base64
import pickle

# Load environment
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

db_connection = os.getenv('DATABASE_CONNECTION_STRING')
client = MongoClient(db_connection)
db = client["langgraph_db"]
collection = db["checkpoints"]

# Get one checkpoint
checkpoint_doc = collection.find_one({}, sort=[("_id", -1)])

if not checkpoint_doc:
    print("No checkpoints found!")
    client.close()
    exit()

print(f"Found checkpoint: {checkpoint_doc.get('checkpoint_id')}")
print(f"Has checkpoint_data: {bool(checkpoint_doc.get('checkpoint_data'))}")

if 'checkpoint_data' in checkpoint_doc:
    # Decode and unpickle
    decoded = base64.b64decode(checkpoint_doc['checkpoint_data'])
    print(f"Decoded bytes length: {len(decoded)}")
    
    # Deserialize using pickle
    data = pickle.loads(decoded)
    print(f"Data type: {type(data)}")
    print(f"Data keys: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}")
    
    if isinstance(data, dict) and 'channel_values' in data:
        channel_values = data['channel_values']
        print(f"Channel_values keys: {list(channel_values.keys())}")
        
        if 'messages' in channel_values:
            messages = channel_values['messages']
            print(f"\n✅ Found {len(messages)} messages!")
            
            # Print first 3 messages
            for i, msg in enumerate(messages[:3], 1):
                msg_type = msg.type if hasattr(msg, 'type') else 'unknown'
                content = str(msg.content if hasattr(msg, 'content') else '')[:100]  # First 100 chars
                print(f"\n{i}. Type: {msg_type}")
                print(f"   Content: {content}")
        else:
            print("❌ No 'messages' in channel_values")
    else:
        print("❌ No 'channel_values' in data")

client.close()
