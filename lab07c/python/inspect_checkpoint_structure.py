"""
Inspect the actual structure of checkpoints in MongoDB
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
import pickle
import base64
import json

# Load environment
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Connect to MongoDB
connection_string = os.getenv('DATABASE_CONNECTION_STRING')
client = MongoClient(connection_string)
db = client["langgraph_db"]
collection = db["checkpoints"]

print("=" * 80)
print("CHECKPOINT STRUCTURE INSPECTION")
print("=" * 80)

# Get one checkpoint
checkpoint = collection.find_one()

if checkpoint:
    print("\n1. DOCUMENT STRUCTURE (Metadata only):")
    print("-" * 80)
    # Show structure without the large checkpoint_data
    for key in checkpoint.keys():
        value = checkpoint[key]
        if key == 'checkpoint_data':
            print(f"  {key}: <base64-encoded-pickle> (length: {len(value)} chars)")
        elif key == 'metadata':
            print(f"  {key}: {json.dumps(value, indent=4)}")
        elif key == 'writes':
            print(f"  {key}: {value} (length: {len(value)})")
        else:
            print(f"  {key}: {value}")
    
    print("\n2. DECODED CHECKPOINT DATA:")
    print("-" * 80)
    try:
        # Decode the checkpoint_data
        decoded = base64.b64decode(checkpoint['checkpoint_data'])
        data = pickle.loads(decoded)
        
        print(f"  Type: {type(data)}")
        if isinstance(data, dict):
            print(f"  Keys: {list(data.keys())}")
            
            # Show channel_values if present
            if 'channel_values' in data:
                print(f"\n  channel_values keys: {list(data['channel_values'].keys())}")
                
                # Show messages
                if 'messages' in data['channel_values']:
                    messages = data['channel_values']['messages']
                    print(f"\n  Messages (count: {len(messages)}):")
                    for i, msg in enumerate(messages[:3]):  # Show first 3
                        msg_type = msg.type if hasattr(msg, 'type') else 'unknown'
                        content = (msg.content[:50] + '...') if hasattr(msg, 'content') and len(str(msg.content)) > 50 else (msg.content if hasattr(msg, 'content') else '')
                        print(f"    [{i}] {msg_type}: {content}")
                    if len(messages) > 3:
                        print(f"    ... and {len(messages) - 3} more messages")
    except Exception as e:
        print(f"  Error decoding: {e}")
    
    print("\n3. WRITES STRUCTURE:")
    print("-" * 80)
    if checkpoint.get('writes'):
        print(f"  Number of writes: {len(checkpoint['writes'])}")
        for i, write in enumerate(checkpoint['writes'][:3]):
            print(f"  Write {i}: {write}")
    else:
        print("  No writes in this checkpoint")
    
    print("\n4. COMPARISON WITH POSTGRESQL SCHEMA:")
    print("-" * 80)
    print("  PostgreSQL uses 3 tables:")
    print("    - checkpoints: thread_id, checkpoint_id, checkpoint (JSONB)")
    print("    - checkpoint_blobs: Large binary data separate from main checkpoint")
    print("    - checkpoint_writes: Pending writes for the checkpoint")
    print()
    print("  MongoDB uses 1 collection:")
    print("    - checkpoints: Everything embedded in one document")
    print("      * checkpoint_data: Contains all the checkpoint state (base64 pickle)")
    print("      * writes: Embedded array of writes")
    print("      * metadata: Embedded metadata object")
    print()
    print("  Advantages of MongoDB approach:")
    print("    ✅ Simpler - everything in one document")
    print("    ✅ Atomic updates - no joins needed")
    print("    ✅ Faster reads - single query gets everything")
    print()
    print("  Disadvantages:")
    print("    ⚠️  Document size limit (16MB in MongoDB, 2MB in Cosmos DB)")
    print("    ⚠️  Can't query inside pickled checkpoint_data efficiently")

else:
    print("\n❌ No checkpoints found in database")
    print("   Run simple_chat.py first to create some conversation history")

# Show total count
total = collection.count_documents({})
print(f"\n5. TOTAL CHECKPOINTS IN DATABASE: {total}")

client.close()
print("\n" + "=" * 80)
