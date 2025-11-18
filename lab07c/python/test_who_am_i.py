"""
Test script to verify "who am I" functionality
"""
import os
import sys
import subprocess
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

from snowflake_ai_assistant import SnowflakeAIAssistant

print("=" * 60)
print("Testing 'who am I' functionality")
print("=" * 60)

# Create assistant
assistant = SnowflakeAIAssistant(use_azure=True)
print(f"\n1. Assistant created")

# Try to get Azure user from Azure CLI (only if logged in)
print(f"\n2. Checking Azure CLI login...")
azure_user = None
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
        azure_user = {
            'user_id': user_info.get('id'),
            'email': user_info.get('userPrincipalName') or user_info.get('mail'),
            'name': user_info.get('displayName')
        }
        print(f"   ✅ Azure CLI user found: {azure_user['name']} ({azure_user['email']})")
    else:
        print(f"   ❌ Not logged in with Azure CLI")
        print(f"   Please run 'az login' first to test user context functionality")
        sys.exit(0)
except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
    print(f"   ❌ Azure CLI not available or not logged in: {e}")
    print(f"   Please run 'az login' first to test user context functionality")
    sys.exit(0)

# Set user context only if Azure user is available
if azure_user:
    assistant.user_id = azure_user['user_id']
    assistant.user_name = azure_user['name']
    assistant.user_email = azure_user['email']
    print(f"   ✅ User context set from Azure CLI login")

# Check if system prompt has user context
print(f"\n3. Checking system prompt...")
if "CURRENT USER CONTEXT" in assistant.system_prompt:
    print("   ✅ System prompt contains user context")
    # Print the user context section
    idx = assistant.system_prompt.find("CURRENT USER CONTEXT")
    print(f"\n   User context section:")
    print(f"   {assistant.system_prompt[idx:idx+200]}...")
else:
    print("   ❌ System prompt DOES NOT contain user context")
    print(f"\n   Last 200 chars of system prompt:")
    print(f"   ...{assistant.system_prompt[-200:]}")

# Test the chat
print(f"\n4. Testing chat with 'who am I' question...")
response = assistant.chat("who am I?")
print(f"\n   Response: {response}")

print(f"\n5. Testing follow-up question...")
response2 = assistant.chat("what is my email?")
print(f"\n   Response: {response2}")

print("\n" + "=" * 60)
