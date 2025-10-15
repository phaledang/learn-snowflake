#!/usr/bin/env python3
"""
Diagnostic script to test .env loading in VS Code debug environment
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def main():
    print("🔍 ENVIRONMENT DIAGNOSTIC REPORT")
    print("=" * 50)
    
    # Check current working directory
    current_dir = os.getcwd()
    print(f"📂 Current Working Directory: {current_dir}")
    
    # Check Python path
    print(f"🐍 Python Path: {sys.path}")
    
    # Check for .env file
    env_file = Path(".env")
    env_exists = env_file.exists()
    print(f"📄 .env file exists: {env_exists}")
    
    if env_exists:
        print(f"📍 .env file location: {env_file.absolute()}")
        print(f"📏 .env file size: {env_file.stat().st_size} bytes")
    
    # Try to load .env
    print("\n🔄 Attempting to load .env...")
    try:
        dotenv_loaded = load_dotenv()
        print(f"✅ load_dotenv() result: {dotenv_loaded}")
    except Exception as e:
        print(f"❌ Error loading .env: {e}")
    
    # Check specific environment variables
    print("\n🌍 ENVIRONMENT VARIABLES CHECK:")
    
    # Snowflake connection variables
    snowflake_vars = [
        'SNOWFLAKE_CONNECTION_STRING',
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA',
        'SNOWFLAKE_ROLE'
    ]
    
    for var in snowflake_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive data
            if 'PASSWORD' in var or 'CONNECTION_STRING' in var:
                display_value = f"{value[:10]}...{value[-5:]}" if len(value) > 15 else "***"
            else:
                display_value = value
            print(f"  ✅ {var}: {display_value}")
        else:
            print(f"  ❌ {var}: Not set")
    
    # Azure OpenAI variables
    print("\n🤖 AI CONFIGURATION:")
    ai_vars = [
        'AZURE_OPENAI_API_KEY',
        'AZURE_OPENAI_ENDPOINT',
        'AZURE_OPENAI_API_VERSION',
        'AZURE_OPENAI_DEPLOYMENT_NAME'
    ]
    
    for var in ai_vars:
        value = os.getenv(var)
        if value:
            if 'KEY' in var:
                display_value = f"{value[:8]}...{value[-4:]}" if len(value) > 12 else "***"
            else:
                display_value = value
            print(f"  ✅ {var}: {display_value}")
        else:
            print(f"  ❌ {var}: Not set")
    
    # Test connection setup
    print("\n🔗 CONNECTION TEST:")
    try:
        # Add current directory to path for imports
        sys.path.append(os.path.dirname(__file__))
        from snowflake_connection import get_connection_info
        
        config = get_connection_info()
        print("✅ Connection configuration loaded successfully:")
        for key, value in config.items():
            print(f"  {key}: {value}")
            
    except ImportError as e:
        print(f"❌ Could not import snowflake_connection: {e}")
    except Exception as e:
        print(f"❌ Error getting connection info: {e}")
    
    print("\n📋 RECOMMENDATIONS:")
    
    if not env_exists:
        print("  ❗ Create .env file in lab07 directory")
        print("  📄 Copy from .env.example and fill in your values")
    
    connection_string = os.getenv('SNOWFLAKE_CONNECTION_STRING')
    if not connection_string:
        print("  ❗ Set SNOWFLAKE_CONNECTION_STRING in .env")
    
    azure_key = os.getenv('AZURE_OPENAI_API_KEY')
    if not azure_key:
        print("  ❗ Set AZURE_OPENAI_API_KEY in .env")
    
    if current_dir.endswith('learn-snowflake'):
        print("  ⚠️  Current directory should be 'lab07' for proper .env loading")
        print("  💡 Use 'cwd': '${workspaceFolder}/lab07' in launch.json")
    
    print("\n🎯 Next Steps:")
    print("  1. Ensure .env file exists in lab07 directory")
    print("  2. Check VS Code debug configuration has correct 'cwd'")
    print("  3. Run this diagnostic again to verify fixes")
    print("  4. Try running the API server")

if __name__ == "__main__":
    main()