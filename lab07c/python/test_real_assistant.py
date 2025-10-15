#!/usr/bin/env python3
"""
Test Real Snowflake AI Assistant
This script tests the AI assistant directly with your Snowflake data
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from snowflake_ai_assistant import SnowflakeAIAssistant

def test_real_assistant():
    """Test the AI assistant with real Snowflake data"""
    
    print("=" * 60)
    print("🧪 TESTING REAL SNOWFLAKE AI ASSISTANT")
    print("=" * 60)
    
    try:
        print("\n1. Initializing AI Assistant...")
        assistant = SnowflakeAIAssistant(use_azure=True)
        print("✅ Assistant initialized successfully!")
        
        print(f"\n2. Environment Check:")
        print(f"   - Snowflake Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
        print(f"   - Snowflake Database: {os.getenv('SNOWFLAKE_DATABASE')}")
        print(f"   - Snowflake Schema: {os.getenv('SNOWFLAKE_SCHEMA')}")
        print(f"   - Azure OpenAI Endpoint: {os.getenv('AZURE_OPENAI_ENDPOINT')}")
        
        print("\n3. 🎯 PRIMARY TEST: 'show me the employee list'")
        print("   Sending query to AI assistant...")
        
        # Test the primary use case
        response = assistant.chat("show me the employee list")
        
        print("\n📊 RESPONSE:")
        print("-" * 40)
        print(response)
        print("-" * 40)
        
        print("\n4. Testing database schema exploration...")
        schema_response = assistant.chat("what tables are available in the database?")
        
        print("\n📋 SCHEMA RESPONSE:")
        print("-" * 40)
        print(schema_response)
        print("-" * 40)
        
        print("\n✅ SUCCESS: Real Snowflake AI Assistant is working!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print(f"\nError type: {type(e).__name__}")
        
        # Provide specific troubleshooting
        error_msg = str(e).lower()
        if "snowflake" in error_msg or "connection" in error_msg:
            print("\n💡 TROUBLESHOOTING: Snowflake Connection Issue")
            print("   - Check your SNOWFLAKE_* environment variables")
            print("   - Verify your Snowflake credentials are correct")
            print("   - Test connection in Snowflake web console first")
            
        elif "openai" in error_msg or "azure" in error_msg or "api" in error_msg:
            print("\n💡 TROUBLESHOOTING: OpenAI/Azure API Issue")
            print("   - Check your AZURE_OPENAI_API_KEY")
            print("   - Verify your AZURE_OPENAI_ENDPOINT is correct")
            print("   - Test API key in Azure OpenAI Studio")
            
        else:
            print(f"\n💡 UNKNOWN ERROR: {e}")
            
        return False

def test_individual_components():
    """Test individual components separately"""
    
    print("\n" + "=" * 60)
    print("🔧 COMPONENT TESTING")
    print("=" * 60)
    
    # Test Snowflake connection
    print("\n1. Testing Snowflake Connection...")
    try:
        import snowflake.connector
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()
        
        print("✅ Snowflake Connection Successful!")
        print(f"   - User: {result[0]}")
        print(f"   - Database: {result[1]}")
        print(f"   - Schema: {result[2]}")
        
        # Test if we can see any tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if tables:
            print(f"   - Found {len(tables)} tables")
            for table in tables[:5]:  # Show first 5 tables
                print(f"     * {table[1]}")
        else:
            print("   - No tables found in current schema")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Snowflake Connection Failed: {e}")
        return False
    
    # Test Azure OpenAI
    print("\n2. Testing Azure OpenAI Connection...")
    try:
        from langchain_openai import AzureChatOpenAI
        
        llm = AzureChatOpenAI(
            azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
            api_key=os.getenv('AZURE_OPENAI_API_KEY'),
            api_version=os.getenv('AZURE_OPENAI_API_VERSION'),
            deployment_name=os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME'),
            temperature=0.1
        )
        
        # Test with a simple message
        response = llm.invoke("Hello, this is a test message.")
        
        print("✅ Azure OpenAI Connection Successful!")
        print(f"   - Response: {response.content[:100]}...")
        
    except Exception as e:
        print(f"❌ Azure OpenAI Connection Failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Testing Real Snowflake AI Assistant with your credentials...")
    
    # Test components first
    components_ok = test_individual_components()
    
    if components_ok:
        # Test full assistant
        assistant_ok = test_real_assistant()
        
        if assistant_ok:
            print("\n🎉 ALL TESTS PASSED!")
            print("Your Snowflake AI Assistant is ready for production use.")
            print("\nNext: Start the API server and test endpoints:")
            print("python python/api_server.py")
        else:
            print("\n⚠️  Assistant test failed, but components work.")
            print("Check the error messages above for troubleshooting.")
    else:
        print("\n❌ Component tests failed.")
        print("Fix the connection issues before testing the full assistant.")