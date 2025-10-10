"""
Test script for Snowflake AI Assistant
Basic functionality validation without requiring full setup.
"""

import os
import sys
from unittest.mock import Mock, patch

def test_imports():
    """Test that all required imports work."""
    print("🔧 Testing imports...")
    try:
        # Test LangChain imports
        from langchain.agents import AgentExecutor
        from langchain_core.prompts import ChatPromptTemplate
        from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
        from langchain_core.tools import BaseTool
        print("✅ LangChain imports successful")
        
        # Test other core imports
        from datetime import datetime
        from typing import Any, List, Optional
        import pandas as pd
        print("✅ Core imports successful")
        
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Run: pip install -r requirements.txt")
        return False

def test_tools_structure():
    """Test tool class structures."""
    print("\n🔧 Testing tool structures...")
    
    try:
        # Mock the snowflake connection to avoid requiring actual credentials
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connect.return_value = Mock()
            
            from snowflake_ai_assistant import SnowflakeQueryTool, SchemaInspectionTool, FileProcessingTool
            
            # Test tool initialization
            query_tool = SnowflakeQueryTool()
            schema_tool = SchemaInspectionTool()
            file_tool = FileProcessingTool()
            
            print("✅ SnowflakeQueryTool initialized")
            print("✅ SchemaInspectionTool initialized")
            print("✅ FileProcessingTool initialized")
            
            # Test tool properties
            assert hasattr(query_tool, 'name')
            assert hasattr(query_tool, 'description')
            assert hasattr(query_tool, '_run')
            print("✅ Tool interfaces validated")
            
            return True
    except Exception as e:
        print(f"❌ Tool structure error: {e}")
        return False

def test_assistant_structure():
    """Test assistant class structure without full initialization."""
    print("\n🔧 Testing assistant structure...")
    
    try:
        with patch('snowflake.connector.connect') as mock_connect, \
             patch('langchain_openai.AzureChatOpenAI') as mock_llm:
            
            mock_connect.return_value = Mock()
            mock_llm.return_value = Mock()
            
            # Set minimal environment variables for testing
            os.environ.setdefault('SNOWFLAKE_ACCOUNT', 'test')
            os.environ.setdefault('SNOWFLAKE_USER', 'test')
            os.environ.setdefault('SNOWFLAKE_PASSWORD', 'test')
            os.environ.setdefault('SNOWFLAKE_WAREHOUSE', 'test')
            os.environ.setdefault('SNOWFLAKE_DATABASE', 'test')
            os.environ.setdefault('SNOWFLAKE_SCHEMA', 'test')
            os.environ.setdefault('AZURE_OPENAI_API_KEY', 'test')
            os.environ.setdefault('AZURE_OPENAI_ENDPOINT', 'test')
            os.environ.setdefault('AZURE_OPENAI_DEPLOYMENT_NAME', 'test')
            
            from snowflake_ai_assistant import SnowflakeAIAssistant
            
            # Test basic structure without full initialization
            print("✅ SnowflakeAIAssistant class imported successfully")
            
            # Test class attributes
            assert hasattr(SnowflakeAIAssistant, '__init__')
            assert hasattr(SnowflakeAIAssistant, 'chat')
            assert hasattr(SnowflakeAIAssistant, 'clear_memory')
            print("✅ Assistant interface validated")
            
            return True
    except Exception as e:
        print(f"❌ Assistant structure error: {e}")
        return False

def test_environment_template():
    """Test environment template validity."""
    print("\n🔧 Testing environment template...")
    
    try:
        env_example_path = '.env.example'
        if os.path.exists(env_example_path):
            with open(env_example_path, 'r') as f:
                content = f.read()
                
            required_vars = [
                'SNOWFLAKE_ACCOUNT',
                'SNOWFLAKE_USER',
                'AZURE_OPENAI_API_KEY',
                'OPENAI_API_KEY'
            ]
            
            for var in required_vars:
                if var in content:
                    print(f"✅ {var} found in template")
                else:
                    print(f"⚠️  {var} missing from template")
            
            print("✅ Environment template validated")
            return True
        else:
            print("❌ .env.example file not found")
            return False
    except Exception as e:
        print(f"❌ Environment template error: {e}")
        return False

def test_file_structure():
    """Test that all required files exist."""
    print("\n🔧 Testing file structure...")
    
    required_files = [
        'requirements.txt',
        '.env.example',
        'snowflake_ai_assistant.py',
        'streamlit_app.py',
        'demo_cli.py',
        'business_guidelines.md'
    ]
    
    all_files_exist = True
    for file_name in required_files:
        if os.path.exists(file_name):
            print(f"✅ {file_name} exists")
        else:
            print(f"❌ {file_name} missing")
            all_files_exist = False
    
    return all_files_exist

def main():
    """Run all tests."""
    print("🚀 Snowflake AI Assistant - Test Suite")
    print("=" * 50)
    
    tests = [
        ("File Structure", test_file_structure),
        ("Environment Template", test_environment_template),
        ("Python Imports", test_imports),
        ("Tool Structures", test_tools_structure),
        ("Assistant Structure", test_assistant_structure)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n📋 Running {test_name} Test...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:20} : {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Lab 07 is ready to use.")
        print("\n💡 Next steps:")
        print("1. Configure your .env file with actual credentials")
        print("2. Run: python demo_cli.py")
        print("3. Or run: streamlit run streamlit_app.py")
    else:
        print("⚠️  Some tests failed. Please check the issues above.")
        print("\n💡 Common fixes:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Check file structure and ensure all files are present")
        print("3. Verify Python version (3.8+ required)")

if __name__ == "__main__":
    main()