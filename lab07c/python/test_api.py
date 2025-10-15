#!/usr/bin/env python3
"""
API Test Script for Lab 07 - Snowflake AI Assistant
Tests the assistant with specific chat messages including "show me the employee list"
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from snowflake_ai_assistant import SnowflakeAIAssistant
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_employee_query():
    """Test the specific 'show me the employee list' query."""
    print("🧪 Testing Snowflake AI Assistant API")
    print("=" * 60)
    
    # Initialize assistant
    print("📝 Step 1: Initializing AI Assistant...")
    try:
        assistant = SnowflakeAIAssistant(use_azure=True)
        print("✅ Assistant initialized successfully!")
    except Exception as e:
        print(f"❌ Failed to initialize assistant: {e}")
        print("\n💡 Troubleshooting tips:")
        print("1. Check your .env file exists with valid credentials")
        print("2. Verify Snowflake connection parameters")
        print("3. Ensure OpenAI/Azure OpenAI API key is valid")
        return False
    
    # Test queries
    test_queries = [
        {
            "query": "show me the employee list",
            "description": "Primary test - Employee list query",
            "expected": "Should return employee data from available tables"
        },
        {
            "query": "What tables do we have in the database?",
            "description": "Schema exploration",
            "expected": "Should list all available tables"
        },
        {
            "query": "Show me the first 5 rows from any employee or customer table",
            "description": "Sample data retrieval",
            "expected": "Should return sample data if tables exist"
        }
    ]
    
    print(f"\n📋 Running {len(test_queries)} test queries...")
    print("=" * 60)
    
    results = []
    
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n🔍 Test {i}: {test_case['description']}")
        print(f"❓ Query: '{test_case['query']}'")
        print(f"📈 Expected: {test_case['expected']}")
        print("-" * 40)
        
        try:
            # Execute the query
            response = assistant.chat(test_case['query'])
            print(f"🤖 Assistant Response:\n{response}")
            results.append({
                'query': test_case['query'], 
                'status': 'success',
                'response_length': len(response)
            })
            
        except Exception as e:
            print(f"❌ Error: {e}")
            results.append({
                'query': test_case['query'], 
                'status': 'failed',
                'error': str(e)
            })
        
        print("-" * 60)
        
        # Wait for user to review
        if i < len(test_queries):
            input("⏸️  Press Enter to continue to next test...")
    
    # Summary
    print("\n📊 Test Results Summary:")
    print("=" * 40)
    success_count = sum(1 for r in results if r['status'] == 'success')
    
    for i, result in enumerate(results, 1):
        status_icon = "✅" if result['status'] == 'success' else "❌"
        print(f"{status_icon} Test {i}: {result['query'][:50]}{'...' if len(result['query']) > 50 else ''}")
    
    print(f"\n📈 Overall: {success_count}/{len(results)} tests passed")
    
    if success_count == len(results):
        print("🎉 All API tests passed! The assistant is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Check the error messages above.")
        return False

def test_conversation_flow():
    """Test a conversation flow with follow-up questions."""
    print("\n💬 Testing Conversation Flow")
    print("=" * 40)
    
    try:
        assistant = SnowflakeAIAssistant(use_azure=True)
        
        conversation = [
            "show me the employee list",
            "How many employees do we have?",
            "What departments are represented?",
            "Show me employees in the IT department"
        ]
        
        for i, query in enumerate(conversation, 1):
            print(f"\n👤 User: {query}")
            response = assistant.chat(query)
            print(f"🤖 Assistant: {response[:200]}{'...' if len(response) > 200 else ''}")
            
            if i < len(conversation):
                input("⏸️  Press Enter for next message...")
        
        print("\n✅ Conversation flow test completed!")
        
    except Exception as e:
        print(f"❌ Conversation test failed: {e}")

if __name__ == "__main__":
    print("🚀 Snowflake AI Assistant API Test")
    print("This script tests the assistant with the specific query: 'show me the employee list'")
    print()
    
    # Run main test
    success = test_employee_query()
    
    if success:
        # Run conversation test
        test_conversation_flow()
    
    print("\n🏁 Testing completed!")
    print("💡 To run interactively: python demo_cli.py")
    print("🌐 For web interface: streamlit run streamlit_app.py")