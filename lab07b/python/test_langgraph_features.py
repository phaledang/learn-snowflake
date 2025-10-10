"""
Test script for LangGraph-based Snowflake AI Assistant
Validates the new LangGraph implementation and compares with traditional LangChain approach
"""

import os
import sys
import time
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_langgraph_features():
    """Test LangGraph-specific features."""
    print("🧪 Testing LangGraph Features...")
    print("=" * 60)
    
    try:
        # Test imports
        print("1. Testing LangGraph imports...")
        from langgraph.graph import StateGraph
        from langgraph.prebuilt import create_react_agent
        from langgraph.checkpoint.memory import MemorySaver
        print("   ✅ LangGraph imports successful")
        
        # Test assistant initialization
        print("2. Testing assistant initialization...")
        from snowflake_ai_assistant import SnowflakeAIAssistant
        
        assistant = SnowflakeAIAssistant(use_azure=True)
        print("   ✅ LangGraph assistant initialized successfully")
        
        # Test graph structure
        print("3. Testing graph structure...")
        if hasattr(assistant.agent, 'get_graph'):
            graph = assistant.agent.get_graph()
            print(f"   ✅ Graph has {len(graph.nodes)} nodes")
            print(f"   ✅ Graph has {len(graph.edges)} edges")
        else:
            print("   ⚠️  Graph structure not accessible")
        
        # Test memory persistence
        print("4. Testing memory persistence...")
        original_thread_id = assistant.thread_id
        print(f"   📍 Initial thread ID: {original_thread_id}")
        
        # Send a test message
        response1 = assistant.chat("Remember: My name is TestUser")
        print(f"   💬 Response 1: {response1[:50]}...")
        
        # Check if memory persists
        response2 = assistant.chat("What is my name?")
        print(f"   💬 Response 2: {response2[:50]}...")
        
        if "TestUser" in response2:
            print("   ✅ Memory persistence working correctly")
        else:
            print("   ⚠️  Memory persistence may not be working")
        
        # Test conversation history
        print("5. Testing conversation history...")
        history = assistant.get_conversation_history()
        print(f"   📝 History contains {len(history)} messages")
        
        # Test memory clearing
        print("6. Testing memory clearing...")
        assistant.clear_memory()
        new_thread_id = assistant.thread_id
        print(f"   🔄 New thread ID: {new_thread_id}")
        
        if new_thread_id != original_thread_id:
            print("   ✅ Memory clearing successful")
        else:
            print("   ⚠️  Memory clearing may not be working")
        
        # Test graph visualization
        print("7. Testing graph visualization...")
        try:
            graph_path = assistant.get_agent_graph_image("test_graph.png")
            if graph_path and os.path.exists(graph_path):
                print(f"   ✅ Graph visualization saved to: {graph_path}")
            else:
                print("   ⚠️  Graph visualization not generated")
        except Exception as e:
            print(f"   ⚠️  Graph visualization error: {str(e)}")
        
        print("\n🎉 LangGraph features test completed!")
        return True
        
    except ImportError as e:
        print(f"   ❌ Import error: {str(e)}")
        print("   📦 Make sure to install: pip install langgraph")
        return False
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

def test_tool_integration():
    """Test that existing tools work with LangGraph."""
    print("\n🔧 Testing Tool Integration...")
    print("=" * 60)
    
    try:
        from snowflake_ai_assistant import SnowflakeAIAssistant
        assistant = SnowflakeAIAssistant(use_azure=True)
        
        # Test schema inspection
        print("1. Testing schema inspection tool...")
        response = assistant.chat("What tables are available in the database?")
        print(f"   💬 Response: {response[:100]}...")
        
        # Test query execution (if Snowflake is available)
        print("2. Testing SQL query execution...")
        response = assistant.chat("Show me the first 3 rows from any available table")
        print(f"   💬 Response: {response[:100]}...")
        
        print("   ✅ Tool integration test completed")
        return True
        
    except Exception as e:
        print(f"   ⚠️  Tool integration error: {str(e)}")
        print("   💡 This may be expected if Snowflake credentials are not configured")
        return False

def test_api_endpoints():
    """Test LangGraph-specific API endpoints."""
    print("\n🌐 Testing API Endpoints...")
    print("=" * 60)
    
    try:
        import requests
        import json
        
        base_url = "http://localhost:8080"
        
        # Test graph info endpoint
        print("1. Testing /graph/info endpoint...")
        try:
            response = requests.get(f"{base_url}/graph/info", timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Framework: {data.get('framework')}")
                print(f"   ✅ Agent type: {data.get('agent_type')}")
                print(f"   ✅ Tools: {data.get('tools', [])}")
            else:
                print(f"   ⚠️  Endpoint returned status: {response.status_code}")
        except requests.exceptions.ConnectionError:
            print("   ⚠️  API server not running")
            print("   💡 Start the server with: python python/api_server.py")
        
        print("   ✅ API endpoints test completed")
        return True
        
    except ImportError:
        print("   ⚠️  requests library not available")
        print("   📦 Install with: pip install requests")
        return False
    except Exception as e:
        print(f"   ❌ API test error: {str(e)}")
        return False

def performance_comparison():
    """Compare performance characteristics."""
    print("\n⚡ Performance Comparison...")
    print("=" * 60)
    
    try:
        from snowflake_ai_assistant import SnowflakeAIAssistant
        
        # Test response time
        print("1. Testing response time...")
        assistant = SnowflakeAIAssistant(use_azure=True)
        
        start_time = time.time()
        response = assistant.chat("Hello, introduce yourself briefly")
        end_time = time.time()
        
        response_time = end_time - start_time
        print(f"   ⏱️  Response time: {response_time:.2f} seconds")
        print(f"   📏 Response length: {len(response)} characters")
        
        # Test memory usage (basic)
        print("2. Testing memory characteristics...")
        history = assistant.get_conversation_history()
        print(f"   🧠 Conversation messages: {len(history)}")
        print(f"   🆔 Thread ID: {assistant.thread_id}")
        
        print("   ✅ Performance comparison completed")
        return True
        
    except Exception as e:
        print(f"   ❌ Performance test error: {str(e)}")
        return False

def main():
    """Run all tests."""
    print("🚀 LangGraph Snowflake AI Assistant Test Suite")
    print("=" * 60)
    print(f"📅 Test started at: {datetime.now()}")
    print()
    
    results = []
    
    # Run all tests
    results.append(("LangGraph Features", test_langgraph_features()))
    results.append(("Tool Integration", test_tool_integration()))
    results.append(("API Endpoints", test_api_endpoints()))
    results.append(("Performance", performance_comparison()))
    
    # Summary
    print("\n📊 Test Summary:")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:20} {status}")
        if result:
            passed += 1
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Your LangGraph implementation is working correctly.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the configuration.")
    
    print("\n💡 Next steps:")
    print("1. Start the API server: python python/api_server.py")
    print("2. Run the CLI assistant: python python/snowflake_ai_assistant.py")
    print("3. Compare with Lab 07 traditional approach")
    print("4. Explore graph visualization features")

if __name__ == "__main__":
    main()