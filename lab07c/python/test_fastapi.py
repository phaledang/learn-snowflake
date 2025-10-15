#!/usr/bin/env python3
"""
Test script for FastAPI Snowflake AI Assistant
Tests all API endpoints including the primary "show me the employee list" functionality
"""

import requests
import json
import time
from datetime import datetime

# FastAPI server URL
BASE_URL = "http://localhost:8000"

def test_endpoint(method, endpoint, data=None, description=""):
    """Test a specific API endpoint."""
    url = f"{BASE_URL}{endpoint}"
    
    print(f"\n🔍 Testing: {description}")
    print(f"   Method: {method.upper()}")
    print(f"   URL: {url}")
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, timeout=30)
        elif method.upper() == "POST":
            response = requests.post(url, json=data, timeout=30)
        else:
            print(f"❌ Unsupported method: {method}")
            return False
        
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success!")
            
            # Print relevant parts of the response
            if "response" in result:
                response_text = result["response"]
                if len(response_text) > 200:
                    print(f"   Response: {response_text[:200]}...")
                else:
                    print(f"   Response: {response_text}")
            elif "test_results" in result:
                print(f"   Test Results: {result['successful_queries']}/{result['total_queries']} successful")
            elif "status" in result:
                print(f"   Status: {result['status']}")
            else:
                print(f"   Response: {json.dumps(result, indent=2, default=str)}")
                
            return True
        else:
            print(f"❌ Failed with status {response.status_code}")
            try:
                error_detail = response.json()
                print(f"   Error: {error_detail}")
            except:
                print(f"   Error: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Make sure the FastAPI server is running on localhost:8000")
        return False
    except requests.exceptions.Timeout:
        print("❌ Timeout: Request took too long")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Run all API tests."""
    print("🚀 FastAPI Snowflake AI Assistant - API Test Suite")
    print("=" * 60)
    print(f"Testing server at: {BASE_URL}")
    print(f"Test started at: {datetime.now()}")
    print("=" * 60)
    
    # Test cases
    test_cases = [
        {
            "method": "GET",
            "endpoint": "/health",
            "description": "Health Check - Verify server is running"
        },
        {
            "method": "GET", 
            "endpoint": "/status",
            "description": "Status Check - Verify assistant initialization"
        },
        {
            "method": "GET",
            "endpoint": "/employees",
            "description": "🎯 PRIMARY TEST: Get Employee List"
        },
        {
            "method": "GET",
            "endpoint": "/schema/tables", 
            "description": "Schema Exploration - Get available tables"
        },
        {
            "method": "GET",
            "endpoint": "/data/sample",
            "description": "Data Sampling - Get sample data"
        },
        {
            "method": "POST",
            "endpoint": "/chat",
            "data": {"message": "show me the employee list"},
            "description": "🎯 Chat Endpoint - Employee List Query"
        },
        {
            "method": "POST",
            "endpoint": "/chat", 
            "data": {"message": "How many employees do we have?"},
            "description": "Chat Endpoint - Employee Count Query"
        },
        {
            "method": "GET",
            "endpoint": "/test/queries",
            "description": "Automated Test Suite - Run predefined queries"
        },
        {
            "method": "GET",
            "endpoint": "/memory/history",
            "description": "Memory Management - Get conversation history"
        }
    ]
    
    # Run tests
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'='*60}")
        print(f"Test {i}/{len(test_cases)}")
        
        success = test_endpoint(
            test_case["method"],
            test_case["endpoint"],
            test_case.get("data"),
            test_case["description"]
        )
        
        results.append({
            "test": test_case["description"],
            "endpoint": test_case["endpoint"],
            "success": success
        })
        
        # Small delay between tests
        time.sleep(1)
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    successful_tests = sum(1 for r in results if r["success"])
    
    for i, result in enumerate(results, 1):
        status = "✅ PASS" if result["success"] else "❌ FAIL"
        print(f"{status} Test {i}: {result['test']}")
    
    print(f"\n📈 Overall Results: {successful_tests}/{len(results)} tests passed")
    
    if successful_tests == len(results):
        print("🎉 All tests passed! FastAPI server is working correctly.")
    else:
        print("⚠️  Some tests failed. Check the error messages above.")
    
    # Additional interactive test
    print(f"\n{'='*60}")
    print("🎮 INTERACTIVE TEST")
    print("=" * 60)
    print("You can now test the API interactively:")
    print(f"1. Open your browser to: {BASE_URL}/docs")
    print("2. Try the /employees endpoint for employee list")
    print("3. Use the /chat endpoint with custom messages")
    print("4. Check the full API documentation")
    
    return successful_tests == len(results)

if __name__ == "__main__":
    print("🧪 Starting FastAPI Test Suite...")
    print("Make sure the FastAPI server is running with: python python/api_server.py")
    print()
    
    # Small delay to ensure server is ready
    time.sleep(2)
    
    success = main()
    
    print(f"\n🏁 Testing completed!")
    
    if success:
        print("✅ All tests passed - API is ready for use!")
    else:
        print("❌ Some tests failed - check server logs and configuration")
    
    print("\n💡 Next steps:")
    print("  - Visit http://localhost:8000/docs for interactive API docs")
    print("  - Use curl or Postman to test endpoints manually")
    print("  - Integrate the API with your applications")