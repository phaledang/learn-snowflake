#!/usr/bin/env python3
"""
Test script for smart thread naming functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from state_persistence import generate_thread_name, resolve_thread_name_conflicts

def test_thread_naming():
    """Test the thread naming functionality"""
    print("🧪 Testing Smart Thread Naming")
    print("=" * 40)
    
    # Test basic naming
    test_cases = [
        "How do I connect to Snowflake?",
        "Hi, can you help me with SQL queries?",
        "I need to create a table in Snowflake with customer data",
        "Please assist me with data analysis",
        "What is the best way to optimize my queries?",
        "Hello! I want to learn about window functions",
        "",  # Empty string
        "This is a very long question that exceeds the normal length limit and should be truncated properly to fit within the 200 character limit that we have set for thread names",
    ]
    
    print("\n📝 Thread Name Generation Tests:")
    for i, message in enumerate(test_cases, 1):
        name = generate_thread_name(message)
        print(f"{i}. '{message[:50]}{'...' if len(message) > 50 else ''}'")
        print(f"   → '{name}'\n")
    
    # Test conflict resolution
    print("\n🔄 Name Conflict Resolution Tests:")
    base_name = "How do I connect to Snowflake?"
    existing_names = [
        "How do I connect to Snowflake?",
        "How do I connect to Snowflake? (2)",
        "How do I connect to Snowflake? (3)",
    ]
    
    print(f"Base name: '{base_name}'")
    print(f"Existing names: {existing_names}")
    
    resolved = resolve_thread_name_conflicts(base_name, existing_names)
    print(f"Resolved name: '{resolved}'")
    
    # Test another conflict
    new_name = "SQL query help"
    existing_names2 = ["SQL query help", "Data analysis"]
    resolved2 = resolve_thread_name_conflicts(new_name, existing_names2)
    print(f"\nBase name: '{new_name}'")
    print(f"Existing names: {existing_names2}")
    print(f"Resolved name: '{resolved2}'")
    
    print("\n✅ All thread naming tests completed!")

if __name__ == "__main__":
    test_thread_naming()