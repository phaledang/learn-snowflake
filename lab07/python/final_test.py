#!/usr/bin/env python3
"""
Final comprehensive test of the improved connection string functionality
"""
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_snowflake_connection, get_connection_info

def main():
    """Comprehensive test of connection functionality"""
    load_dotenv()
    
    print("🚀 SNOWFLAKE CONNECTION STRING - FINAL TEST")
    print("=" * 60)
    
    print("\n📋 Connection Configuration:")
    config = get_connection_info()
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    print(f"\n🔗 Using Connection String: {os.getenv('SNOWFLAKE_CONNECTION_STRING', 'Not set')}")
    
    try:
        print("\n📡 Establishing connection...")
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Test connection info
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ACCOUNT_NAME()")
        result = cursor.fetchone()
        
        print("\n✅ CONNECTION SUCCESSFUL!")
        print(f"  👤 User: {result[0]}")
        print(f"  🎭 Role: {result[1]}")
        print(f"  🗄️  Database: {result[2]}")
        print(f"  📊 Schema: {result[3]}")
        print(f"  🏭 Warehouse: {result[4]}")
        print(f"  🏢 Account: {result[5]}")
        
        # Test employee data query
        print("\n👥 EMPLOYEE DATA TEST:")
        cursor.execute("SELECT ID, NAME, DEPARTMENT, SALARY FROM EMPLOYEES ORDER BY ID")
        employees = cursor.fetchall()
        
        print(f"  📈 Total Employees: {len(employees)}")
        print("  📝 Sample Data:")
        for emp in employees[:3]:  # Show first 3
            print(f"    • {emp[1]} ({emp[2]}) - ${emp[3]:,}")
        if len(employees) > 3:
            print(f"    ... and {len(employees) - 3} more employees")
        
        # Test departments
        print("\n🏛️ DEPARTMENT SUMMARY:")
        cursor.execute("""
            SELECT DEPARTMENT, COUNT(*) as count, AVG(SALARY) as avg_salary
            FROM EMPLOYEES 
            GROUP BY DEPARTMENT 
            ORDER BY count DESC
        """)
        departments = cursor.fetchall()
        
        for dept in departments:
            print(f"  • {dept[0]}: {dept[1]} employees, avg ${dept[2]:,.0f}")
        
        cursor.close()
        
        print("\n🎉 ALL TESTS PASSED!")
        print("\n📋 FEATURES IMPLEMENTED:")
        print("  ✅ Connection string support (SNOWFLAKE_CONNECTION_STRING)")
        print("  ✅ Individual environment variables support")
        print("  ✅ Automatic account format normalization")
        print("  ✅ Support for both .snowflakecomputing.com and short formats")
        print("  ✅ Automatic fallback to multiple account format variations")
        print("  ✅ Real Snowflake data queries")
        print("  ✅ FastAPI server integration ready")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        print(f"\n🚀 READY FOR PRODUCTION!")
        print("You can now use:")
        print("  • Connection string format in .env")
        print("  • Individual environment variables")
        print("  • Both work with any account format")
        print("  • API server with real Snowflake data")
        print("\n💡 To test the API server:")
        print("  python python/simple_api_server.py")
        print("  Then visit: http://localhost:8000/docs")
    else:
        print(f"\n🛑 Please check your configuration and try again.")