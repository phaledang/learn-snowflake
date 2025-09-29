#!/usr/bin/env python3
"""
Test script for SQLAlchemy-based Snowflake pandas integration
"""
import os
import sys
from pathlib import Path

# Add the current directory to Python path for imports
sys.path.append(str(Path(__file__).parent))

from sqlalchemy_integration import SnowflakeDataAnalyzer
import pandas as pd

def test_sqlalchemy_integration():
    """Test the SQLAlchemy-based Snowflake connection"""
    print("🔧 Testing SQLAlchemy Integration with Snowflake")
    print("=" * 50)
    
    try:
        # Initialize analyzer
        print("📡 Creating SQLAlchemy connection...")
        analyzer = SnowflakeDataAnalyzer()
        print("✅ SQLAlchemy engine created successfully")
        
        # Test basic query
        print("\n🔍 Testing basic query...")
        test_query = """
        SELECT 
            'SQLAlchemy' as connection_type,
            CURRENT_USER() as current_user,
            CURRENT_DATABASE() as current_database,
            CURRENT_SCHEMA() as current_schema,
            CURRENT_WAREHOUSE() as current_warehouse,
            CURRENT_TIMESTAMP() as query_time
        """
        
        df = analyzer.query_to_dataframe(test_query)
        
        if df is not None:
            print("✅ Query executed successfully!")
            print(f"📊 Result shape: {df.shape}")
            print("\nQuery results:")
            print(df.to_string(index=False))
            
            # Test writing data back to Snowflake
            print("\n💾 Testing write operation...")
            
            # Create a sample DataFrame
            sample_data = pd.DataFrame({
                'test_id': [1, 2, 3],
                'test_name': ['SQLAlchemy Test 1', 'SQLAlchemy Test 2', 'SQLAlchemy Test 3'],
                'test_value': [100.5, 200.7, 300.9],
                'test_timestamp': pd.Timestamp.now()
            })
            
            print(f"📝 Writing {len(sample_data)} rows to test table...")
            analyzer.dataframe_to_snowflake(sample_data, 'python_sqlalchemy_test')
            
            # Verify the write by reading back
            print("\n🔍 Verifying write operation...")
            verify_df = analyzer.query_to_dataframe("SELECT * FROM python_sqlalchemy_test ORDER BY test_id")
            
            if verify_df is not None:
                print("✅ Write verification successful!")
                print(f"📊 Verified data shape: {verify_df.shape}")
                print("\nVerified data:")
                print(verify_df.to_string(index=False))
            else:
                print("⚠️ Could not verify write operation")
            
        else:
            print("❌ Query failed")
            return False
            
        # Cleanup
        print("\n🧹 Cleaning up...")
        try:
            analyzer.query_to_dataframe("DROP TABLE IF EXISTS python_sqlalchemy_test")
            print("✅ Test table cleaned up")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")
        
        analyzer.close()
        print("✅ Connection closed")
        
        print("\n🎉 SQLAlchemy integration test completed successfully!")
        print("🔇 The pandas UserWarning should now be eliminated!")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def check_requirements():
    """Check if required packages are installed"""
    print("📋 Checking required packages...")
    
    required_packages = [
        'pandas',
        'sqlalchemy', 
        'snowflake-sqlalchemy',
        'python-dotenv'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - MISSING")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❗ Missing packages: {', '.join(missing_packages)}")
        print("💡 Install with: pip install " + " ".join(missing_packages))
        return False
    
    print("✅ All required packages are installed")
    return True

def main():
    """Main test function"""
    print("🧪 SQLALCHEMY SNOWFLAKE INTEGRATION TEST")
    print("=" * 60)
    
    # Check requirements
    if not check_requirements():
        print("\n❌ Please install missing packages before running the test")
        return
    
    print()
    
    # Check environment variables
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER', 
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ]
    
    print("🌍 Checking environment variables...")
    missing_vars = []
    for var in required_vars:
        if os.getenv(var):
            print(f"✅ {var}")
        else:
            print(f"❌ {var} - NOT SET")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n❗ Missing environment variables: {', '.join(missing_vars)}")
        print("💡 Make sure your .env file is configured properly")
        return
    
    print("✅ All environment variables are set")
    print()
    
    # Run the integration test
    if test_sqlalchemy_integration():
        print("\n🎉 SUCCESS: SQLAlchemy integration is working correctly!")
        print("📝 You can now use pandas.read_sql without warnings")
    else:
        print("\n❌ FAILURE: Integration test failed")
        print("🔍 Check your connection settings and try again")

if __name__ == "__main__":
    main()