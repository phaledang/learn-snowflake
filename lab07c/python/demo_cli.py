"""
Simple CLI Demo for Snowflake AI Assistant
Demonstrates basic usage without the full interactive interface.
"""

from snowflake_ai_assistant import SnowflakeAIAssistant
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def demo_assistant():
    """Demonstrate the assistant with predefined examples."""
    
    print("🚀 Snowflake AI Assistant Demo")
    print("=" * 50)
    
    # Initialize assistant
    print("📝 Initializing assistant...")
    try:
        assistant = SnowflakeAIAssistant(use_azure=True)
        print("✅ Assistant initialized successfully!")
    except Exception as e:
        print(f"❌ Failed to initialize assistant: {e}")
        print("\n💡 Make sure you have:")
        print("1. Configured your .env file with Snowflake and OpenAI credentials")
        print("2. Installed all required dependencies: pip install -r requirements.txt")
        print("3. Valid Snowflake connection parameters")
        return
    
    # Demo scenarios
    scenarios = [
        {
            "title": "Schema Inspection",
            "query": "Show me all available tables in the database",
            "description": "Exploring database structure"
        },
        {
            "title": "Sample Data Analysis", 
            "query": "If there's a sales or customer table, show me a sample of the data and basic statistics",
            "description": "Basic data exploration"
        },
        {
            "title": "Data Quality Check",
            "query": "Check for any data quality issues in the available tables, like null values or duplicates",
            "description": "Data quality assessment"
        },
        {
            "title": "Performance Optimization",
            "query": "What are some best practices for optimizing queries in Snowflake?",
            "description": "Query optimization advice"
        }
    ]
    
    for i, scenario in enumerate(scenarios, 1):
        print(f"\n{'='*60}")
        print(f"📊 Scenario {i}: {scenario['title']}")
        print(f"📋 Description: {scenario['description']}")
        print(f"❓ Query: {scenario['query']}")
        print("=" * 60)
        
        try:
            response = assistant.chat(scenario['query'])
            print(f"🤖 Response:\n{response}")
        except Exception as e:
            print(f"❌ Error in scenario {i}: {e}")
        
        # Wait for user input to continue
        input("\n⏸️  Press Enter to continue to next scenario...")
    
    print("\n🎉 Demo completed!")
    print("💡 You can now use the assistant interactively by running: python snowflake_ai_assistant.py")

def test_tools():
    """Test individual tools separately."""
    print("\n🔧 Testing Individual Tools")
    print("=" * 30)
    
    # Test schema inspection
    from snowflake_ai_assistant import SchemaInspectionTool
    
    print("🔍 Testing Schema Inspection Tool...")
    try:
        schema_tool = SchemaInspectionTool()
        result = schema_tool._run("tables")
        print(f"✅ Schema Tool Result:\n{result}")
    except Exception as e:
        print(f"❌ Schema Tool Error: {e}")
    
    # Test basic query
    from snowflake_ai_assistant import SnowflakeQueryTool
    
    print("\n📊 Testing Snowflake Query Tool...")
    try:
        query_tool = SnowflakeQueryTool()
        result = query_tool._run("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        print(f"✅ Query Tool Result:\n{result}")
    except Exception as e:
        print(f"❌ Query Tool Error: {e}")

def check_environment():
    """Check if environment is properly configured."""
    print("🔍 Environment Configuration Check")
    print("=" * 40)
    
    # Check if Snowflake is enabled
    enable_snowflake = os.getenv('ENABLE_SNOWFLAKE', 'true').lower() == 'true'
    
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER', 
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ] if enable_snowflake else []
    
    openai_vars = [
        ['AZURE_OPENAI_API_KEY', 'AZURE_OPENAI_ENDPOINT'],  # Azure OpenAI
        ['OPENAI_API_KEY']  # Direct OpenAI
    ]
    
    # Check Snowflake variables only if enabled
    if enable_snowflake:
        print("📊 Snowflake Configuration:")
        missing_sf = []
        for var in required_vars:
            value = os.getenv(var)
            if value:
                # Mask sensitive information
                if 'PASSWORD' in var or 'KEY' in var:
                    display_value = f"{value[:4]}{'*' * (len(value)-8)}{value[-4:]}" if len(value) > 8 else "***"
                else:
                    display_value = value
                print(f"  ✅ {var}: {display_value}")
            else:
                print(f"  ❌ {var}: Not set")
                missing_sf.append(var)
    else:
        print("📊 Snowflake Configuration:")
        print(f"  ℹ️  Snowflake integration is disabled (ENABLE_SNOWFLAKE=false)")
        missing_sf = []
    
    # Check OpenAI configuration
    print("\n🤖 OpenAI Configuration:")
    has_openai_config = False
    
    for var_group in openai_vars:
        group_complete = all(os.getenv(var) for var in var_group)
        if group_complete:
            has_openai_config = True
            config_type = "Azure OpenAI" if "AZURE" in var_group[0] else "OpenAI Direct"
            print(f"  ✅ {config_type} configuration found")
            for var in var_group:
                value = os.getenv(var)
                if 'KEY' in var:
                    display_value = f"{value[:4]}{'*' * (len(value)-8)}{value[-4:]}" if len(value) > 8 else "***"
                else:
                    display_value = value
                print(f"    ✅ {var}: {display_value}")
            break
    
    if not has_openai_config:
        print("  ❌ No complete OpenAI configuration found")
        print("  💡 Configure either Azure OpenAI or OpenAI Direct API")
    
    # Summary
    print("\n📋 Configuration Summary:")
    if not missing_sf and has_openai_config:
        print("✅ Environment is properly configured!")
        return True
    else:
        print("❌ Environment configuration incomplete")
        if missing_sf:
            print(f"  Missing Snowflake vars: {', '.join(missing_sf)}")
        if not has_openai_config:
            print("  Missing OpenAI configuration")
        print("\n💡 Create a .env file with the required variables")
        return False

def main():
    """Main function with menu options."""
    print("🤖 Snowflake AI Assistant - CLI Demo")
    print("=" * 50)
    
    while True:
        print("\n📋 Available Options:")
        print("1. 🔍 Check Environment Configuration")
        print("2. 🔧 Test Individual Tools")
        print("3. 🚀 Run Full Demo")
        print("4. 💬 Interactive Chat")
        print("5. ❌ Exit")
        
        choice = input("\n👉 Select an option (1-5): ").strip()
        
        if choice == '1':
            check_environment()
        elif choice == '2':
            if check_environment():
                test_tools()
            else:
                print("⚠️  Please fix environment configuration first")
        elif choice == '3':
            if check_environment():
                demo_assistant()
            else:
                print("⚠️  Please fix environment configuration first")
        elif choice == '4':
            if check_environment():
                print("\n🚀 Starting interactive chat...")
                print("💡 Tip: Type 'quit' to return to menu")
                from snowflake_ai_assistant import main as interactive_main
                interactive_main()
            else:
                print("⚠️  Please fix environment configuration first")
        elif choice == '5':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select 1-5.")

if __name__ == "__main__":
    main()