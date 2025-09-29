#!/usr/bin/env python3
"""
Environment Setup and Validation Script for Snowflake AI Assistant
This script helps diagnose and fix configuration issues
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def print_header(title: str):
    """Print a formatted header"""
    print("\n" + "=" * 60)
    print(f"🔧 {title}")
    print("=" * 60)

def print_step(step: int, title: str):
    """Print a formatted step"""
    print(f"\n{step}. {title}")
    print("-" * 40)

def check_env_file():
    """Check if .env file exists"""
    env_path = Path('.env')
    template_path = Path('.env.template')
    
    print_step(1, "Environment File Check")
    
    if env_path.exists():
        print("✅ .env file found")
        return True
    elif template_path.exists():
        print("❌ .env file not found, but .env.template exists")
        print("📋 To fix this:")
        print("   1. Copy .env.template to .env")
        print("   2. Fill in your actual credentials")
        print("\n   PowerShell command:")
        print("   Copy-Item .env.template .env")
        return False
    else:
        print("❌ Neither .env nor .env.template found")
        print("📋 You need to create a .env file with your credentials")
        return False

def load_and_check_env():
    """Load environment variables and check what's available"""
    print_step(2, "Loading Environment Variables")
    
    # Load from .env file if it exists
    load_dotenv()
    
    required_vars = {
        "OpenAI/Azure OpenAI": [
            "AZURE_OPENAI_API_KEY",
            "AZURE_OPENAI_ENDPOINT", 
            "AZURE_OPENAI_API_VERSION",
            "AZURE_OPENAI_DEPLOYMENT_NAME"
        ],
        "OpenAI Direct": [
            "OPENAI_API_KEY"
        ],
        "Snowflake": [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA"
        ]
    }
    
    print("🔍 Checking required environment variables...")
    
    all_good = True
    
    # Check Azure OpenAI
    azure_vars = all(os.getenv(var) for var in required_vars["OpenAI/Azure OpenAI"])
    openai_vars = os.getenv("OPENAI_API_KEY") is not None
    
    print("\n📊 AI Service Configuration:")
    if azure_vars:
        print("✅ Azure OpenAI - All variables set")
        ai_configured = True
    elif openai_vars:
        print("✅ OpenAI Direct - API key set")
        ai_configured = True
    else:
        print("❌ No AI service configured")
        print("   Need either:")
        print("   - Azure OpenAI: AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, etc.")
        print("   - OpenAI Direct: OPENAI_API_KEY")
        ai_configured = False
        all_good = False
    
    # Check Snowflake
    print("\n📊 Snowflake Configuration:")
    snowflake_missing = []
    for var in required_vars["Snowflake"]:
        if os.getenv(var):
            print(f"✅ {var}")
        else:
            print(f"❌ {var} - MISSING")
            snowflake_missing.append(var)
    
    if snowflake_missing:
        all_good = False
        print(f"\n⚠️  Missing {len(snowflake_missing)} Snowflake variables")
    else:
        print("✅ All Snowflake variables configured")
    
    return all_good, ai_configured, len(snowflake_missing) == 0

def test_imports():
    """Test if all required packages can be imported"""
    print_step(3, "Package Import Test")
    
    packages = [
        ("fastapi", "FastAPI"),
        ("uvicorn", "Uvicorn"),
        ("langchain_openai", "LangChain OpenAI"),
        ("snowflake.connector", "Snowflake Connector"),
        ("pandas", "Pandas"),
        ("dotenv", "Python-dotenv")
    ]
    
    all_imports_ok = True
    
    for package_name, display_name in packages:
        try:
            __import__(package_name)
            print(f"✅ {display_name}")
        except ImportError as e:
            print(f"❌ {display_name} - {e}")
            all_imports_ok = False
    
    return all_imports_ok

def diagnose_startup_error():
    """Try to identify the specific startup issue"""
    print_step(4, "Startup Error Diagnosis")
    
    print("🔍 Attempting to identify the exact issue...")
    
    try:
        # Try importing and initializing the assistant
        from snowflake_ai_assistant import SnowflakeAIAssistant
        
        print("✅ AI Assistant module imported successfully")
        
        # Try creating instance
        try:
            assistant = SnowflakeAIAssistant()
            print("✅ AI Assistant instance created successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to create AI Assistant: {e}")
            
            # Specific error analysis
            error_msg = str(e).lower()
            if "missing credentials" in error_msg or "api_key" in error_msg:
                print("\n💡 SOLUTION: Missing API credentials")
                print("   The error indicates missing OpenAI/Azure OpenAI API key")
                print("   1. Set AZURE_OPENAI_API_KEY in your .env file")
                print("   2. Or set OPENAI_API_KEY for OpenAI direct")
            elif "snowflake" in error_msg:
                print("\n💡 SOLUTION: Snowflake connection issue")
                print("   Check your Snowflake credentials in .env file")
            else:
                print(f"\n💡 UNKNOWN ERROR: {e}")
            
            return False
            
    except ImportError as e:
        print(f"❌ Failed to import AI Assistant module: {e}")
        return False

def provide_solutions(has_env_file, ai_configured, snowflake_configured, imports_ok):
    """Provide specific solutions based on diagnosis"""
    print_step(5, "Solutions & Next Steps")
    
    if not has_env_file:
        print("🎯 PRIMARY ISSUE: Missing .env file")
        print("   SOLUTION:")
        print("   1. Copy .env.template to .env")
        print("   2. Fill in your credentials")
        print("   3. Restart the API server")
        print("\n   PowerShell:")
        print("   Copy-Item .env.template .env")
        return
    
    if not ai_configured:
        print("🎯 PRIMARY ISSUE: Missing AI service credentials")
        print("   SOLUTION:")
        print("   Add one of these to your .env file:")
        print("\n   For Azure OpenAI:")
        print("   AZURE_OPENAI_API_KEY=your-key")
        print("   AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/")
        print("   AZURE_OPENAI_API_VERSION=2024-02-15-preview")
        print("   AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4")
        print("\n   OR for OpenAI Direct:")
        print("   OPENAI_API_KEY=your-openai-api-key")
        return
    
    if not snowflake_configured:
        print("🎯 SECONDARY ISSUE: Missing Snowflake credentials")
        print("   SOLUTION:")
        print("   Add these to your .env file:")
        print("   SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com")
        print("   SNOWFLAKE_USER=your-username")
        print("   SNOWFLAKE_PASSWORD=your-password")
        print("   SNOWFLAKE_WAREHOUSE=COMPUTE_WH")
        print("   SNOWFLAKE_DATABASE=your-database")
        print("   SNOWFLAKE_SCHEMA=PUBLIC")
        return
    
    if not imports_ok:
        print("🎯 ISSUE: Missing Python packages")
        print("   SOLUTION:")
        print("   pip install -r python/requirements.txt")
        return
    
    print("✅ Configuration looks good!")
    print("   Try starting the API server:")
    print("   python python/api_server.py")

def main():
    print_header("Snowflake AI Assistant - Configuration Diagnosis")
    print("This script will help identify and fix configuration issues.")
    
    # Check if we're in the right directory
    if not Path("python/snowflake_ai_assistant.py").exists():
        print("❌ Please run this script from the lab07 directory")
        return
    
    # Run all checks
    has_env_file = check_env_file()
    all_env_good, ai_configured, snowflake_configured = load_and_check_env()
    imports_ok = test_imports()
    startup_ok = False
    
    if has_env_file and ai_configured:
        startup_ok = diagnose_startup_error()
    
    # Provide solutions
    provide_solutions(has_env_file, ai_configured, snowflake_configured, imports_ok)
    
    # Summary
    print_header("Summary")
    status_items = [
        ("Environment file (.env)", "✅" if has_env_file else "❌"),
        ("AI service credentials", "✅" if ai_configured else "❌"),
        ("Snowflake credentials", "✅" if snowflake_configured else "❌"),
        ("Python packages", "✅" if imports_ok else "❌"),
        ("Assistant startup", "✅" if startup_ok else "❌")
    ]
    
    for item, status in status_items:
        print(f"{status} {item}")
    
    if all([has_env_file, ai_configured, snowflake_configured, imports_ok, startup_ok]):
        print("\n🎉 All checks passed! Your environment is ready.")
        print("   Start the API server: python python/api_server.py")
    else:
        print("\n⚠️  Configuration needs attention. Follow the solutions above.")

if __name__ == "__main__":
    main()