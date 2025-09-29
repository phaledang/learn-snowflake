#!/usr/bin/env python3
"""
Getting Started with Snowflake AI Assistant FastAPI
This script guides you through the complete API setup and testing workflow
"""

import os
import sys
import subprocess
from pathlib import Path
import time

def print_header(title: str):
    """Print a formatted header"""
    print("\n" + "=" * 60)
    print(f"🚀 {title}")
    print("=" * 60)

def print_step(step: int, title: str):
    """Print a formatted step"""
    print(f"\n{step}. {title}")
    print("-" * 40)

def check_virtual_env():
    """Check if virtual environment is activated"""
    return hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)

def main():
    print_header("Snowflake AI Assistant FastAPI - Getting Started Guide")
    
    print("This script will guide you through setting up and testing the Snowflake AI Assistant API.")
    print("Follow along to test the primary use case: 'show me the employee list'")
    
    # Check environment
    print_step(1, "Environment Check")
    
    if check_virtual_env():
        print("✅ Virtual environment is activated")
    else:
        print("❌ Virtual environment not detected")
        print("Please run: .\\venv\\Scripts\\Activate.ps1 (Windows)")
        print("Then run this script again")
        return
    
    # Check if we're in the right directory
    current_dir = Path.cwd()
    if not (current_dir / "python" / "demo_api_responses.py").exists():
        print("❌ Not in the lab07 directory or missing files")
        print("Please run this script from the lab07 directory")
        return
    
    print("✅ In the correct lab07 directory")
    
    # Step 2: Show available options
    print_step(2, "API Server Options")
    print("We have two ways to test the API:")
    print("A) Demo Mode - No credentials required, shows API structure")
    print("B) Production Mode - Requires API keys, full functionality")
    
    print("\nFor this demo, we'll use Demo Mode (Option A)")
    
    # Step 3: Run demo responses
    print_step(3, "Running API Response Demo")
    print("This shows what the API endpoints would return...")
    
    try:
        subprocess.run([sys.executable, "python/demo_api_responses.py"], check=True)
        print("\n✅ API response demo completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running demo: {e}")
        return
    
    # Step 4: Instructions for server testing
    print_step(4, "Server Testing Instructions")
    print("To test with a running server:")
    print()
    print("1. Open a new terminal/PowerShell window")
    print("2. Navigate to the lab07 directory")
    print("3. Activate the virtual environment: .\\venv\\Scripts\\Activate.ps1")
    print("4. Start the demo server: python python/demo_api_server.py")
    print("5. Open browser to: http://localhost:8000/docs")
    print("6. Try the /employees endpoint for the employee list")
    print()
    print("OR run the test suite: python python/test_fastapi.py")
    
    # Step 5: Next steps
    print_step(5, "Key Endpoints to Try")
    print("🎯 Primary Use Case: 'show me the employee list'")
    print("   - GET /employees (direct data)")
    print('   - POST /chat with {"message": "show me the employee list"} (conversational)')
    print()
    print("📋 Other Useful Endpoints:")
    print("   - GET /health (health check)")
    print("   - GET /status (assistant status)")
    print("   - GET /schema/tables (database structure)")
    print("   - GET /data/sample (sample data)")
    
    print_step(6, "Production Setup (Optional)")
    print("For full functionality with real Snowflake data:")
    print("1. Set environment variables:")
    print("   $env:OPENAI_API_KEY = 'your-key-here'")
    print("   # OR for Azure OpenAI:")
    print("   $env:AZURE_OPENAI_API_KEY = 'your-azure-key'")
    print("   $env:AZURE_OPENAI_ENDPOINT = 'your-endpoint'")
    print()
    print("2. Configure Snowflake credentials in the assistant")
    print("3. Run: python python/api_server.py")
    
    print_header("Setup Complete!")
    print("🎯 You've successfully explored the Snowflake AI Assistant API structure.")
    print("📖 The API provides structured endpoints for database interaction and AI queries.")
    print("🔧 Use the demo mode for development and testing without credentials.")
    print("🚀 Deploy in production mode when ready with real credentials.")
    print()
    print("Next: Try opening http://localhost:8000/docs in your browser after starting the server!")

if __name__ == "__main__":
    main()