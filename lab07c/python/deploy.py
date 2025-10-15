#!/usr/bin/env python3
"""
Deployment script for Lab 07c Enhanced Snowflake AI Assistant
Properly activates virtual environment and installs dependencies
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

def get_script_dir():
    """Get the directory where this script is located"""
    return Path(__file__).parent.absolute()

def get_project_root():
    """Get the project root directory (lab07c)"""
    return get_script_dir().parent

def get_venv_path():
    """Get the path to the virtual environment"""
    project_root = get_project_root()
    
    # Check different possible virtual environment locations
    possible_venv_paths = [
        project_root / "venv",
        project_root / ".venv", 
        project_root.parent / "lab07" / "venv",  # Use lab07 venv if exists
        project_root.parent / "venv",
    ]
    
    for venv_path in possible_venv_paths:
        if venv_path.exists():
            return venv_path
    
    return None

def get_python_executable(venv_path):
    """Get the Python executable path for the virtual environment"""
    if platform.system() == "Windows":
        return venv_path / "Scripts" / "python.exe"
    else:
        return venv_path / "bin" / "python"

def run_command(cmd, cwd=None):
    """Run a command and return the result"""
    print(f"🔧 Running: {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"✅ Success: {' '.join(cmd)}")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"❌ Failed: {' '.join(cmd)}")
        if result.stderr:
            print(result.stderr)
        if result.stdout:
            print(result.stdout)
    
    return result

def create_virtual_environment():
    """Create a new virtual environment if none exists"""
    project_root = get_project_root()
    venv_path = project_root / "venv"
    
    print(f"📦 Creating virtual environment at: {venv_path}")
    
    result = run_command([sys.executable, "-m", "venv", str(venv_path)])
    
    if result.returncode == 0:
        print(f"✅ Virtual environment created successfully!")
        return venv_path
    else:
        print(f"❌ Failed to create virtual environment")
        return None

def install_requirements(python_executable, requirements_file):
    """Install requirements using the virtual environment's Python"""
    print(f"📋 Installing requirements from: {requirements_file}")
    
    # Upgrade pip first
    result = run_command([
        str(python_executable), "-m", "pip", "install", "--upgrade", "pip"
    ])
    
    if result.returncode != 0:
        print("⚠️ Warning: Failed to upgrade pip, continuing anyway...")
    
    # Install requirements
    result = run_command([
        str(python_executable), "-m", "pip", "install", "-r", str(requirements_file)
    ])
    
    return result.returncode == 0

def verify_installation(python_executable):
    """Verify that key packages are installed correctly"""
    print("🔍 Verifying installation...")
    
    test_imports = [
        "langchain",
        "langchain_core", 
        "langchain_openai",
        "langgraph",
        "azure.identity",
        "msal",
        "dotenv",
        "pandas",
        "snowflake.connector"
    ]
    
    all_good = True
    
    for package in test_imports:
        result = run_command([
            str(python_executable), "-c", f"import {package}; print(f'{package} imported successfully')"
        ])
        
        if result.returncode != 0:
            print(f"❌ Failed to import {package}")
            all_good = False
        else:
            print(f"✅ {package} imported successfully")
    
    return all_good

def main():
    """Main deployment function"""
    print("🚀 Lab 07c Enhanced Snowflake AI Assistant Deployment")
    print("=" * 60)
    
    script_dir = get_script_dir()
    project_root = get_project_root()
    requirements_file = script_dir / "requirements.txt"
    
    print(f"📁 Script directory: {script_dir}")
    print(f"📁 Project root: {project_root}")
    print(f"📋 Requirements file: {requirements_file}")
    
    # Check if requirements.txt exists
    if not requirements_file.exists():
        print(f"❌ Requirements file not found: {requirements_file}")
        return False
    
    # Find or create virtual environment
    venv_path = get_venv_path()
    
    if venv_path is None:
        print("📦 No virtual environment found. Creating one...")
        venv_path = create_virtual_environment()
        if venv_path is None:
            print("❌ Failed to create virtual environment")
            return False
    else:
        print(f"✅ Found existing virtual environment: {venv_path}")
    
    # Get Python executable
    python_executable = get_python_executable(venv_path)
    
    if not python_executable.exists():
        print(f"❌ Python executable not found: {python_executable}")
        return False
    
    print(f"🐍 Using Python: {python_executable}")
    
    # Install requirements
    if not install_requirements(python_executable, requirements_file):
        print("❌ Failed to install requirements")
        return False
    
    # Verify installation
    if not verify_installation(python_executable):
        print("⚠️ Some packages failed verification, but continuing...")
    
    print("\n🎉 Deployment completed successfully!")
    print(f"🐍 Virtual environment: {venv_path}")
    print(f"🔧 Python executable: {python_executable}")
    print("\n💡 To activate the environment manually:")
    
    if platform.system() == "Windows":
        activate_script = venv_path / "Scripts" / "Activate.ps1"
        print(f"   PowerShell: & {activate_script}")
        print(f"   CMD: {venv_path / 'Scripts' / 'activate.bat'}")
    else:
        print(f"   source {venv_path / 'bin' / 'activate'}")
    
    print(f"\n🚀 To run the enhanced assistant:")
    print(f"   {python_executable} enhanced_assistant.py")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n🛑 Deployment cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error during deployment: {e}")
        sys.exit(1)