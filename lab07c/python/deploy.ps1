# PowerShell Deployment Script for Lab 07c Enhanced Snowflake AI Assistant
# Properly activates virtual environment and installs dependencies

param(
    [switch]$CreateNew,
    [switch]$Force
)

# Colors for output
$colors = @{
    'Green' = 'Green'
    'Red' = 'Red' 
    'Yellow' = 'Yellow'
    'Cyan' = 'Cyan'
    'Magenta' = 'Magenta'
}

function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = 'White'
    )
    Write-Host $Message -ForegroundColor $Color
}

function Test-PythonVersion {
    try {
        $pythonVersion = python --version 2>$null
        if ($pythonVersion -match "Python (\d+)\.(\d+)") {
            $major = [int]$Matches[1]
            $minor = [int]$Matches[2]
            if ($major -ge 3 -and $minor -ge 8) {
                Write-ColorOutput "✅ Python version: $pythonVersion" $colors.Green
                return $true
            } else {
                Write-ColorOutput "❌ Python 3.8+ required. Found: $pythonVersion" $colors.Red
                return $false
            }
        }
    } catch {
        Write-ColorOutput "❌ Python not found in PATH" $colors.Red
        return $false
    }
}

function Find-VirtualEnvironment {
    $script:scriptDir = Split-Path -Parent $MyInvocation.ScriptName
    $script:projectRoot = Split-Path -Parent $script:scriptDir
    
    $possibleVenvPaths = @(
        (Join-Path $script:projectRoot "venv"),
        (Join-Path $script:projectRoot ".venv"),
        (Join-Path (Split-Path -Parent $script:projectRoot) "lab07" "venv"),
        (Join-Path (Split-Path -Parent $script:projectRoot) "venv")
    )
    
    foreach ($venvPath in $possibleVenvPaths) {
        if (Test-Path $venvPath) {
            Write-ColorOutput "✅ Found virtual environment: $venvPath" $colors.Green
            return $venvPath
        }
    }
    
    return $null
}

function New-VirtualEnvironment {
    $script:projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.ScriptName)
    $venvPath = Join-Path $script:projectRoot "venv"
    
    Write-ColorOutput "📦 Creating virtual environment at: $venvPath" $colors.Cyan
    
    try {
        python -m venv $venvPath
        if ($LASTEXITCODE -eq 0) {
            Write-ColorOutput "✅ Virtual environment created successfully!" $colors.Green
            return $venvPath
        } else {
            Write-ColorOutput "❌ Failed to create virtual environment" $colors.Red
            return $null
        }
    } catch {
        Write-ColorOutput "❌ Error creating virtual environment: $_" $colors.Red
        return $null
    }
}

function Install-Requirements {
    param(
        [string]$VenvPath,
        [string]$RequirementsFile
    )
    
    $pythonExe = Join-Path $VenvPath "Scripts" "python.exe"
    $pipExe = Join-Path $VenvPath "Scripts" "pip.exe"
    
    if (-not (Test-Path $pythonExe)) {
        Write-ColorOutput "❌ Python executable not found: $pythonExe" $colors.Red
        return $false
    }
    
    Write-ColorOutput "📋 Installing requirements from: $RequirementsFile" $colors.Cyan
    
    # Upgrade pip first
    Write-ColorOutput "🔧 Upgrading pip..." $colors.Yellow
    & $pythonExe -m pip install --upgrade pip
    
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "⚠️ Warning: Failed to upgrade pip, continuing anyway..." $colors.Yellow
    }
    
    # Install requirements
    Write-ColorOutput "📦 Installing packages..." $colors.Cyan
    & $pythonExe -m pip install -r $RequirementsFile
    
    if ($LASTEXITCODE -eq 0) {
        Write-ColorOutput "✅ Requirements installed successfully!" $colors.Green
        return $true
    } else {
        Write-ColorOutput "❌ Failed to install requirements" $colors.Red
        return $false
    }
}

function Test-Installation {
    param([string]$VenvPath)
    
    $pythonExe = Join-Path $VenvPath "Scripts" "python.exe"
    
    Write-ColorOutput "🔍 Verifying installation..." $colors.Cyan
    
    $testImports = @(
        "langchain",
        "langchain_core", 
        "langchain_openai",
        "langgraph",
        "azure.identity",
        "msal",
        "dotenv",
        "pandas"
    )
    
    $allGood = $true
    
    foreach ($package in $testImports) {
        try {
            & $pythonExe -c "import $package; print('$package imported successfully')" 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-ColorOutput "✅ $package imported successfully" $colors.Green
            } else {
                Write-ColorOutput "❌ Failed to import $package" $colors.Red
                $allGood = $false
            }
        } catch {
            Write-ColorOutput "❌ Error testing $package`: $($_.Exception.Message)" $colors.Red
            $allGood = $false
        }
    }
    
    return $allGood
}

# Main deployment function
function Start-Deployment {
    Write-ColorOutput "🚀 Lab 07c Enhanced Snowflake AI Assistant Deployment" $colors.Magenta
    Write-ColorOutput ("=" * 60) $colors.Magenta
    
    # Test Python version
    if (-not (Test-PythonVersion)) {
        Write-ColorOutput "Please install Python 3.8+ and ensure it's in your PATH" $colors.Red
        exit 1
    }
    
    $scriptDir = Split-Path -Parent $MyInvocation.ScriptName
    $requirementsFile = Join-Path $scriptDir "requirements.txt"
    
    Write-ColorOutput "📁 Script directory: $scriptDir" $colors.Cyan
    Write-ColorOutput "📋 Requirements file: $requirementsFile" $colors.Cyan
    
    # Check if requirements.txt exists
    if (-not (Test-Path $requirementsFile)) {
        Write-ColorOutput "❌ Requirements file not found: $requirementsFile" $colors.Red
        exit 1
    }
    
    # Find or create virtual environment
    $venvPath = Find-VirtualEnvironment
    
    if ($venvPath -eq $null -or $CreateNew) {
        if ($CreateNew) {
            Write-ColorOutput "📦 Creating new virtual environment as requested..." $colors.Yellow
        } else {
            Write-ColorOutput "📦 No virtual environment found. Creating one..." $colors.Yellow
        }
        
        $venvPath = New-VirtualEnvironment
        if ($venvPath -eq $null) {
            Write-ColorOutput "❌ Failed to create virtual environment" $colors.Red
            exit 1
        }
    }
    
    $pythonExe = Join-Path $venvPath "Scripts" "python.exe"
    Write-ColorOutput "🐍 Using Python: $pythonExe" $colors.Cyan
    
    # Install requirements
    if (-not (Install-Requirements -VenvPath $venvPath -RequirementsFile $requirementsFile)) {
        Write-ColorOutput "❌ Failed to install requirements" $colors.Red
        exit 1
    }
    
    # Verify installation
    if (-not (Test-Installation -VenvPath $venvPath)) {
        Write-ColorOutput "⚠️ Some packages failed verification, but continuing..." $colors.Yellow
    }
    
    Write-ColorOutput "`n🎉 Deployment completed successfully!" $colors.Green
    Write-ColorOutput "🐍 Virtual environment: $venvPath" $colors.Cyan
    Write-ColorOutput "🔧 Python executable: $pythonExe" $colors.Cyan
    
    Write-ColorOutput "`n💡 To activate the environment manually:" $colors.Yellow
    $activateScript = Join-Path $venvPath "Scripts" "Activate.ps1"
    Write-ColorOutput "   & $activateScript" $colors.Cyan
    
    Write-ColorOutput "`n🚀 To run the enhanced assistant:" $colors.Yellow
    Write-ColorOutput "   & $pythonExe enhanced_assistant.py" $colors.Cyan
}

# Run deployment
try {
    Start-Deployment
} catch {
    Write-ColorOutput "`n💥 Unexpected error during deployment: $($_.Exception.Message)" $colors.Red
    exit 1
}