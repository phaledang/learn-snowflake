# Simple deployment script for Lab 07c
# Just activates virtual environment and installs requirements.txt

Write-Host "🚀 Simple Lab 07c Deployment" -ForegroundColor Green
Write-Host "=" * 40

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path (Split-Path -Parent $scriptDir) "venv"
$activateScript = Join-Path $venvPath "Scripts\Activate.ps1"
$requirementsFile = Join-Path $scriptDir "requirements.txt"

# Check if virtual environment exists
if (-not (Test-Path $venvPath)) {
    Write-Host "❌ Virtual environment not found at: $venvPath" -ForegroundColor Red
    Write-Host "Please create it first with: python -m venv ..\venv" -ForegroundColor Yellow
    exit 1
}

# Check if requirements.txt exists
if (-not (Test-Path $requirementsFile)) {
    Write-Host "❌ requirements.txt not found at: $requirementsFile" -ForegroundColor Red
    exit 1
}

Write-Host "📁 Script directory: $scriptDir" -ForegroundColor Cyan
Write-Host "🐍 Virtual environment: $venvPath" -ForegroundColor Cyan
Write-Host "📋 Requirements file: $requirementsFile" -ForegroundColor Cyan

# Activate virtual environment
Write-Host "`n🔧 Activating virtual environment..." -ForegroundColor Yellow
try {
    & $activateScript
    Write-Host "✅ Virtual environment activated" -ForegroundColor Green
} catch {
    Write-Host "❌ Failed to activate virtual environment: $_" -ForegroundColor Red
    exit 1
}

# Install requirements
Write-Host "`n📦 Installing packages from requirements.txt..." -ForegroundColor Yellow
try {
    pip install -r $requirementsFile
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ All packages installed successfully" -ForegroundColor Green
    } else {
        Write-Host "⚠️ Some packages may have had issues, but continuing..." -ForegroundColor Yellow
    }
} catch {
    Write-Host "❌ Failed to install requirements: $_" -ForegroundColor Red
    exit 1
}

Write-Host "`n🎉 Deployment completed!" -ForegroundColor Green
Write-Host "You can now run your Python scripts in this environment." -ForegroundColor Cyan