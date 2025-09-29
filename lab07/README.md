# Lab 07: Advanced Python LangChain OpenAI Assistant

## 🎯 Objectives
By the end of this lab, you will:
- Build an advanced AI assistant using LangChain and OpenAI
- Integrate natural language processing with Snowflake databases
- Implement conversation memory and context management
- Create custom tools for database interaction and file processing
- Deploy both CLI and web-based interfaces
- Apply business guidelines and governance to AI operations
- Handle file attachments and code interpretation

## ⏱️ Estimated Time: 90 minutes

## 📋 Prerequisites
- Completed Labs 01-06
- Python 3.8+ installed
- Understanding of LangChain framework concepts
- OpenAI API access (Azure OpenAI or OpenAI direct)
- Knowledge of conversational AI principles

## 🤖 Overview: Snowflake AI Assistant

We'll build a sophisticated AI assistant that combines the power of large language models with Snowflake's data capabilities. The assistant can:

- **Natural Language SQL**: Convert business questions into SQL queries
- **Schema Understanding**: Automatically explore and understand database structures
- **File Processing**: Analyze uploaded documents and data files
- **Memory Management**: Maintain conversation context across interactions
- **Business Compliance**: Follow organizational guidelines and governance rules
- **Multi-Modal Interface**: Support both CLI and web-based interactions

## 🛠️ Step 1: Environment Setup

### 1.1 Create Virtual Environment

First, create and activate a Python virtual environment to isolate dependencies:

**Windows (PowerShell):**
```powershell
# Navigate to lab07 directory
cd lab07

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Verify activation (should show (venv) in prompt)

## 🚀 FastAPI Server Setup & Testing

### Quick Start Guide

We've created a FastAPI REST API server for the Snowflake AI Assistant that provides structured endpoints for database interaction and AI queries.

#### 1. Environment Validation

Ensure your virtual environment is properly configured:

```powershell
# Activate the virtual environment (if not already active)
.\venv\Scripts\Activate.ps1

# Validate environment setup
python python/validate_environment.py
```

#### 2. API Server Options

**Option A: Demo Mode (No Credentials Required)**
```powershell
# Run the demo server that simulates API responses
python python/demo_api_server.py
```

**Option B: Full Production Mode (Requires Credentials)**
```powershell
# Set your API credentials first
$env:OPENAI_API_KEY = "your-openai-api-key-here"
# OR for Azure OpenAI:
$env:AZURE_OPENAI_API_KEY = "your-azure-openai-key"
$env:AZURE_OPENAI_ENDPOINT = "your-azure-endpoint"

# Run the full production server
python python/api_server.py
```

#### 3. Testing the API

**Primary Test Case: "Show me the employee list"**

Once the server is running, test it using any of these methods:

**Method 1: Demo Script**
```powershell
# Run the comprehensive demo that shows all API responses
python python/demo_api_responses.py
```

**Method 2: FastAPI Test Suite**
```powershell
# Run automated tests against the running server
python python/test_fastapi.py
```

**Method 3: Interactive Browser**
```powershell
# Open your browser to: http://localhost:8080/docs
# This provides a full interactive API documentation interface
```

**Method 4: PowerShell/curl Commands**
```powershell
# Test the primary employee endpoint
Invoke-RestMethod -Uri "http://localhost:8080/employees" -Method GET

# Test the chat endpoint with the specific query
$body = @{ message = "show me the employee list" } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/chat" -Method POST -Body $body -ContentType "application/json"

# Test health check
Invoke-RestMethod -Uri "http://localhost:8080/health" -Method GET
```

### 📋 Available API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check and server status |
| `/status` | GET | AI assistant initialization status |
| `/employees` | GET | 🎯 **Primary**: Get complete employee list |
| `/chat` | POST | Interactive chat with AI assistant |
| `/schema/tables` | GET | Database schema exploration |
| `/data/sample` | GET | Sample data from tables |
| `/test/queries` | GET | Run automated test queries |
| `/memory/clear` | POST | Clear conversation memory |
| `/memory/history` | GET | Get conversation history |

### 🎯 Primary Use Case Example

The main test case "show me the employee list" can be accessed via:

1. **Direct endpoint**: `GET /employees` - Returns structured JSON with employee data
2. **Chat interface**: `POST /chat` with `{"message": "show me the employee list"}` - Returns conversational response
3. **Interactive docs**: Visit `http://localhost:8080/docs` and try the endpoints directly

### 📊 Expected Response Format

When you query for the employee list, you'll get responses like:

**Direct API Response:**
```json
{
  "status": "success",
  "count": 4,
  "data": [
    {
      "employee_id": 1001,
      "name": "John Smith",
      "department": "Engineering",
      "position": "Senior Developer",
      "salary": 95000
    }
    // ... more employees
  ]
}
```

**Chat Response:**
```json
{
  "response": "Here are all the employees in the database:\n• John Smith (Engineering) - Senior Developer\n• Jane Doe (Sales) - Account Manager\n...",
  "timestamp": "2025-01-20T10:30:00",
  "success": true
}
```

### 🔧 Troubleshooting

**Server Won't Start:**
- Check that the virtual environment is activated
- Verify all dependencies are installed: `pip install -r python/requirements.txt`
- Try the demo mode first: `python python/demo_api_server.py`

**Connection Errors:**
- Ensure the server is running on `http://localhost:8080`
- Check that no other process is using port 8080
- Try the demo responses script: `python python/demo_api_responses.py`

**Missing Credentials:**
- For demo mode, no credentials are required
- For production mode, set `OPENAI_API_KEY` or Azure OpenAI credentials

### 💡 Next Steps

After successful API testing:
1. Integrate the API endpoints into your applications
2. Customize the chat queries for your specific use cases
3. Explore the schema endpoints to understand your data structure
4. Use the interactive documentation for development

---
```

**Windows (Command Prompt):**
```cmd
# Navigate to lab07 directory
cd lab07

# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate.bat
```

**macOS/Linux:**
```bash
# Navigate to lab07 directory
cd lab07

# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate
```

### 1.2 Install Required Dependencies

With the virtual environment activated, install the required packages:

```bash
# Install dependencies from requirements file
pip install -r python/requirements.txt


# Verify installation
pip list
```

### 1.3 Environment Configuration

Create your `.env` file from the example:

```bash
# Copy example environment file
cp .env.example .env

# Edit with your actual credentials
nano .env
```

## 🔗 Connection Configuration Options

You have **two flexible options** for configuring your Snowflake connection:

### Option A: Connection String (Recommended) ⭐

Use a single connection string for all Snowflake settings:

```env
# Snowflake Connection String (All-in-One)
SNOWFLAKE_CONNECTION_STRING=snowflake://USERNAME:PASSWORD@ACCOUNT/DATABASE/SCHEMA?warehouse=WAREHOUSE_NAME&role=ROLE_NAME



**✅ Benefits of Connection String:**
- Single line configuration
- Works with both account formats (with or without `.snowflakecomputing.com`)
- Automatic account format normalization
- URL encoding handled automatically
- Easy to copy/paste and share (without sensitive data)

**🔧 Account Format Support:**
Both of these formats work seamlessly:
```env
# Short format (recommended)
SNOWFLAKE_CONNECTION_STRING=snowflake://user:pass@account.region.cloud/DB/SCHEMA?warehouse=WH&role=ROLE

# Full format (also works)
SNOWFLAKE_CONNECTION_STRING=snowflake://user:pass@account.region.cloud.snowflakecomputing.com/DB/SCHEMA?warehouse=WH&role=ROLE
```

### Option B: Individual Environment Variables

Traditional approach with separate variables:

```env
# Snowflake Connection (Individual Parameters)
SNOWFLAKE_ACCOUNT=your_account_identifier  # e.g., name.east-us-2.azure
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=LEARN_WH
SNOWFLAKE_DATABASE=LEARN_SNOWFLAKE
SNOWFLAKE_SCHEMA=SANDBOX
SNOWFLAKE_ROLE=ACCOUNTADMIN  # Important: Include role for proper permissions
```

**💡 Connection Priority:**
1. If `SNOWFLAKE_CONNECTION_STRING` is set, it takes priority
2. Otherwise, individual parameters are used
3. Automatic fallback with multiple account format attempts

## 🤖 AI Provider Configuration

Choose your preferred AI provider:

```env
# Azure OpenAI (recommended)
AZURE_OPENAI_API_KEY=your_azure_openai_api_key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-11-20
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4o

# Alternative: Direct OpenAI API
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4-turbo-preview

# Optional Configuration
ASSISTANT_NAME=SnowflakeAI
MAX_CONVERSATION_MEMORY=50
```

### 1.4 Verify Configuration

**Test Connection String Configuration:**
```bash
# Test the connection string approach
python python/test_connection_formats.py

# Test both connection string and individual variables
python python/test_env_vars_formats.py

# Comprehensive connection test
python python/final_test.py
```

**Test Environment Setup:**
```bash
# Test environment setup
python validate_environment.py

# If successful, test the demo
python python/demo_cli.py
```

**Expected Output:**
```
🚀 SNOWFLAKE CONNECTION STRING - FINAL TEST
============================================================

📋 Connection Configuration:
  account: your-account.region.cloud
  user: your-username
  password: ***
  database: LEARN_SNOWFLAKE
  schema: SANDBOX
  warehouse: LEARN_WH
  role: ACCOUNTADMIN

✅ CONNECTION SUCCESSFUL!
  👤 User: YOUR-USERNAME
  🎭 Role: ACCOUNTADMIN
  🗄️  Database: LEARN_SNOWFLAKE
  📊 Schema: SANDBOX
  🏭 Warehouse: LEARN_WH

👥 EMPLOYEE DATA TEST:
  📈 Total Employees: 8
  📝 Sample Data:
    • John Smith (Engineering) - $75,000
    • Jane Doe (Marketing) - $65,000
    • Bob Johnson (Sales) - $60,000
    ... and 5 more employees

🎉 ALL TESTS PASSED!
```

## 🧠 Step 2: Understanding the AI Assistant Architecture

### 2.1 Core Components

The assistant is built using several key components:

#### **LangChain Agent Framework**
```python
# Core framework imports
from datetime import datetime
from typing import Any, List, Optional
from langchain.agents import AgentExecutor
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
from langchain_core.tools import BaseTool
```

#### **Custom Tools**
- **SnowflakeQueryTool**: Executes SQL queries against Snowflake
- **SchemaInspectionTool**: Explores database structure and metadata
- **FileProcessingTool**: Handles file uploads and content extraction

#### **Memory Management**
- Conversation buffer with configurable window size
- Persistent context across interactions
- Export/import conversation history

### 2.2 Assistant Initialization

```python
# Initialize the assistant
from snowflake_ai_assistant import SnowflakeAIAssistant

# Using Azure OpenAI
assistant = SnowflakeAIAssistant(use_azure=True)

# Using OpenAI Direct API
assistant = SnowflakeAIAssistant(use_azure=False)
```

## 💬 Step 3: Basic Interaction Patterns

### 3.1 Simple Chat Interface

```python
# Basic conversation
response = assistant.chat("Show me all tables in the database")
print(response)

# Follow-up question
response = assistant.chat("What columns are in the employees table?")
print(response)
```

### 3.2 Database Exploration

```python
# Natural language database queries
questions = [
    "What tables do we have available?",
    "Show me the structure of the sales_data table",
    "Give me the first 5 rows from the customer table",
    "What's the total revenue by region this month?",
    "Are there any data quality issues in our main tables?"
]

for question in questions:
    print(f"Q: {question}")
    print(f"A: {assistant.chat(question)}")
    print("-" * 50)
```

### 3.3 File Processing

```python
# Process uploaded files
file_path = "/path/to/your/data.csv"
response = assistant.chat(f"Analyze the data in this file: {file_path}")
print(response)
```

## 🔧 Step 4: Advanced Features

### 4.1 Custom Business Guidelines

The assistant follows business guidelines defined in `business_guidelines.md`. You can customize these:

```python
# Load custom guidelines
guidelines_path = "./custom_business_rules.md"
assistant = SnowflakeAIAssistant(use_azure=True)
# Guidelines are automatically loaded from BUSINESS_GUIDELINES_PATH environment variable
```

### 4.2 Memory Management

```python
# Check conversation history
history = assistant.get_conversation_history()
print(f"Conversation has {len(history)} messages")

# Clear memory when needed
assistant.clear_memory()

# Save conversation
assistant.save_conversation("./conversation_backup.json")
```

### 4.3 Error Handling and Validation

```python
try:
    response = assistant.chat("Complex business question here")
    print(response)
except Exception as e:
    print(f"Error: {e}")
    # Assistant handles graceful degradation
```

## 🌐 Step 5: Web Interface with Streamlit

### 5.1 Launch Web Application

```bash
# Start the Streamlit app
streamlit run streamlit_app.py
```

### 5.2 Web Interface Features

The Streamlit interface provides:

- **Interactive Chat**: Real-time conversation with the AI assistant
- **File Upload**: Drag-and-drop file processing
- **Conversation History**: View and export chat logs
- **Quick Actions**: Pre-built queries for common tasks
- **Configuration Display**: Current database and model settings

### 5.3 Customizing the Web Interface

Key sections you can modify in `streamlit_app.py`:

```python
# Add custom quick actions
quick_actions = [
    {"title": "Sales Analysis", "query": "Analyze sales trends"},
    {"title": "Customer Insights", "query": "Show customer behavior patterns"},
    {"title": "Performance Review", "query": "Review system performance metrics"}
]
```

## 🏢 Step 6: Enterprise Features

### 6.1 Security and Compliance

```python
# The assistant automatically follows security guidelines:
# - Validates user permissions before data access
# - Logs all database operations
# - Masks sensitive information in responses
# - Follows GDPR and data protection principles

# Custom security checks
def custom_security_check(query):
    sensitive_terms = ['password', 'ssn', 'credit_card']
    if any(term in query.lower() for term in sensitive_terms):
        return False
    return True
```

### 6.2 Performance Optimization

```python
# Assistant provides automatic optimization suggestions:
response = assistant.chat("""
Analyze this query for performance optimization:
SELECT * FROM large_table 
WHERE date_column > '2024-01-01'
ORDER BY amount DESC
""")
```

### 6.3 Data Quality Monitoring

```python
# Built-in data quality checks
quality_check = assistant.chat("""
Perform a comprehensive data quality assessment:
1. Check for null values in key columns
2. Identify duplicate records
3. Validate data type consistency
4. Look for outliers in numerical columns
""")
```

## 🎮 Step 7: Interactive Examples

### 7.1 Business Intelligence Scenarios

```python
# Sales analysis
response = assistant.chat("""
I need a comprehensive sales analysis including:
1. Total sales by month for the last year
2. Top 10 products by revenue
3. Regional performance comparison
4. Growth trends and insights
""")

# Customer analytics
response = assistant.chat("""
Help me understand our customer base:
1. Customer segmentation by purchase behavior
2. Retention rates by customer segment
3. Lifetime value analysis
4. Recommendations for customer engagement
""")
```

### 7.2 Operational Analytics

```python
# Performance monitoring
response = assistant.chat("""
Monitor our system performance:
1. Query execution times over the last week
2. Warehouse utilization patterns
3. Credit consumption analysis
4. Optimization recommendations
""")

# Data pipeline health
response = assistant.chat("""
Check the health of our data pipelines:
1. Recent data load status
2. Data freshness indicators
3. Error rates and patterns
4. Suggested improvements
""")
```

### 7.3 Predictive Analytics

```python
# Trend analysis
response = assistant.chat("""
Based on historical data, help me understand:
1. Seasonal patterns in our business
2. Forecasting models for next quarter
3. Key factors driving business growth
4. Risk indicators to monitor
""")
```

## 📱 Step 8: CLI and Automation

### 8.1 Command Line Interface

```bash
# Interactive CLI session
python snowflake_ai_assistant.py

# Demo with predefined scenarios
python demo_cli.py
```

### 8.2 Batch Processing

```python
# Process multiple files or queries in batch
batch_queries = [
    "Show me sales summary for last month",
    "Check data quality in customer table", 
    "Identify top performing products",
    "Generate executive dashboard metrics"
]

for query in batch_queries:
    result = assistant.chat(query)
    print(f"Query: {query}")
    print(f"Result: {result}")
    print("-" * 80)
```

### 8.3 Integration with Existing Workflows

```python
# Example: Integration with reporting pipeline
def generate_weekly_report():
    assistant = SnowflakeAIAssistant(use_azure=True)
    
    report_sections = [
        "Weekly sales performance summary",
        "Customer acquisition metrics",
        "Product performance analysis",
        "Operational efficiency indicators"
    ]
    
    full_report = ""
    for section in report_sections:
        response = assistant.chat(f"Generate {section}")
        full_report += f"## {section}\n{response}\n\n"
    
    return full_report
```

## 🔍 Step 9: Monitoring and Debugging

### 9.1 Enable Detailed Logging

```python
# Set environment variable for detailed logging
import os
os.environ['LANGCHAIN_TRACING_V2'] = 'true'

# The assistant will log all interactions and tool usage
```

### 9.2 Performance Monitoring

```python
# Monitor response times and token usage
import time

start_time = time.time()
response = assistant.chat("Complex analysis query")
end_time = time.time()

print(f"Response time: {end_time - start_time:.2f} seconds")
print(f"Response length: {len(response)} characters")
```

### 9.3 Error Analysis

```python
# Common troubleshooting scenarios
troubleshooting_guide = {
    "Connection Error": "Check Snowflake credentials and network access",
    "OpenAI API Error": "Verify API key and check usage limits",
    "Memory Error": "Clear conversation history or reduce memory window",
    "File Processing Error": "Check file format and permissions"
}
```

### 9.4 VS Code Debugging Setup

The workspace includes pre-configured debugging configurations for VS Code. These configurations automatically handle path resolution and environment loading.

#### **Available Debug Configurations:**

1. **🚀 Lab 07 FastAPI Server (Main)** - Runs the full-featured API server with AI assistant
2. **🎯 Lab 07 Simple API Server (Testing)** - Runs the simple test API server  
3. **🔧 Lab 07 Uvicorn Debug Mode** - Runs with uvicorn module and auto-reload
4. **🧪 Lab 07 Connection Test** - Tests Snowflake connection configuration
5. **👥 Lab 07 Employee Data Test** - Tests connection with actual data queries
6. **🔍 Environment Diagnostic** - Comprehensive environment and configuration diagnostic

#### **How to Debug:**

1. **Set up .env file** (if not already done):
   ```powershell
   # Copy example file
   cp .env.example .env
   # Edit with your actual values
   ```

2. **Open VS Code** in the learn-snowflake workspace

3. **Go to Debug view** (Ctrl+Shift+D or click Debug icon)

4. **Select configuration** from dropdown (e.g., "🚀 Lab 07 FastAPI Server (Main)")

5. **Start debugging** (F5 or click green play button)

#### **Troubleshooting Debug Issues:**

**🔍 Environment Variables Not Loading?**

First, run the diagnostic tool:
- Select "🔍 Environment Diagnostic" from debug configurations  
- This will show detailed environment status and recommendations

**🔧 Manual Verification:**

```powershell
# Check .env file exists
ls .env

# Test environment loading manually
python python/diagnose_env_loading.py
```

**🎯 Common Solutions:**

- Ensure `.env` file is in the `lab07` directory (not root workspace)
- Verify launch.json has `"cwd": "${workspaceFolder}/lab07"`
- Check that sensitive values are set (not placeholder values)

#### **Alternative: Direct Python Execution**

If debugging setup has issues, you can run directly:

```powershell
# Navigate to lab07 directory
cd lab07

# Activate virtual environment  
.\venv\Scripts\Activate.ps1

# Run the API server directly
python python/api_server.py

# Or run the simple API server
python python/simple_api_server.py

# Or with uvicorn
uvicorn python.api_server:app --reload --host 127.0.0.1 --port 8080
```

#### **Debugging Tips**

**🔧 Common Issues & Solutions:**

1. **Module Import Error**: `Could not import module`
   ```powershell
   # Ensure you're in the lab07 directory
   cd lab07
   
   # Check Python path
   python -c "import sys; print(sys.path)"
   
   # Use correct module path
   uvicorn python.api_server:app --reload
   ```

2. **Connection String Errors**:
   ```powershell
   # Test your connection
   python python/final_test.py
   
   # Test specific formats
   python python/test_connection_formats.py
   ```

3. **Environment Variables Not Loading**:
   ```powershell
   # Verify .env file location (should be in lab07/)
   Get-ChildItem .env
   
   # Test environment loading
   python python/diagnose_config.py
   ```

4. **Port Already in Use**:
   ```powershell
   # Find what's using port 8080
   netstat -ano | findstr :8080
   
   # Kill the process or use different port
   uvicorn python.api_server:app --reload --port 8001
   ```

#### **VS Code Debug Console Commands**

Once debugging is active, you can test in the debug console:

```python
# Test connection
from python.snowflake_connection import get_snowflake_connection
conn = get_snowflake_connection()

# Test employee query
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM employees")
print(cursor.fetchone())

# Test API endpoints
import requests
response = requests.get("http://localhost:8080/health")
print(response.json())
```

#### **Breakpoint Debugging**

Set breakpoints in these key files for debugging:
- `python/api_server.py` - API endpoint handlers
- `python/snowflake_connection.py` - Connection logic
- `python/snowflake_ai_assistant.py` - AI assistant logic

**Recommended breakpoint locations:**
```python
# In api_server.py
@app.get("/employees")  # Breakpoint here
async def get_employees():
    
# In snowflake_connection.py
def get_connection(self):  # Breakpoint here
    
# In snowflake_ai_assistant.py
def _run(self, query: str):  # Breakpoint here
```

## 🚀 Quick Start Guide

### Running the CLI Assistant

Once your environment is set up, you can quickly test the assistant:

```bash
# Activate virtual environment (if not already active)
.\venv\Scripts\Activate.ps1  # Windows PowerShell
# source venv/bin/activate    # macOS/Linux

# Run the CLI demo
python python/demo_cli.py

# Or run interactive CLI
python python/interactive_cli.py
```

### Running the Web Interface

For a user-friendly web interface using Streamlit:

```bash
# Activate virtual environment (if not already active)
.\venv\Scripts\Activate.ps1  # Windows PowerShell
# source venv/bin/activate    # macOS/Linux

# Launch Streamlit app
streamlit run python/streamlit_app.py

# Open browser to http://localhost:8501
```

### Common Commands

```bash
# Check installed packages
pip list

# Run specific examples
python python/basic_examples.py
python python/advanced_features.py

# Deactivate virtual environment when done
deactivate
```

## ✅ Lab Completion Checklist

- [ ] Set up Python environment with LangChain dependencies
- [ ] Configured Azure OpenAI or OpenAI API credentials
- [ ] Created secure connection to Snowflake using assistant
- [ ] Tested natural language to SQL conversion
- [ ] Implemented file upload and processing capabilities
- [ ] Configured conversation memory and context management
- [ ] Applied business guidelines and governance rules
- [ ] Built and tested CLI interface
- [ ] Deployed Streamlit web application
- [ ] Validated security and compliance features
- [ ] Created custom business intelligence queries
- [ ] Tested error handling and edge cases

## 🎉 Congratulations!

You've successfully built an advanced AI assistant that bridges natural language processing with Snowflake data operations! Key achievements:

- **Intelligent Data Access**: Natural language queries converted to optimized SQL
- **Context Awareness**: Conversation memory maintains business context
- **Multi-Modal Processing**: Handle text, files, and complex data analysis requests
- **Enterprise Ready**: Security, compliance, and governance built-in
- **Scalable Architecture**: Support for both individual and team usage
## Output
![lab07 debug](lab07-debug.png)
![lab07 api docs](lab07-api-docs.png)
![snowflake app data query](lab07-snowflake-app-data.png)
## 🔜 Next Steps

Continue with [Lab 08: Real-World Data Engineering Project](../lab08/) to apply your AI assistant in:
- Complex data pipeline automation
- Real-time business intelligence
- Advanced analytics and reporting
- Production deployment scenarios

## 🆘 Troubleshooting

### Common Issues:

**Issue**: Assistant initialization fails
**Solution**: 
- Verify environment variables in `.env` file
- Check Snowflake connection parameters
- Validate OpenAI API credentials and quotas

**Issue**: Slow response times
**Solution**:
- Optimize SQL queries generated by assistant
- Reduce conversation memory window size
- Use appropriate Snowflake warehouse size

**Issue**: File processing errors
**Solution**:
- Check supported file formats (txt, csv, xlsx, pdf)
- Ensure files are not corrupted or password-protected
- Verify sufficient disk space for temporary files

**Issue**: Memory or context errors
**Solution**:
- Clear conversation history periodically
- Reduce MAX_CONVERSATION_MEMORY setting
- Restart assistant session if needed

## � Quick Reference Commands

### **Start API Server (Recommended Methods)**

```powershell
# Method 1: Direct Python execution (simplest)
cd lab07
.\venv\Scripts\Activate.ps1
python python/api_server.py

# Method 2: Simple API server (for testing)
python python/simple_api_server.py

# Method 3: Uvicorn with correct module path
uvicorn python.api_server:app --reload --host 127.0.0.1 --port 8080
```

### **Test Your Setup**

```powershell
# Test connection configuration
python python/final_test.py

# Test all connection formats
python python/test_connection_formats.py

# Test API endpoints
curl http://localhost:8080/health
curl http://localhost:8080/employees
```

### **Common VS Code Debug Commands**

```bash
# If you see "Could not import module" error:
cd lab07  # Make sure you're in the correct directory
uvicorn python.api_server:app --reload  # Use correct module path

# If port is busy:
uvicorn python.api_server:app --reload --port 8001  # Use different port

# If environment variables not loading:
python python/diagnose_config.py  # Diagnose configuration
```

### **Endpoints to Test**

Once the server is running, test these endpoints:

| Endpoint | URL | Description |
|----------|-----|-------------|
| Health Check | `http://localhost:8080/health` | Server status |
| Employees | `http://localhost:8080/employees` | 👥 **Main endpoint** - Employee list |
| Departments | `http://localhost:8080/departments` | Department statistics |
| API Docs | `http://localhost:8080/docs` | Interactive API documentation |

## �📚 Additional Resources

- [LangChain Documentation](https://docs.langchain.com/)
- [OpenAI API Guide](https://platform.openai.com/docs)
- [Azure OpenAI Service](https://docs.microsoft.com/en-us/azure/cognitive-services/openai/)
- [Snowflake Python Connector](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [Streamlit Documentation](https://docs.streamlit.io/)

## 🔐 Security Best Practices

1. **API Key Management**: Store credentials securely, never commit to version control
2. **Data Access Control**: Implement proper role-based access controls
3. **Query Validation**: Validate all AI-generated queries before execution
4. **Audit Logging**: Log all database access and AI interactions
5. **Error Handling**: Implement graceful degradation for failures
6. **Content Filtering**: Screen inputs and outputs for sensitive information
