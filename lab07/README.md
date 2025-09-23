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

### 1.1 Install Required Dependencies

```bash
# Navigate to lab07 directory
cd lab07/python

# Install dependencies
pip install -r requirements.txt
```

### 1.2 Environment Configuration

Create your `.env` file from the example:

```bash
# Copy example environment file
cp .env.example .env

# Edit with your actual credentials
nano .env
```

Required environment variables:

```env
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=LEARN_WH
SNOWFLAKE_DATABASE=LEARN_SNOWFLAKE
SNOWFLAKE_SCHEMA=SANDBOX

# Azure OpenAI (recommended)
AZURE_OPENAI_API_KEY=your_azure_openai_api_key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2023-12-01-preview
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4

# Alternative: Direct OpenAI API
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4-turbo-preview

# Optional Configuration
ASSISTANT_NAME=SnowflakeAI
MAX_CONVERSATION_MEMORY=50
```

### 1.3 Verify Configuration

```bash
# Test environment setup
python demo_cli.py
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

## 📚 Additional Resources

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