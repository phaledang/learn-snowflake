# Core framework imports
from datetime import datetime
from typing import Any, List, Optional
from langchain.agents import AgentExecutor
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
from langchain_core.tools import BaseTool

import os
import sys
import json
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from langchain.memory import ConversationBufferWindowMemory

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_snowflake_connection
from langchain.schema import SystemMessage

try:
    from pydantic import BaseModel, Field
except ImportError:
    from langchain_core.pydantic_v1 import BaseModel, Field

# Load environment variables
load_dotenv()


class SnowflakeQueryTool(BaseTool):
    """Tool for executing SQL queries against Snowflake database."""
    
    name: str = "snowflake_query"
    description: str = """Execute SQL queries against the Snowflake database. 
    Use this tool to retrieve data, analyze information, or perform database operations.
    Input should be a valid SQL query string."""
    
    def _run(self, query: str) -> str:
        """Execute the SQL query and return results."""
        try:
            conn = get_snowflake_connection()
            df = pd.read_sql(query, conn)
            
            if df.empty:
                return "Query executed successfully but returned no results."
            
            # Format results for display
            if len(df) > 10:
                result = f"Query returned {len(df)} rows. First 10 rows:\n"
                result += df.head(10).to_string(index=False)
                result += f"\n\n... and {len(df) - 10} more rows."
            else:
                result = f"Query returned {len(df)} rows:\n"
                result += df.to_string(index=False)
            
            return result
            
        except Exception as e:
            return f"Error executing query: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version of the run method."""
        return self._run(query)


class SchemaInspectionTool(BaseTool):
    """Tool for inspecting database schema and table structures."""
    
    name: str = "schema_inspection"
    description: str = """Inspect database schema, table structures, and column information.
    Use this to understand available tables and their columns before writing queries.
    Input should be 'tables' to list all tables, or a table name to get column details."""
    
    def _run(self, input_str: str) -> str:
        """Inspect schema or table structure."""
        try:
            conn = get_snowflake_connection()
            
            if input_str.lower() == 'tables':
                # List all tables in current schema
                query = """
                SELECT table_name, table_type, row_count, comment
                FROM information_schema.tables 
                WHERE table_schema = CURRENT_SCHEMA()
                ORDER BY table_name
                """
                df = pd.read_sql(query, conn)
                if df.empty:
                    return "No tables found in the current schema."
                return f"Available tables:\n{df.to_string(index=False)}"
            
            else:
                # Get column information for specific table
                table_name = input_str.upper()
                query = f"""
                SELECT column_name, data_type, is_nullable, comment
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND table_schema = CURRENT_SCHEMA()
                ORDER BY ordinal_position
                """
                df = pd.read_sql(query, conn)
                if df.empty:
                    return f"Table '{table_name}' not found or no columns information available."
                return f"Columns for table {table_name}:\n{df.to_string(index=False)}"
                
        except Exception as e:
            return f"Error inspecting schema: {str(e)}"
    
    async def _arun(self, input_str: str) -> str:
        """Async version of the run method."""
        return self._run(input_str)


class FileProcessingTool(BaseTool):
    """Tool for processing uploaded files and extracting content."""
    
    name: str = "file_processor"
    description: str = """Process uploaded files (PDF, Word, Excel, text) and extract content.
    Input should be the file path. Returns extracted text content for analysis."""
    
    def _run(self, file_path: str) -> str:
        """Process file and extract content."""
        try:
            if not os.path.exists(file_path):
                return f"File not found: {file_path}"
            
            file_extension = os.path.splitext(file_path)[1].lower()
            
            if file_extension == '.txt':
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return f"Text file content:\n{content[:2000]}{'...' if len(content) > 2000 else ''}"
            
            elif file_extension in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
                return f"Excel file summary:\n{df.head().to_string()}\n\nTotal rows: {len(df)}"
            
            elif file_extension == '.csv':
                df = pd.read_csv(file_path)
                return f"CSV file summary:\n{df.head().to_string()}\n\nTotal rows: {len(df)}"
            
            else:
                return f"Unsupported file type: {file_extension}"
                
        except Exception as e:
            return f"Error processing file: {str(e)}"
    
    async def _arun(self, file_path: str) -> str:
        """Async version of the run method."""
        return self._run(file_path)


class CurrencyConverterTool(BaseTool):
    """Tool for converting currency amounts from USD to EUR using live exchange rates."""
    
    name: str = "currency_converter"
    description: str = """Convert currency amounts from USD to EUR using live exchange rates.
    Input should be a number representing the USD amount to convert.
    Returns the equivalent amount in EUR with current exchange rate."""
    
    def _run(self, usd_amount: str) -> str:
        """Convert USD amount to EUR using exchange rate API."""
        try:
            import requests
            
            # Parse the USD amount
            try:
                amount = float(str(usd_amount).replace('$', '').replace(',', '').strip())
            except ValueError:
                return f"Invalid amount format: {usd_amount}. Please provide a numeric value."
            
            if amount < 0:
                return "Amount must be positive."
            
            # Use a free exchange rate API (Exchange Rates API)
            # Alternative: https://api.exchangerate-api.com/v4/latest/USD
            api_url = "https://api.exchangerate-api.com/v4/latest/USD"
            
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Get EUR exchange rate
            if 'rates' not in data or 'EUR' not in data['rates']:
                return "EUR exchange rate not available in API response."
            
            eur_rate = data['rates']['EUR']
            eur_amount = amount * eur_rate
            
            # Format the response
            result = f"💰 Currency Conversion:\n"
            result += f"USD ${amount:,.2f} = EUR €{eur_amount:,.2f}\n"
            result += f"Exchange Rate: 1 USD = {eur_rate:.4f} EUR\n"
            result += f"Last Updated: {data.get('date', 'N/A')}"
            
            return result
            
        except requests.exceptions.RequestException as e:
            return f"Error fetching exchange rate: {str(e)}. Please check your internet connection."
        except KeyError as e:
            return f"Error parsing exchange rate data: Missing key {str(e)}"
        except Exception as e:
            return f"Error converting currency: {str(e)}"
    
    async def _arun(self, usd_amount: str) -> str:
        """Async version of the run method."""
        return self._run(usd_amount)


class SnowflakeAIAssistant:
    """Advanced LangChain OpenAI Assistant with Snowflake integration."""
    
    def __init__(self, use_azure: bool = True):
        """Initialize the assistant with Azure OpenAI or OpenAI API."""
        self.use_azure = use_azure
        self.assistant_name = os.getenv('ASSISTANT_NAME', 'SnowflakeAI')
        self.max_memory = int(os.getenv('MAX_CONVERSATION_MEMORY', '50'))
        
        # Initialize LLM
        self.llm = self._initialize_llm()
        
        # Initialize memory
        self.memory = ConversationBufferWindowMemory(
            k=self.max_memory,
            memory_key="chat_history",
            return_messages=True
        )
        
        # Initialize tools
        self.tools = [
            SnowflakeQueryTool(),
            SchemaInspectionTool(),
            FileProcessingTool(),
            CurrencyConverterTool()
        ]
        
        # Load business guidelines
        self.business_guidelines = self._load_business_guidelines()
        
        # Create system prompt
        self.system_prompt = self._create_system_prompt()
        
        # Initialize agent
        self.agent = self._create_agent()
    
    def _initialize_llm(self):
        """Initialize the language model (Azure OpenAI or OpenAI)."""
        if self.use_azure:
            return AzureChatOpenAI(
                azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
                api_key=os.getenv('AZURE_OPENAI_API_KEY'),
                api_version=os.getenv('AZURE_OPENAI_API_VERSION'),
                deployment_name=os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME'),
                temperature=0.1
            )
        else:
            return ChatOpenAI(
                api_key=os.getenv('OPENAI_API_KEY'),
                model=os.getenv('OPENAI_MODEL', 'gpt-4-turbo-preview'),
                temperature=0.1
            )
    
    def _load_business_guidelines(self) -> str:
        """Load business guidelines from file or return default."""
        guidelines_path = os.getenv('BUSINESS_GUIDELINES_PATH', './business_guidelines.md')
        try:
            if os.path.exists(guidelines_path):
                with open(guidelines_path, 'r', encoding='utf-8') as f:
                    return f.read()
        except Exception:
            pass
        
        return """
        # Business Guidelines for Snowflake AI Assistant
        
        ## Data Access Principles
        1. Always verify user permissions before accessing sensitive data
        2. Provide clear explanations of query results
        3. Suggest optimizations for expensive queries
        4. Follow data privacy and security best practices
        
        ## Query Best Practices
        1. Use LIMIT clauses for exploratory queries
        2. Explain query logic and results clearly
        3. Suggest indexes or optimizations when relevant
        4. Validate data quality in results
        
        ## Communication Style
        1. Be helpful and professional
        2. Explain technical concepts in business terms when needed
        3. Provide actionable insights from data analysis
        4. Always acknowledge limitations and assumptions
        """
    
    def _create_system_prompt(self) -> str:
        """Create the system prompt for the assistant."""
        return f"""You are {self.assistant_name}, an advanced AI assistant specialized in Snowflake database operations and data analysis.

You have access to the following capabilities:
1. Execute SQL queries against Snowflake databases
2. Inspect database schemas and table structures  
3. Process and analyze uploaded files
4. Convert currency from USD to EUR using live exchange rates

Business Guidelines:
{self.business_guidelines}

Key Instructions:
- Always prioritize data security and user privacy
- Provide clear, actionable insights from data analysis
- Explain your reasoning and methodology
- Suggest query optimizations when relevant
- If uncertain about data access permissions, ask for clarification
- Format results in a clear, business-friendly manner
- When users ask about prices in EUR, automatically convert USD prices using the currency converter tool
- Always show both USD and EUR amounts when doing currency conversions

Current database context:
- Database: {os.getenv('SNOWFLAKE_DATABASE', 'LEARN_SNOWFLAKE')}
- Schema: {os.getenv('SNOWFLAKE_SCHEMA', 'SANDBOX')}
- Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE', 'LEARN_WH')}

Remember to use the available tools to interact with the database and process files. Always explain what you're doing and why."""
    
    def _create_agent(self):
        """Create the LangChain agent with tools and memory."""
        try:
            from langchain.agents import create_openai_tools_agent
        except ImportError:
            from langchain.agents import create_openai_functions_agent as create_openai_tools_agent
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", self.system_prompt),
            ("placeholder", "{chat_history}"),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}")
        ])
        
        agent = create_openai_tools_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=prompt
        )
        
        return AgentExecutor(
            agent=agent,
            tools=self.tools,
            memory=self.memory,
            verbose=True,
            max_iterations=5,
            early_stopping_method="generate"
        )
    
    def chat(self, message: str) -> str:
        """Send a message to the assistant and get a response."""
        try:
            response = self.agent.invoke({"input": message})
            return response.get("output", "No response generated.")
        except Exception as e:
            return f"Error processing request: {str(e)}"
    
    def clear_memory(self):
        """Clear the conversation memory."""
        self.memory.clear()
        print("Conversation memory cleared.")
    
    def get_conversation_history(self) -> List[BaseMessage]:
        """Get the current conversation history."""
        return self.memory.chat_memory.messages
    
    def save_conversation(self, file_path: str):
        """Save conversation history to a file."""
        try:
            history = [
                {
                    "type": msg.__class__.__name__,
                    "content": msg.content,
                    "timestamp": datetime.now().isoformat()
                }
                for msg in self.get_conversation_history()
            ]
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=2, ensure_ascii=False)
            
            print(f"Conversation saved to {file_path}")
        except Exception as e:
            print(f"Error saving conversation: {str(e)}")


def main():
    """Main function to demonstrate the assistant."""
    print("🚀 Initializing Snowflake AI Assistant...")
    
    # Initialize assistant
    assistant = SnowflakeAIAssistant(use_azure=True)
    
    print(f"✅ {assistant.assistant_name} is ready!")
    print("Type 'quit' to exit, 'clear' to clear memory, 'history' to see conversation history")
    print("-" * 60)
    
    while True:
        try:
            user_input = input("\n🔹 You: ").strip()
            
            if user_input.lower() == 'quit':
                print("👋 Goodbye!")
                break
            elif user_input.lower() == 'clear':
                assistant.clear_memory()
                continue
            elif user_input.lower() == 'history':
                history = assistant.get_conversation_history()
                print(f"\n📝 Conversation History ({len(history)} messages):")
                for msg in history[-10:]:  # Show last 10 messages
                    role = "🔹 You" if isinstance(msg, HumanMessage) else "🤖 Assistant"
                    print(f"{role}: {msg.content[:100]}{'...' if len(msg.content) > 100 else ''}")
                continue
            elif not user_input:
                continue
            
            print("\n🤖 Assistant: ", end="")
            response = assistant.chat(user_input)
            print(response)
            
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")


if __name__ == "__main__":
    main()