#!/usr/bin/env python3
"""
Enhanced Snowflake AI Assistant for Lab 07c
Advanced LangGraph implementation with multi-database thread management
"""

import os
import asyncio
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, TypedDict
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../.env')

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_openai import AzureChatOpenAI
from langchain.tools import BaseTool
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver

# Import our custom modules
from thread_config import get_thread_config, ThreadManagerConfig
from state_persistence import get_state_manager
from azure_auth import get_login_tool, LoginTool
import snowflake_connection

class ThreadSelection:
    """Handle thread selection and management for conversations"""
    
    def __init__(self):
        self.state_manager = get_state_manager()
        self.config = get_thread_config()
        self.login_tool = get_login_tool()
    
    async def list_available_threads(self, user_id: str = "") -> List[Dict[str, Any]]:
        """Get list of available threads for selection"""
        try:
            threads = await self.state_manager.get_threads(user_id, 50)  # Get recent 50 threads
            
            return [
                {
                    "thread_id": t.thread_id,
                    "title": t.title or f"Thread {t.thread_id[:8]}...",
                    "last_updated": t.last_updated.strftime("%Y-%m-%d %H:%M"),
                    "message_count": t.message_count,
                    "summary": t.summary[:100] + "..." if len(t.summary) > 100 else t.summary,
                    "tags": t.tags,
                    "user_id": t.user_id
                }
                for t in threads
            ]
        except Exception as e:
            print(f"Error listing threads: {e}")
            return []
    
    def display_thread_options(self, threads: List[Dict[str, Any]]) -> str:
        """Format thread options for display"""
        if not threads:
            return "No existing threads found."
        
        output = "\n🧵 Available Conversation Threads:\n" + "="*50 + "\n"
        
        for i, thread in enumerate(threads, 1):
            output += f"\n{i}. {thread['title']}\n"
            output += f"   ID: {thread['thread_id']}\n"
            output += f"   Last Updated: {thread['last_updated']}\n"
            output += f"   Messages: {thread['message_count']}\n"
            if thread['summary']:
                output += f"   Summary: {thread['summary']}\n"
            if thread['tags']:
                output += f"   Tags: {', '.join(thread['tags'])}\n"
        
        output += f"\n{len(threads) + 1}. Create New Thread\n"
        output += f"\nEnter your choice (1-{len(threads) + 1}): "
        
        return output
    
    async def select_thread_interactive(self, user_id: str = "") -> Optional[str]:
        """Interactive thread selection"""
        if not self.config.should_persist_threads():
            print("💡 Thread persistence is disabled. Starting with new session.")
            return None
        
        print("\n🔄 Thread Management Enabled")
        print(f"Database: {self.config.config.database_type.value}")
        
        # Check authentication requirement
        if self.config.config.require_authentication and not user_id:
            print("⚠️  Authentication required to access thread history.")
            print("You can continue without authentication, but won't have access to saved conversations.")
            return None
        
        threads = await self.list_available_threads(user_id)
        
        if not threads:
            if user_id:
                print(f"No existing threads found for user {user_id}. Starting with new thread.")
            else:
                print("No existing threads found. Starting with new thread.")
            return self.state_manager.generate_thread_id("interactive", user_id)
        
        print(self.display_thread_options(threads))
        
        try:
            choice = input().strip()
            choice_num = int(choice)
            
            if 1 <= choice_num <= len(threads):
                selected_thread = threads[choice_num - 1]
                print(f"\n✅ Selected: {selected_thread['title']}")
                return selected_thread['thread_id']
            elif choice_num == len(threads) + 1:
                new_thread_id = self.state_manager.generate_thread_id("interactive", user_id)
                print(f"\n✨ Created new thread: {new_thread_id}")
                return new_thread_id
            else:
                print("Invalid choice. Creating new thread.")
                return self.state_manager.generate_thread_id("interactive", user_id)
                
        except (ValueError, KeyboardInterrupt):
            print("Invalid input. Creating new thread.")
            return self.state_manager.generate_thread_id("interactive", user_id)

class CurrencyConverterTool(BaseTool):
    """Enhanced currency converter tool with thread-aware logging"""
    name: str = "currency_converter"
    description: str = "Convert USD prices to EUR using current exchange rates. Input should be a number (USD amount)."
    
    def _run(self, usd_amount: float) -> str:
        """Convert USD to EUR"""
        try:
            import requests
            
            # Get current exchange rate
            response = requests.get("https://api.exchangerate-api.com/v4/latest/USD")
            data = response.json()
            
            eur_rate = data['rates']['EUR']
            eur_amount = round(usd_amount * eur_rate, 2)
            
            result = f"💱 Currency Conversion:\n"
            result += f"   ${usd_amount} USD = €{eur_amount} EUR\n"
            result += f"   Exchange Rate: {eur_rate}\n"
            result += f"   Date: {data.get('date', 'Unknown')}"
            
            return result
            
        except Exception as e:
            return f"❌ Currency conversion failed: {str(e)}"

class EnhancedSnowflakeAssistant:
    """Enhanced Snowflake AI Assistant with advanced thread management"""
    
    def __init__(self, user_id: str = ""):
        self.user_id = user_id
        self.state_manager = get_state_manager()
        self.config = get_thread_config()
        self.thread_selector = ThreadSelection()
        self.current_thread_id = None
        self.graph = None
        
        # Initialize tools
        self.tools = [
            CurrencyConverterTool(),
            # Add other tools here as needed
        ]
        
        # Setup LLM - Azure OpenAI configuration
        self.llm = AzureChatOpenAI(
            temperature=0.1,
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
            streaming=True
        )
        
        # Enhanced system prompt
        self.system_prompt = """You are an advanced Snowflake AI Assistant with persistent memory capabilities.

CORE CAPABILITIES:
- Advanced SQL query generation and optimization for Snowflake
- Data analysis and insights from Snowflake tables
- Currency conversion and financial calculations
- Thread-based conversation memory and context retention
- Multi-session conversation continuity

THREAD MANAGEMENT:
- You maintain conversation history across sessions using thread IDs
- Each conversation thread preserves context, preferences, and ongoing work
- You can reference previous conversations and build upon past interactions

SNOWFLAKE EXPERTISE:
- Generate efficient SQL queries for complex data operations
- Provide insights on data patterns, trends, and anomalies
- Suggest performance optimizations and best practices
- Handle window functions, CTEs, and advanced Snowflake features

CONVERSATION STYLE:
- Be helpful, accurate, and context-aware
- Reference previous conversations when relevant
- Maintain professional yet friendly tone
- Provide detailed explanations when requested

When users ask questions, leverage your tools and maintain conversation context through the thread system."""

    async def initialize_session(self) -> str:
        """Initialize a new session with thread selection"""
        print("\n🚀 Lab 07c: Enhanced Snowflake AI Assistant")
        print("🔧 Features: Advanced LangGraph + Multi-Database Thread Management")
        print("-" * 60)
        
        # Thread selection with user context
        self.current_thread_id = await self.thread_selector.select_thread_interactive(self.user_id)
        
        if not self.current_thread_id:
            # Fallback for no persistence
            self.current_thread_id = "temp-session"
        
        # Create LangGraph agent with checkpointing
        checkpointer = self.state_manager.get_checkpointer()
        
        self.graph = create_react_agent(
            model=self.llm,
            tools=self.tools,
            checkpointer=checkpointer
        )
        
        # Load conversation history if available
        await self._load_conversation_history()
        
        return self.current_thread_id
    
    async def _load_conversation_history(self):
        """Load and display conversation history"""
        if not self.config.should_persist_threads():
            return
            
        try:
            summary = await self.state_manager.get_thread_summary(self.current_thread_id)
            if summary and summary['message_count'] > 0:
                print(f"\n📜 Conversation History Loaded:")
                print(f"   Thread: {summary['title']}")
                print(f"   Messages: {summary['message_count']}")
                print(f"   Last Updated: {summary['last_updated']}")
                if summary['summary']:
                    print(f"   Summary: {summary['summary']}")
                print()
            else:
                print(f"\n✨ Starting fresh conversation in thread: {self.current_thread_id[:16]}...\n")
                
        except Exception as e:
            print(f"Note: Could not load conversation history: {e}")
    
    async def chat(self, message: str) -> str:
        """Process a chat message with thread-aware context"""
        if not self.graph:
            raise RuntimeError("Session not initialized. Call initialize_session() first.")
        
        try:
            # Create config for this thread
            config = {
                "configurable": {
                    "thread_id": self.current_thread_id
                }
            }
            
            # Process message through LangGraph
            response = self.graph.invoke(
                {"messages": [HumanMessage(content=message)]},
                config=config
            )
            
            # Extract the assistant's response
            if response and "messages" in response:
                last_message = response["messages"][-1]
                if hasattr(last_message, 'content'):
                    return last_message.content
                else:
                    return str(last_message)
            
            return "I apologize, but I couldn't process your request properly."
            
        except Exception as e:
            return f"❌ Error processing message: {str(e)}"
    
    async def get_conversation_stats(self) -> Dict[str, Any]:
        """Get statistics about the current conversation"""
        try:
            summary = await self.state_manager.get_thread_summary(self.current_thread_id)
            
            stats = {
                "thread_id": self.current_thread_id,
                "persistence_enabled": self.config.should_persist_threads(),
                "database_type": self.config.config.database_type.value,
                "message_count": summary.get('message_count', 0) if summary else 0,
                "created_at": summary.get('created_at') if summary else None,
                "last_updated": summary.get('last_updated') if summary else None
            }
            
            return stats
            
        except Exception as e:
            return {"error": f"Could not retrieve stats: {e}"}
    
    async def list_all_threads(self) -> List[Dict[str, Any]]:
        """List all available threads for the current user"""
        return await self.thread_selector.list_available_threads(self.user_id)
    
    async def switch_thread(self, thread_id: str) -> bool:
        """Switch to a different thread (user must own the thread)"""
        try:
            # Verify thread exists and user has access
            summary = await self.state_manager.get_thread_summary(thread_id, self.user_id)
            if not summary:
                return False
            
            self.current_thread_id = thread_id
            await self._load_conversation_history()
            return True
            
        except Exception as e:
            print(f"Error switching thread: {e}")
            return False

async def main():
    """Main entry point with integrated authentication and thread management"""
    
    # Initialize thread configuration
    thread_config = ThreadManagerConfig()
    config = thread_config.config
    
    user_info = None
    user_id = ""
    
    # Handle authentication if required
    if config.require_authentication:
        print("🔐 Authentication Required")
        login_tool = get_login_tool()
        
        # Check if already authenticated
        user_info = login_tool.auth.get_current_user()
        if not user_info:
            print("Please log in to access your conversation threads.")
            print("Choose login method:")
            print("1. Interactive Browser Login")
            print("2. Device Code Flow") 
            print("3. Integrated Windows Authentication")
            print("4. Continue without authentication (no thread access)")
            
            try:
                choice = input("Select option (1-4): ").strip()
                
                if choice == "1":
                    success, msg = login_tool.auth.login_interactive()
                    if success:
                        user_info = login_tool.auth.get_current_user()
                elif choice == "2":
                    success, msg = login_tool.auth.login_device_code()
                    if success:
                        user_info = login_tool.auth.get_current_user()
                elif choice == "3":
                    success, msg = login_tool.auth.login_integrated()
                    if success:
                        user_info = login_tool.auth.get_current_user()
                elif choice == "4":
                    print("Continuing without authentication...")
                else:
                    print("Invalid choice. Continuing without authentication...")
                    
            except KeyboardInterrupt:
                print("\nLogin cancelled. Continuing without authentication...")
        
        if user_info:
            user_id = user_info.user_id
            print(f"✅ Authenticated as: {user_info.display_name} ({user_info.email})")
        else:
            print("⚠️  Not authenticated. You won't have access to saved conversation threads.")
    
    # Initialize assistant with authentication context
    assistant = EnhancedSnowflakeAssistant(user_id=user_id)
    
    try:
        # Initialize session with thread selection (includes authentication context)
        thread_id = await assistant.initialize_session()
        
        print("💬 Chat started! Type 'quit', 'exit', or 'bye' to end.")
        print("📊 Type 'stats' to see conversation statistics.")
        print("🧵 Type 'threads' to list all available threads.")
        print("🔄 Type 'switch <thread_id>' to switch to another thread.")
        if config.require_authentication and not user_id:
            print("🔐 Type 'login' to authenticate and access your saved threads.")
        print("-" * 60)
        
        while True:
            try:
                user_input = input("\n🧑‍💻 You: ").strip()
                
                if user_input.lower() in ['quit', 'exit', 'bye']:
                    print("\n👋 Goodbye! Your conversation has been saved.")
                    break
                elif user_input.lower() == 'login' and config.require_authentication:
                    if user_id:
                        print("✅ Already authenticated!")
                        continue
                    
                    login_tool = get_login_tool()
                    print("Choose login method:")
                    print("1. Interactive Browser Login")
                    print("2. Device Code Flow") 
                    print("3. Integrated Windows Authentication")
                    
                    try:
                        choice = input("Select option (1-3): ").strip()
                        
                        if choice == "1":
                            success, msg = login_tool.auth.login_interactive()
                            if success:
                                user_info = login_tool.auth.get_current_user()
                        elif choice == "2":
                            success, msg = login_tool.auth.login_device_code()
                            if success:
                                user_info = login_tool.auth.get_current_user()
                        elif choice == "3":
                            success, msg = login_tool.auth.login_integrated()
                            if success:
                                user_info = login_tool.auth.get_current_user()
                        else:
                            print("Invalid choice.")
                            continue
                            
                        if user_info:
                            user_id = user_info.user_id
                            assistant.user_id = user_id  # Update assistant's user context
                            print(f"✅ Authenticated as: {user_info.display_name} ({user_info.email})")
                            print("You can now access your saved conversation threads with 'threads' command.")
                        else:
                            print("❌ Authentication failed.")
                            
                    except KeyboardInterrupt:
                        print("\nLogin cancelled.")
                    continue
                    
                elif user_input.lower() == 'stats':
                    stats = await assistant.get_conversation_stats()
                    print("\n📊 Conversation Statistics:")
                    for key, value in stats.items():
                        print(f"   {key}: {value}")
                    continue
                elif user_input.lower() == 'threads':
                    if config.require_authentication and not user_id:
                        print("🔐 Please log in first to view your threads. Type 'login' to authenticate.")
                        continue
                        
                    threads = await assistant.list_all_threads()
                    if threads:
                        print("\n🧵 Available Threads:")
                        for thread in threads[:10]:  # Show first 10
                            print(f"   - {thread['title']} ({thread['thread_id'][:16]}...)")
                    else:
                        print("\n🧵 No threads found.")
                    continue
                elif user_input.lower().startswith('switch '):
                    if config.require_authentication and not user_id:
                        print("🔐 Please log in first to switch threads. Type 'login' to authenticate.")
                        continue
                        
                    target_thread = user_input[7:].strip()
                    if await assistant.switch_thread(target_thread):
                        print(f"✅ Switched to thread: {target_thread[:16]}...")
                    else:
                        print(f"❌ Could not switch to thread: {target_thread}")
                    continue
                elif not user_input:
                    continue
                
                # Process normal chat message
                print("🤔 Assistant: ", end="", flush=True)
                response = await assistant.chat(user_input)
                print(response)
                
            except KeyboardInterrupt:
                print("\n\n👋 Conversation interrupted. Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}")
                
    except Exception as e:
        print(f"❌ Failed to start assistant: {e}")

if __name__ == "__main__":
    asyncio.run(main())