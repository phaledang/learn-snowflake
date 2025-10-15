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
            # Get threads from both the metadata backend and the checkpointer
            metadata_threads = await self.state_manager.get_threads(user_id, 50)
            
            # Also get threads from the checkpointer (where actual conversation data is stored)
            checkpointer_threads = []
            if hasattr(self.state_manager, 'checkpointer') and hasattr(self.state_manager.checkpointer, 'get_threads_with_counts'):
                checkpointer_threads = list(self.state_manager.checkpointer.get_threads_with_counts(50))
            
            # Combine and deduplicate threads
            all_threads = []
            seen_thread_ids = set()
            
            # Add metadata threads first
            for t in metadata_threads:
                if t.thread_id not in seen_thread_ids:
                    all_threads.append({
                        "thread_id": t.thread_id,
                        "title": t.title or f"Thread {t.thread_id[:8]}...",
                        "last_updated": t.last_updated.strftime("%Y-%m-%d %H:%M"),
                        "message_count": t.message_count,
                        "summary": t.summary[:100] + "..." if len(t.summary) > 100 else t.summary,
                        "tags": t.tags,
                        "user_id": t.user_id
                    })
                    seen_thread_ids.add(t.thread_id)
            
            # Add checkpointer threads that aren't in metadata
            for t in checkpointer_threads:
                if t["thread_id"] not in seen_thread_ids:
                    all_threads.append({
                        "thread_id": t["thread_id"],
                        "title": f"Thread {t['thread_id'][:20]}...",
                        "last_updated": t["last_updated"],
                        "message_count": t.get("message_count", 0),  # Now get actual count from database
                        "summary": "Conversation thread",
                        "tags": [],
                        "user_id": user_id
                    })
                    seen_thread_ids.add(t["thread_id"])
            
            return all_threads
        except Exception as e:
            print(f"Error listing threads: {e}")
            return []
    
    def display_thread_options(self, threads: List[Dict[str, Any]], show_summary: bool = True) -> str:
        """Format thread options for display with smart naming"""
        if not threads:
            return "No existing threads found."
        
        output = "\n🧵 Available Conversation Threads:\n" + "="*50 + "\n"
        
        for i, thread in enumerate(threads, 1):
            title = thread.get('title', f"Thread {i}")
            if not title or title.strip() == "":
                title = f"Thread {thread['thread_id'][:8]}"
            
            # Format last updated time nicely
            last_updated = thread.get('last_updated', '')
            if isinstance(last_updated, str):
                try:
                    import datetime as dt
                    parsed_dt = dt.datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    time_str = parsed_dt.strftime('%m/%d %H:%M')
                except:
                    time_str = last_updated
            else:
                time_str = str(last_updated)
            
            message_count = thread.get('message_count', 0)
            
            output += f"\n{i}. 💬 {title}\n"
            output += f"   📅 {time_str} | 💬 {message_count} messages\n"
            
            # Show summary if available and requested
            if show_summary and thread.get('summary'):
                summary = thread['summary']
                if len(summary) > 80:
                    summary = summary[:77] + "..."
                output += f"   📋 {summary}\n"
            
            # Show tags if available
            if thread.get('tags'):
                tags_str = ', '.join(thread['tags'])
                if len(tags_str) > 40:
                    tags_str = tags_str[:37] + "..."
                output += f"   🏷️  {tags_str}\n"
        
        output += f"\n{len(threads) + 1}. ✨ Create New Thread\n"
        output += f"\n💡 Enter your choice (1-{len(threads) + 1}) or type 'new' for new thread: "
        
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
            choice = input().strip().lower()
            
            # Handle 'new' keyword
            if choice in ['new', 'n']:
                new_thread_id = self.state_manager.generate_thread_id("interactive", user_id)
                print(f"\n✨ Created new thread: {new_thread_id[:8]}...")
                return new_thread_id
            
            # Handle numeric choice
            choice_num = int(choice)
            
            if 1 <= choice_num <= len(threads):
                selected_thread = threads[choice_num - 1]
                title = selected_thread.get('title', f"Thread {choice_num}")
                print(f"\n✅ Selected: {title}")
                return selected_thread['thread_id']
            elif choice_num == len(threads) + 1:
                new_thread_id = self.state_manager.generate_thread_id("interactive", user_id)
                print(f"\n✨ Created new thread: {new_thread_id[:8]}...")
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
        
        # Set user ID in state manager for thread isolation
        if user_id:
            self.state_manager.set_user_id(user_id)
        
        self.config = get_thread_config()
        self.thread_selector = ThreadSelection()
        
        # Also set user ID in thread selector's state manager
        if user_id:
            self.thread_selector.state_manager.set_user_id(user_id)
        
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
        print("🔐 Authentication Options")
        
        # Show immediate options without delay
        print("Choose an option:")
        print("1. Quick check existing authentication (fast)")
        print("2. Login with new account") 
        print("3. Continue without authentication")
        print("4. Skip auth check and login directly")
        
        try:
            choice = input("Select option (1-4): ").strip()
            
            if choice == "1":
                # Quick authentication check with timeout
                print("🔍 Quick authentication check...")
                try:
                    import signal
                    
                    def timeout_handler(signum, frame):
                        raise TimeoutError("Authentication check timed out")
                    
                    # Set 3-second timeout for Windows (if supported)
                    try:
                        signal.signal(signal.SIGALRM, timeout_handler)
                        signal.alarm(3)  # 3 second timeout
                    except AttributeError:
                        # Windows doesn't support SIGALRM, just proceed normally
                        pass
                    
                    login_tool = get_login_tool()
                    user_info = login_tool.auth.get_current_user()
                    
                    try:
                        signal.alarm(0)  # Cancel timeout
                    except AttributeError:
                        pass
                    
                    if user_info:
                        print(f"✅ Found existing authentication: {user_info.display_name} ({user_info.email})")
                    else:
                        print("ℹ️ No existing authentication found. Please choose a login method.")
                        choice = "2"  # Fall through to login options
                        
                except (TimeoutError, Exception) as e:
                    try:
                        signal.alarm(0)  # Cancel timeout
                    except AttributeError:
                        pass
                    print(f"⚠️ Authentication check taking too long or failed: {str(e)[:50]}...")
                    print("Please choose: (2) Login with new account, or (3) Continue without auth")
                    choice = input("Select option (2-3): ").strip()
                    if choice not in ["2", "3"]:
                        choice = "3"
                        
            elif choice == "3":
                print("Continuing without authentication...")
            elif choice == "4":
                print("Skipping authentication check, proceeding to login...")
                choice = "2"  # Fall through to login options
            # choice == "2" will fall through to login options
        except KeyboardInterrupt:
            print("\nContinuing without authentication...")
            choice = "3"
        
        if choice == "2" or choice == "4" or (choice == "1" and not user_info):
            # Create login tool only when needed for actual login
            if 'login_tool' not in locals():
                login_tool = get_login_tool()
                
            print("Please log in to access your conversation threads.")
            print("Choose login method:")
            print("1. API-based Login (Recommended for desktop) - MSAL optimized")
            print("2. Interactive Browser Login (fallback) - Legacy mode")
            print("3. Device Code Flow") 
            print("4. Integrated Windows Authentication")
            print("5. Continue without authentication (no thread access)")
            
            try:
                login_choice = input("Select option (1-5): ").strip()
                
                if login_choice == "1":
                    success, msg = login_tool.auth.login_interactive()
                    if success:
                        user_info = login_tool.auth.get_current_user()
                    else:
                        print(f"❌ API-based authentication failed: {msg}")
                        print("💡 This may be due to Azure AD app configuration.")
                        print("   Please see AZURE_AD_SETUP_GUIDE.md for configuration help.")
                elif login_choice == "2":
                    success, msg = login_tool.auth.login_interactive_fallback()
                    if success:
                        user_info = login_tool.auth.get_current_user()
                    else:
                        print(f"❌ Browser authentication failed: {msg}")
                elif login_choice == "3":
                    success, msg = login_tool.auth.login_device_code()
                    if success:
                        user_info = login_tool.auth.get_current_user()
                    else:
                        print(f"❌ Device code authentication failed: {msg}")
                        print("💡 Try checking Azure AD app registration configuration.")
                elif login_choice == "4":
                    success, msg = login_tool.auth.login_integrated()
                    if success:
                        user_info = login_tool.auth.get_current_user()
                    else:
                        print(f"❌ Integrated authentication failed: {msg}")
                elif login_choice == "5":
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
            if 'msg' in locals() and not success:
                print(f"⚠️  Error details: {msg}")
    
    # Initialize assistant with authentication context
    assistant = EnhancedSnowflakeAssistant(user_id=user_id)
    
    try:
        # Initialize session with thread selection (includes authentication context)
        thread_id = await assistant.initialize_session()
        
        print("💬 Chat started! Type 'quit', 'exit', or 'bye' to end.")
        print("📊 Type 'stats' to see conversation statistics.")
        print("🧵 Type 'threads' (10), 'threads 20', or 'threads all' to list threads.")
        print("🔄 Type 'switch #N' to switch to thread N, or 'switch <thread_id>' for full ID.")
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
                    print("1. API-based Login (Recommended for desktop)")
                    print("2. Interactive Browser Login (fallback)")
                    print("3. Device Code Flow") 
                    print("4. Integrated Windows Authentication")
                    
                    try:
                        choice = input("Select option (1-4): ").strip()
                        
                        if choice == "1":
                            success, msg = login_tool.auth.login_interactive()
                            if success:
                                user_info = login_tool.auth.get_current_user()
                            else:
                                print(f"❌ API-based authentication failed: {msg}")
                        elif choice == "2":
                            success, msg = login_tool.auth.login_interactive_fallback()
                            if success:
                                user_info = login_tool.auth.get_current_user()
                            else:
                                print(f"❌ Browser authentication failed: {msg}")
                        elif choice == "3":
                            success, msg = login_tool.auth.login_device_code()
                            if success:
                                user_info = login_tool.auth.get_current_user()
                            else:
                                print(f"❌ Device code authentication failed: {msg}")
                        elif choice == "4":
                            success, msg = login_tool.auth.login_integrated()
                            if success:
                                user_info = login_tool.auth.get_current_user()
                            else:
                                print(f"❌ Integrated authentication failed: {msg}")
                        else:
                            print("Invalid choice.")
                            continue
                            
                        if user_info:
                            user_id = user_info.user_id
                            assistant.user_id = user_id  # Update assistant's user context
                            print(f"✅ Authenticated as: {user_info.display_name} ({user_info.email})")
                            print("You can now access your saved conversation threads with 'threads' command.")
                        else:
                            print(f"❌ Authentication failed: {msg}")
                            
                    except KeyboardInterrupt:
                        print("\nLogin cancelled.")
                    continue
                    
                elif user_input.lower() == 'stats':
                    stats = await assistant.get_conversation_stats()
                    print("\n📊 Conversation Statistics:")
                    for key, value in stats.items():
                        print(f"   {key}: {value}")
                    continue
                elif user_input.lower() == 'threads' or user_input.lower().startswith('threads '):
                    if config.require_authentication and not user_id:
                        print("🔐 Please log in first to view your threads. Type 'login' to authenticate.")
                        continue
                    
                    # Parse threads command for number specification
                    parts = user_input.lower().split()
                    if len(parts) == 1:  # Just 'threads'
                        limit = 10
                        show_help = True
                    elif len(parts) == 2:
                        if parts[1] == 'all':
                            limit = 50  # Max threads to show
                            show_help = False
                        elif parts[1].isdigit():
                            limit = min(int(parts[1]), 50)  # Cap at 50 for performance
                            show_help = False
                        else:
                            print("💡 Usage: 'threads', 'threads 20', 'threads all'")
                            continue
                    else:
                        print("💡 Usage: 'threads', 'threads 20', 'threads all'")
                        continue
                        
                    threads = await assistant.list_all_threads()
                    if threads:
                        thread_selector = ThreadSelection()
                        display_text = thread_selector.display_thread_options(threads[:limit])
                        # Remove the choice prompt since this is just for viewing
                        display_only = display_text.rsplit('\n💡', 1)[0]
                        print(display_only)
                        
                        # Show pagination info
                        total_threads = len(threads)
                        if total_threads > limit:
                            print(f"\n📊 Showing {limit} of {total_threads} threads")
                            remaining = total_threads - limit
                            if limit == 10:
                                print(f"💡 Use 'threads 20' to see 20 threads, or 'threads all' to see all {total_threads}")
                            elif limit < total_threads:
                                print(f"💡 Use 'threads all' to see all {total_threads} threads")
                        
                        if show_help:
                            print("\n💡 Use 'switch #N' to switch to thread N (e.g., 'switch #2')")
                            print("💡 Use 'switch <thread_id>' to switch by full ID")
                    else:
                        print("\n🧵 No threads found.")
                    continue
                elif user_input.lower().startswith('switch '):
                    if config.require_authentication and not user_id:
                        print("🔐 Please log in first to switch threads. Type 'login' to authenticate.")
                        continue
                        
                    target = user_input[7:].strip()
                    target_thread = None
                    
                    # Handle switch #N format
                    if target.startswith('#'):
                        try:
                            thread_num = int(target[1:])
                            threads = await assistant.list_all_threads()
                            if 1 <= thread_num <= len(threads):
                                target_thread = threads[thread_num - 1]['thread_id']
                                print(f"🔄 Switching to thread #{thread_num}: {threads[thread_num - 1]['title']}")
                            else:
                                print(f"❌ Invalid thread number. Use 'threads' to see available threads (1-{len(threads)})")
                                continue
                        except ValueError:
                            print("❌ Invalid format. Use 'switch #N' (e.g., 'switch #2') or 'switch <thread_id>'")
                            continue
                    else:
                        # Handle full thread ID
                        target_thread = target
                    
                    if target_thread and await assistant.switch_thread(target_thread):
                        print(f"✅ Switched to thread: {target_thread[:24]}...")
                    else:
                        print(f"❌ Could not switch to thread: {target}")
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