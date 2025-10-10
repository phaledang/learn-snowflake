"""
Streamlit Web Interface for Snowflake AI Assistant
Advanced example showing file upload, conversation history, and interactive features.
"""

import streamlit as st
import os
import tempfile
import json
from datetime import datetime
from snowflake_ai_assistant import SnowflakeAIAssistant
import pandas as pd

# Configure Streamlit page
st.set_page_config(
    page_title="Snowflake AI Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'assistant' not in st.session_state:
    st.session_state.assistant = None
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []

def initialize_assistant():
    """Initialize the AI assistant."""
    try:
        assistant = SnowflakeAIAssistant(use_azure=True)
        st.session_state.assistant = assistant
        return True
    except Exception as e:
        st.error(f"Failed to initialize assistant: {str(e)}")
        return False

def process_uploaded_file(uploaded_file):
    """Process an uploaded file and return its content."""
    try:
        # Save uploaded file to temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{uploaded_file.name.split('.')[-1]}") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name
        
        # Process file using the assistant's file processing tool
        file_tool = st.session_state.assistant.tools[2]  # FileProcessingTool
        result = file_tool._run(tmp_file_path)
        
        # Clean up temporary file
        os.unlink(tmp_file_path)
        
        return result
    except Exception as e:
        return f"Error processing file: {str(e)}"

def main():
    """Main Streamlit application."""
    
    # Header
    st.title("🤖 Snowflake AI Assistant")
    st.markdown("Advanced LangChain OpenAI Assistant with Snowflake Integration")
    
    # Sidebar for configuration and file uploads
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # Connection status
        if st.session_state.assistant is None:
            if st.button("🚀 Initialize Assistant", type="primary"):
                if initialize_assistant():
                    st.success("Assistant initialized successfully!")
                    st.rerun()
        else:
            st.success("✅ Assistant Ready")
            
            # Database info
            st.subheader("📊 Database Context")
            st.info(f"""
            **Database:** {os.getenv('SNOWFLAKE_DATABASE', 'LEARN_SNOWFLAKE')}  
            **Schema:** {os.getenv('SNOWFLAKE_SCHEMA', 'SANDBOX')}  
            **Warehouse:** {os.getenv('SNOWFLAKE_WAREHOUSE', 'LEARN_WH')}
            """)
            
            # File upload section
            st.subheader("📁 File Upload")
            uploaded_file = st.file_uploader(
                "Upload a file for analysis",
                type=['txt', 'csv', 'xlsx', 'xls', 'pdf'],
                help="Upload files to analyze with the AI assistant"
            )
            
            if uploaded_file is not None and uploaded_file not in st.session_state.uploaded_files:
                with st.spinner("Processing file..."):
                    file_content = process_uploaded_file(uploaded_file)
                    st.session_state.uploaded_files.append(uploaded_file)
                    
                    # Add file processing message to conversation
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": f"📁 **File Processed:** {uploaded_file.name}\n\n{file_content}",
                        "timestamp": datetime.now()
                    })
                    st.success(f"File {uploaded_file.name} processed!")
                    st.rerun()
            
            # Conversation controls
            st.subheader("💬 Conversation")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🗑️ Clear History"):
                    st.session_state.messages = []
                    if st.session_state.assistant:
                        st.session_state.assistant.clear_memory()
                    st.rerun()
            
            with col2:
                if st.button("💾 Export Chat"):
                    if st.session_state.messages:
                        chat_data = {
                            "export_time": datetime.now().isoformat(),
                            "message_count": len(st.session_state.messages),
                            "messages": st.session_state.messages
                        }
                        st.download_button(
                            "📥 Download JSON",
                            data=json.dumps(chat_data, indent=2, default=str),
                            file_name=f"snowflake_ai_chat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
    
    # Main chat interface
    if st.session_state.assistant is None:
        st.warning("Please initialize the assistant using the sidebar.")
        
        # Show environment setup instructions
        st.subheader("🔧 Setup Instructions")
        st.markdown("""
        1. **Environment Variables**: Make sure you have configured your `.env` file with:
           - Snowflake connection parameters
           - Azure OpenAI or OpenAI API credentials
           
        2. **Dependencies**: Install required packages:
           ```bash
           pip install -r requirements.txt
           ```
           
        3. **Initialize**: Click the "Initialize Assistant" button in the sidebar.
        """)
        
        # Show example .env file
        with st.expander("📝 Example .env Configuration"):
            st.code("""
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=LEARN_WH
SNOWFLAKE_DATABASE=LEARN_SNOWFLAKE
SNOWFLAKE_SCHEMA=SANDBOX

# Azure OpenAI
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2023-12-01-preview
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4
            """, language="bash")
        
        return
    
    # Display chat messages
    chat_container = st.container()
    with chat_container:
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                if "timestamp" in message:
                    st.caption(f"*{message['timestamp'].strftime('%H:%M:%S')}*")
    
    # Chat input
    if prompt := st.chat_input("Ask me anything about your Snowflake data..."):
        # Add user message
        st.session_state.messages.append({
            "role": "user",
            "content": prompt,
            "timestamp": datetime.now()
        })
        
        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Get assistant response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    response = st.session_state.assistant.chat(prompt)
                    st.markdown(response)
                    
                    # Add assistant message
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response,
                        "timestamp": datetime.now()
                    })
                except Exception as e:
                    error_msg = f"❌ Error: {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": error_msg,
                        "timestamp": datetime.now()
                    })
    
    # Quick action buttons
    st.subheader("🚀 Quick Actions")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("📊 Show Available Tables"):
            if prompt := "Show me all available tables in the database":
                st.session_state.messages.append({
                    "role": "user",
                    "content": prompt,
                    "timestamp": datetime.now()
                })
                st.rerun()
    
    with col2:
        if st.button("📈 Sales Analysis"):
            if prompt := "Perform a sales analysis on available data":
                st.session_state.messages.append({
                    "role": "user",
                    "content": prompt,
                    "timestamp": datetime.now()
                })
                st.rerun()
    
    with col3:
        if st.button("🔍 Data Quality Check"):
            if prompt := "Check data quality for the main tables":
                st.session_state.messages.append({
                    "role": "user",
                    "content": prompt,
                    "timestamp": datetime.now()
                })
                st.rerun()
    
    with col4:
        if st.button("💡 Optimization Tips"):
            if prompt := "Give me optimization recommendations for my Snowflake setup":
                st.session_state.messages.append({
                    "role": "user",
                    "content": prompt,
                    "timestamp": datetime.now()
                })
                st.rerun()
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        🤖 Powered by LangChain, OpenAI, and Snowflake | Built for Lab 07: Advanced AI Assistant
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()