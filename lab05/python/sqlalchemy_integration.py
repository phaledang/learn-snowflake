# SQLAlchemy Integration with Snowflake
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
import sys
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_connection_info

load_dotenv()

class SnowflakeDataAnalyzer:
    def __init__(self):
        # Get connection configuration from our utility
        config = get_connection_info()
        
        # Check if we have a connection string first
        conn_string = os.getenv('SNOWFLAKE_CONNECTION_STRING')
        
        if conn_string:
            # Parse the connection string to get account info for display
            from urllib.parse import urlparse
            parsed = urlparse(conn_string)
            # Normalize account format (remove .snowflakecomputing.com if present)
            account = parsed.hostname
            if account and account.endswith('.snowflakecomputing.com'):
                account = account.replace('.snowflakecomputing.com', '')
            
            # For SQLAlchemy, we need to fix the connection string format
            # SQLAlchemy expects: snowflake://user:password@account-locator/database/schema?params
            user = parsed.username
            password = parsed.password
            database = parsed.path.split('/')[1] if len(parsed.path.split('/')) > 1 else None
            schema = parsed.path.split('/')[2] if len(parsed.path.split('/')) > 2 else None
            query = parsed.query
            
            # Build corrected connection URL with normalized account
            connection_url = f"snowflake://{user}:{password}@{account}/{database}/{schema}"
            if query:
                connection_url += f"?{query}"
            
            print(f"✅ Using connection string for account: {account}")
        else:
            # Build connection string from individual parameters
            account = config.get('account')
            user = config.get('user')
            password = config.get('password')
            warehouse = config.get('warehouse')
            database = config.get('database')
            schema = config.get('schema')
            role = config.get('role')
            
            # URL-encode password to handle special characters
            encoded_password = quote_plus(password) if password else ''
            
            # Build connection URL
            connection_url = f"snowflake://{user}:{encoded_password}@{account}/{database}/{schema}"
            if warehouse:
                connection_url += f"?warehouse={warehouse}"
            if role:
                separator = '&' if '?' in connection_url else '?'
                connection_url += f"{separator}role={role}"
            
            print(f"✅ Built connection string for account: {account}")
        
        # Create SQLAlchemy engine
        try:
            self.engine = create_engine(connection_url)
            print("✅ SQLAlchemy engine created successfully")
        except Exception as e:
            print(f"❌ Error creating SQLAlchemy engine: {e}")
            raise
    
    def query_to_dataframe(self, query):
        """Execute query and return pandas DataFrame"""
        try:
            # Use SQLAlchemy engine with pandas - this eliminates the warning
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    
    def dataframe_to_snowflake(self, df, table_name, if_exists='replace'):
        """Write pandas DataFrame to Snowflake table using SQLAlchemy"""
        try:
            # Use pandas to_sql with SQLAlchemy engine - much cleaner!
            df.to_sql(
                name=table_name.lower(),  # Snowflake prefers lowercase table names
                con=self.engine,
                if_exists=if_exists,
                index=False,  # Don't write DataFrame index as a column
                method='multi'  # Use multi-row inserts for better performance
            )
            print(f"✅ Successfully wrote {len(df)} rows to {table_name}")
            
        except Exception as e:
            print(f"❌ Error writing to Snowflake: {e}")
    
    def close(self):
        """Close connection"""
        if self.engine:
            self.engine.dispose()

# Example usage and analysis functions
def analyze_sample_data():
    """Analyze sample data from Snowflake"""
    analyzer = SnowflakeDataAnalyzer()
    
    # Query data into pandas - using actual employee table columns
    df = analyzer.query_to_dataframe("""
        SELECT 
            id,
            name,
            department,
            salary,
            hire_date
        FROM employees
        LIMIT 100
    """)
    
    if df is not None:
        print("📊 Data Analysis Results:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print("\nFirst 5 rows:")
        print(df.head())
        
        print("\nData types:")
        print(df.dtypes)
        
        print("\nBasic statistics:")
        print(df.describe())
        
        # Department analysis
        if 'department' in df.columns:
            print("\n👥 Department Analysis:")
            dept_counts = df['department'].value_counts()
            print(dept_counts)
        
        # Salary analysis
        if 'salary' in df.columns:
            print(f"\n💰 Salary Analysis:")
            print(f"   Average salary: ${df['salary'].mean():,.2f}")
            print(f"   Median salary: ${df['salary'].median():,.2f}")
            print(f"   Min salary: ${df['salary'].min():,.2f}")
            print(f"   Max salary: ${df['salary'].max():,.2f}")
        
        # Create sample analysis DataFrame using correct column names
        analysis_results = pd.DataFrame({
            'metric': ['total_employees', 'unique_departments', 'avg_salary'],
            'value': [
                len(df), 
                df['department'].nunique() if 'department' in df.columns else 0, 
                df['salary'].mean() if 'salary' in df.columns else 0
            ]
        })
        
        # Write analysis results back to Snowflake
        analyzer.dataframe_to_snowflake(analysis_results, 'python_analysis_results')
        
        # Show what we created
        print("\n📊 Analysis Results Table Created:")
        results_df = analyzer.query_to_dataframe("SELECT * FROM python_analysis_results")
        if results_df is not None:
            print(results_df)
    
    analyzer.close()

if __name__ == "__main__":
    analyze_sample_data()