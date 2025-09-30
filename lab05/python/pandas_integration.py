# Pandas Integration with Snowflake
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
            # Use connection string directly
            connection_url = conn_string.replace('snowflake://', 'snowflake://')
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
        
        # Create SQLAlchemy engine
        print(f"Creating SQLAlchemy engine for account: {config.get('account', 'Unknown')}")
        self.engine = create_engine(connection_url)
    
    def query_to_dataframe(self, query):
        """Execute query and return pandas DataFrame"""
        try:
            # Use SQLAlchemy engine with pandas - eliminates the UserWarning
            print(f"Executing query: {query[:100]}...")
            df = pd.read_sql(query, self.engine)
            print(f"✅ Query successful, returned {len(df)} rows")
            return df
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            return None
    
    def dataframe_to_snowflake(self, df, table_name, if_exists='replace'):
        """Write pandas DataFrame to Snowflake table using SQLAlchemy"""
        try:
            print(f"Writing {len(df)} rows to table: {table_name}")
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
            print("Connection closed")

# Example usage and analysis functions
def analyze_sample_data():
    """Analyze sample data from Snowflake"""
    analyzer = SnowflakeDataAnalyzer()
    
    # Query data into pandas
    df = analyzer.query_to_dataframe("""
        SELECT 
            customer_id,
            customer_name,
            email,
            purchase_date,
            product_category,
            amount,
            region
        FROM sample_data
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
        
        # Create sample analysis DataFrame
        analysis_results = pd.DataFrame({
            'metric': ['total_rows', 'unique_customers', 'avg_amount'],
            'value': [len(df), df['customer_id'].nunique() if 'customer_id' in df.columns else 0, 
                     df['amount'].mean() if 'amount' in df.columns else 0]
        })
        
        # Write analysis results back to Snowflake
        analyzer.dataframe_to_snowflake(analysis_results, 'python_analysis_results')
    
    analyzer.close()

if __name__ == "__main__":
    analyze_sample_data()