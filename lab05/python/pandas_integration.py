# Pandas Integration with Snowflake
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

class SnowflakeDataAnalyzer:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
    
    def query_to_dataframe(self, query):
        """Execute query and return pandas DataFrame"""
        try:
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    
    def dataframe_to_snowflake(self, df, table_name, if_exists='replace'):
        """Write pandas DataFrame to Snowflake table"""
        try:
            cursor = self.conn.cursor()
            
            if if_exists == 'replace':
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table based on DataFrame structure
            create_sql = self._generate_create_table_sql(df, table_name)
            cursor.execute(create_sql)
            
            # Insert data
            for _, row in df.iterrows():
                values = ', '.join([f"'{str(val)}'" if pd.notna(val) else 'NULL' for val in row])
                insert_sql = f"INSERT INTO {table_name} VALUES ({values})"
                cursor.execute(insert_sql)
            
            cursor.close()
            print(f"✅ Successfully wrote {len(df)} rows to {table_name}")
            
        except Exception as e:
            print(f"❌ Error writing to Snowflake: {e}")
    
    def _generate_create_table_sql(self, df, table_name):
        """Generate CREATE TABLE SQL from DataFrame"""
        columns = []
        for col, dtype in df.dtypes.items():
            if dtype == 'object':
                sql_type = 'STRING'
            elif dtype in ['int64', 'int32']:
                sql_type = 'NUMBER'
            elif dtype in ['float64', 'float32']:
                sql_type = 'FLOAT'
            elif dtype == 'bool':
                sql_type = 'BOOLEAN'
            else:
                sql_type = 'STRING'
            
            columns.append(f"{col} {sql_type}")
        
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()

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