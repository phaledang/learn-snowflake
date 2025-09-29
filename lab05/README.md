# Lab 05: Python Integration with Snowflake

## 🎯 Objectives
By the end of this lab, you will:
- Connect to Snowflake using Python
- Use the Snowflake Python connector
- Work with pandas and Snowflake
- Implement data analysis workflows
- Create automated data pipelines
- Build interactive applications

## ⏱️ Estimated Time: 75 minutes

## 📋 Prerequisites
- Completed Labs 01-03
- Python 3.7+ installed
- Basic Python programming knowledge
- Understanding of pandas library

## 🛠️ Step 1: Setting Up Python Environment

### 1.1 Required Libraries

```bash
# Install required packages
pip install snowflake-connector-python
pip install pandas
pip install numpy
pip install matplotlib
pip install seaborn
pip install jupyter
pip install python-dotenv
```

### 1.2 Environment Configuration

Create a `.env` file for secure credential storage:

```env
# Snowflake Connection Parameters
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=LEARN_WH
SNOWFLAKE_DATABASE=LEARN_SNOWFLAKE
SNOWFLAKE_SCHEMA=SANDBOX
```

## 🔌 Step 2: Basic Snowflake Connection

### 2.1 Simple Connection Script

```python
# File: basic_connection.py
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_connection():
    """Create and return Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE'),
        )
        print("✅ Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        return None

def test_connection():
    """Test basic connection and query"""
    conn = create_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Test query
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            
            print(f"Database: {result[0]}")
            print(f"Schema: {result[1]}")
            print(f"Warehouse: {result[2]}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error executing query: {e}")

if __name__ == "__main__":
    test_connection()
```

### 2.2 Connection with Context Manager

```python
# File: connection_manager.py
import snowflake.connector
import os
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv()

@contextmanager
def snowflake_connection():
    """Context manager for Snowflake connections"""
    conn = None
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        yield conn
    except Exception as e:
        print(f"Connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def execute_query(query):
    """Execute a single query and return results"""
    with snowflake_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        return results, columns

# Example usage
if __name__ == "__main__":
    query = "SELECT COUNT(*) as total_records FROM employees"
    results, columns = execute_query(query)
    print(f"Columns: {columns}")
    print(f"Results: {results}")
```

## 📊 Step 3: Working with Pandas

### 3.1 Pandas Integration

```python
# File: pandas_integration.py
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
            # Note: This requires pandas write capability
            # In practice, you might use write_pandas or COPY INTO
            
            # For now, let's show the concept with individual inserts
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

# Example usage
if __name__ == "__main__":
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
    """)
    
    if df is not None:
        print("📊 Data Analysis Results:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print("\nFirst 5 rows:")
        print(df.head())
        
        print("\nSummary statistics:")
        print(df.describe())
        
        print("\nValue counts by region:")
        print(df['region'].value_counts())
    
    analyzer.close()
```

### 3.2 Advanced Data Analysis

```python
# File: advanced_analysis.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas_integration import SnowflakeDataAnalyzer

def analyze_sales_data():
    """Comprehensive sales data analysis"""
    analyzer = SnowflakeDataAnalyzer()
    
    # Get sales data from Snowflake
    sales_df = analyzer.query_to_dataframe("""
        SELECT 
            s.*,
            EXTRACT(MONTH FROM purchase_date) as purchase_month,
            EXTRACT(YEAR FROM purchase_date) as purchase_year,
            EXTRACT(DOW FROM purchase_date) as day_of_week
        FROM sample_data s
        ORDER BY purchase_date
    """)
    
    if sales_df is None:
        print("❌ Failed to retrieve data")
        return
    
    # Basic analysis
    print("📈 SALES DATA ANALYSIS")
    print("=" * 50)
    
    # Revenue by region
    revenue_by_region = sales_df.groupby('region')['amount'].agg([
        'count', 'sum', 'mean', 'std'
    ]).round(2)
    print("\n💰 Revenue by Region:")
    print(revenue_by_region)
    
    # Category analysis
    category_stats = sales_df.groupby('product_category').agg({
        'amount': ['count', 'sum', 'mean'],
        'customer_id': 'nunique'
    }).round(2)
    print("\n📦 Category Statistics:")
    print(category_stats)
    
    # Time-based analysis
    monthly_sales = sales_df.groupby(['purchase_year', 'purchase_month'])['amount'].sum()
    print("\n📅 Monthly Sales Trends:")
    print(monthly_sales)
    
    # Customer insights
    customer_stats = sales_df.groupby('customer_id').agg({
        'amount': ['count', 'sum', 'mean'],
        'product_category': lambda x: len(x.unique())
    }).round(2)
    customer_stats.columns = ['order_count', 'total_spent', 'avg_order', 'categories_purchased']
    
    print("\n👥 Top Customers:")
    print(customer_stats.sort_values('total_spent', ascending=False).head())
    
    # Create visualizations
    create_visualizations(sales_df)
    
    analyzer.close()

def create_visualizations(df):
    """Create data visualizations"""
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # Revenue by region
    df.groupby('region')['amount'].sum().plot(kind='bar', ax=axes[0,0], title='Revenue by Region')
    axes[0,0].set_ylabel('Revenue ($)')
    
    # Amount distribution
    df['amount'].hist(bins=20, ax=axes[0,1], title='Purchase Amount Distribution')
    axes[0,1].set_xlabel('Amount ($)')
    axes[0,1].set_ylabel('Frequency')
    
    # Category breakdown
    df['product_category'].value_counts().plot(kind='pie', ax=axes[1,0], title='Sales by Category')
    
    # Amount by category boxplot
    df.boxplot(column='amount', by='product_category', ax=axes[1,1])
    axes[1,1].set_title('Amount Distribution by Category')
    axes[1,1].set_xlabel('Product Category')
    
    plt.tight_layout()
    plt.savefig('sales_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    analyze_sales_data()
```

## 🤖 Step 4: Automated Data Pipelines

### 4.1 ETL Pipeline Class

```python
# File: etl_pipeline.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pandas_integration import SnowflakeDataAnalyzer

class SnowflakeETLPipeline:
    def __init__(self):
        self.analyzer = SnowflakeDataAnalyzer()
        self.pipeline_run_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def extract_data(self, source_query):
        """Extract data from Snowflake"""
        print(f"🔄 Extracting data...")
        df = self.analyzer.query_to_dataframe(source_query)
        print(f"✅ Extracted {len(df)} rows")
        return df
    
    def transform_data(self, df):
        """Apply transformations to the data"""
        print("🔄 Transforming data...")
        
        # Example transformations
        transformed_df = df.copy()
        
        # Add calculated columns
        transformed_df['email_domain'] = transformed_df['email'].str.split('@').str[1]
        transformed_df['amount_tier'] = pd.cut(
            transformed_df['amount'], 
            bins=[0, 50, 200, 500, float('inf')], 
            labels=['Low', 'Medium', 'High', 'Premium']
        )
        
        # Clean data
        transformed_df['customer_name'] = transformed_df['customer_name'].str.title()
        transformed_df['email'] = transformed_df['email'].str.lower()
        
        # Add metadata
        transformed_df['processed_date'] = datetime.now()
        transformed_df['pipeline_run_id'] = self.pipeline_run_id
        
        print(f"✅ Transformation complete")
        return transformed_df
    
    def validate_data(self, df):
        """Validate data quality"""
        print("🔄 Validating data quality...")
        
        issues = []
        
        # Check for nulls in critical columns
        critical_columns = ['customer_id', 'customer_name', 'amount']
        for col in critical_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                issues.append(f"Found {null_count} null values in {col}")
        
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            issues.append(f"Found {duplicate_count} duplicate rows")
        
        # Check amount ranges
        negative_amounts = (df['amount'] < 0).sum()
        if negative_amounts > 0:
            issues.append(f"Found {negative_amounts} negative amounts")
        
        # Log validation results
        if issues:
            print("⚠️ Data quality issues found:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        else:
            print("✅ Data validation passed")
            return True
    
    def load_data(self, df, target_table):
        """Load data to Snowflake table"""
        print(f"🔄 Loading data to {target_table}...")
        
        try:
            self.analyzer.dataframe_to_snowflake(df, target_table, if_exists='replace')
            print(f"✅ Successfully loaded {len(df)} rows to {target_table}")
            return True
        except Exception as e:
            print(f"❌ Failed to load data: {e}")
            return False
    
    def log_pipeline_run(self, status, records_processed, start_time, end_time):
        """Log pipeline execution details"""
        log_entry = {
            'pipeline_run_id': self.pipeline_run_id,
            'status': status,
            'records_processed': records_processed,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': (end_time - start_time).total_seconds()
        }
        
        # In a real implementation, you might write this to a log table
        print(f"📝 Pipeline Log: {json.dumps(log_entry, indent=2)}")
    
    def run_pipeline(self, source_query, target_table):
        """Execute the complete ETL pipeline"""
        start_time = datetime.now()
        status = 'FAILED'
        records_processed = 0
        
        try:
            print(f"🚀 Starting ETL Pipeline: {self.pipeline_run_id}")
            
            # Extract
            df = self.extract_data(source_query)
            if df is None or len(df) == 0:
                raise Exception("No data extracted")
            
            # Transform
            transformed_df = self.transform_data(df)
            
            # Validate
            if not self.validate_data(transformed_df):
                raise Exception("Data validation failed")
            
            # Load
            if not self.load_data(transformed_df, target_table):
                raise Exception("Data loading failed")
            
            records_processed = len(transformed_df)
            status = 'SUCCESS'
            print(f"🎉 Pipeline completed successfully!")
            
        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            status = 'FAILED'
        
        finally:
            end_time = datetime.now()
            self.log_pipeline_run(status, records_processed, start_time, end_time)
            self.analyzer.close()

# Example usage
if __name__ == "__main__":
    pipeline = SnowflakeETLPipeline()
    
    source_query = """
        SELECT 
            customer_id,
            customer_name,
            email,
            purchase_date,
            product_category,
            amount,
            region
        FROM sample_data
        WHERE purchase_date >= '2024-01-01'
    """
    
    pipeline.run_pipeline(source_query, 'processed_customer_data')
```

### 4.2 Scheduled Data Pipeline

```python
# File: scheduled_pipeline.py
import schedule
import time
from datetime import datetime
from etl_pipeline import SnowflakeETLPipeline

def daily_sales_pipeline():
    """Daily sales data processing pipeline"""
    print(f"⏰ Starting daily pipeline at {datetime.now()}")
    
    pipeline = SnowflakeETLPipeline()
    
    # Yesterday's sales data
    source_query = """
        SELECT 
            customer_id,
            customer_name,
            email,
            purchase_date,
            product_category,
            amount,
            region
        FROM sample_data
        WHERE purchase_date = CURRENT_DATE() - 1
    """
    
    pipeline.run_pipeline(source_query, 'daily_sales_processed')

def weekly_analytics_pipeline():
    """Weekly analytics pipeline"""
    print(f"📊 Starting weekly analytics at {datetime.now()}")
    
    analyzer = SnowflakeDataAnalyzer()
    
    # Generate weekly report
    weekly_report = analyzer.query_to_dataframe("""
        SELECT 
            region,
            product_category,
            COUNT(*) as transaction_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_transaction,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM sample_data
        WHERE purchase_date >= CURRENT_DATE() - 7
        GROUP BY region, product_category
        ORDER BY total_revenue DESC
    """)
    
    if weekly_report is not None:
        # Save report
        analyzer.dataframe_to_snowflake(weekly_report, 'weekly_analytics_report', 'replace')
        print("✅ Weekly analytics completed")
    
    analyzer.close()

def setup_schedules():
    """Setup scheduled jobs"""
    # Schedule daily pipeline at 2 AM
    schedule.every().day.at("02:00").do(daily_sales_pipeline)
    
    # Schedule weekly analytics on Mondays at 6 AM
    schedule.every().monday.at("06:00").do(weekly_analytics_pipeline)
    
    print("📅 Schedules configured:")
    print("  - Daily sales pipeline: 2:00 AM")
    print("  - Weekly analytics: Monday 6:00 AM")

def run_scheduler():
    """Run the scheduler"""
    setup_schedules()
    
    print("🔄 Scheduler started. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n⏹️ Scheduler stopped")

if __name__ == "__main__":
    # For testing, run pipelines immediately
    print("🧪 Running test pipelines...")
    daily_sales_pipeline()
    weekly_analytics_pipeline()
    
    # Uncomment to run scheduler
    # run_scheduler()
```

## 📱 Step 5: Interactive Applications

### 5.1 Command Line Interface

```python
# File: cli_app.py
import argparse
import sys
from pandas_integration import SnowflakeDataAnalyzer

class SnowflakeCLI:
    def __init__(self):
        self.analyzer = SnowflakeDataAnalyzer()
    
    def query_data(self, query, limit=None):
        """Execute query and display results"""
        if limit:
            query = f"{query} LIMIT {limit}"
        
        df = self.analyzer.query_to_dataframe(query)
        if df is not None:
            print(f"\n📊 Query Results ({len(df)} rows):")
            print("=" * 50)
            print(df.to_string(index=False))
        else:
            print("❌ Query failed")
    
    def show_tables(self):
        """Show all tables in current schema"""
        query = """
            SELECT table_name, row_count, bytes
            FROM information_schema.tables
            WHERE table_schema = CURRENT_SCHEMA()
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        self.query_data(query)
    
    def describe_table(self, table_name):
        """Describe table structure"""
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = UPPER('{table_name}')
            AND table_schema = CURRENT_SCHEMA()
            ORDER BY ordinal_position
        """
        self.query_data(query)
    
    def sample_data(self, table_name, limit=10):
        """Show sample data from table"""
        query = f"SELECT * FROM {table_name}"
        self.query_data(query, limit)
    
    def close(self):
        """Close connection"""
        self.analyzer.close()

def main():
    parser = argparse.ArgumentParser(description='Snowflake CLI Tool')
    parser.add_argument('command', choices=['query', 'tables', 'describe', 'sample'])
    parser.add_argument('--table', help='Table name for describe/sample commands')
    parser.add_argument('--query', help='SQL query to execute')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of rows')
    
    args = parser.parse_args()
    
    cli = SnowflakeCLI()
    
    try:
        if args.command == 'query':
            if not args.query:
                print("❌ Query required. Use --query 'SELECT...'")
                sys.exit(1)
            cli.query_data(args.query, args.limit)
        
        elif args.command == 'tables':
            cli.show_tables()
        
        elif args.command == 'describe':
            if not args.table:
                print("❌ Table name required. Use --table TABLE_NAME")
                sys.exit(1)
            cli.describe_table(args.table)
        
        elif args.command == 'sample':
            if not args.table:
                print("❌ Table name required. Use --table TABLE_NAME")
                sys.exit(1)
            cli.sample_data(args.table, args.limit)
    
    finally:
        cli.close()

if __name__ == "__main__":
    main()
```

### 5.2 Simple Web Dashboard

```python
# File: web_dashboard.py
from flask import Flask, render_template, request, jsonify
import pandas as pd
import json
from pandas_integration import SnowflakeDataAnalyzer

app = Flask(__name__)

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/summary')
def get_summary():
    """Get summary statistics"""
    analyzer = SnowflakeDataAnalyzer()
    
    try:
        # Get summary data
        summary_df = analyzer.query_to_dataframe("""
            SELECT 
                COUNT(*) as total_customers,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_order_value,
                COUNT(DISTINCT region) as regions_served
            FROM sample_data
        """)
        
        summary = summary_df.iloc[0].to_dict()
        
        # Get regional breakdown
        region_df = analyzer.query_to_dataframe("""
            SELECT region, SUM(amount) as revenue
            FROM sample_data
            GROUP BY region
            ORDER BY revenue DESC
        """)
        
        regions = region_df.to_dict('records')
        
        return jsonify({
            'summary': summary,
            'regions': regions
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        analyzer.close()

@app.route('/api/customers')
def get_customers():
    """Get customer data"""
    analyzer = SnowflakeDataAnalyzer()
    
    try:
        customers_df = analyzer.query_to_dataframe("""
            SELECT 
                customer_id,
                customer_name,
                email,
                region,
                amount,
                product_category,
                purchase_date
            FROM sample_data
            ORDER BY purchase_date DESC
            LIMIT 100
        """)
        
        # Convert datetime to string for JSON serialization
        customers_df['purchase_date'] = customers_df['purchase_date'].astype(str)
        
        return jsonify(customers_df.to_dict('records'))
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        analyzer.close()

# HTML Template (save as templates/dashboard.html)
dashboard_html = """
<!DOCTYPE html>
<html>
<head>
    <title>Snowflake Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .summary-card { 
            display: inline-block; 
            margin: 10px; 
            padding: 20px; 
            background: #f5f5f5; 
            border-radius: 5px;
            min-width: 200px;
        }
        .chart-container { width: 400px; height: 400px; margin: 20px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>📊 Snowflake Sales Dashboard</h1>
    
    <div id="summary-section">
        <h2>Summary Statistics</h2>
        <div id="summary-cards"></div>
    </div>
    
    <div class="chart-container">
        <canvas id="regionChart"></canvas>
    </div>
    
    <div id="customers-section">
        <h2>Recent Customers</h2>
        <table id="customers-table">
            <thead>
                <tr>
                    <th>Customer</th>
                    <th>Email</th>
                    <th>Region</th>
                    <th>Category</th>
                    <th>Amount</th>
                    <th>Date</th>
                </tr>
            </thead>
            <tbody id="customers-body"></tbody>
        </table>
    </div>

    <script>
        // Load summary data
        fetch('/api/summary')
            .then(response => response.json())
            .then(data => {
                const summaryCards = document.getElementById('summary-cards');
                const summary = data.summary;
                
                summaryCards.innerHTML = `
                    <div class="summary-card">
                        <h3>Total Customers</h3>
                        <p>${summary.total_customers}</p>
                    </div>
                    <div class="summary-card">
                        <h3>Total Revenue</h3>
                        <p>$${summary.total_revenue.toFixed(2)}</p>
                    </div>
                    <div class="summary-card">
                        <h3>Avg Order Value</h3>
                        <p>$${summary.avg_order_value.toFixed(2)}</p>
                    </div>
                    <div class="summary-card">
                        <h3>Regions Served</h3>
                        <p>${summary.regions_served}</p>
                    </div>
                `;
                
                // Create chart
                const ctx = document.getElementById('regionChart').getContext('2d');
                new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: data.regions.map(r => r.region),
                        datasets: [{
                            label: 'Revenue by Region',
                            data: data.regions.map(r => r.revenue),
                            backgroundColor: [
                                'rgba(255, 99, 132, 0.2)',
                                'rgba(54, 162, 235, 0.2)',
                                'rgba(255, 205, 86, 0.2)',
                                'rgba(75, 192, 192, 0.2)'
                            ],
                            borderColor: [
                                'rgba(255, 99, 132, 1)',
                                'rgba(54, 162, 235, 1)',
                                'rgba(255, 205, 86, 1)',
                                'rgba(75, 192, 192, 1)'
                            ],
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        plugins: {
                            title: {
                                display: true,
                                text: 'Revenue by Region'
                            }
                        }
                    }
                });
            });
        
        // Load customer data
        fetch('/api/customers')
            .then(response => response.json())
            .then(customers => {
                const tbody = document.getElementById('customers-body');
                tbody.innerHTML = customers.map(customer => `
                    <tr>
                        <td>${customer.customer_name}</td>
                        <td>${customer.email}</td>
                        <td>${customer.region}</td>
                        <td>${customer.product_category}</td>
                        <td>$${customer.amount}</td>
                        <td>${customer.purchase_date}</td>
                    </tr>
                `).join('');
            });
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    # Create templates directory and file
    import os
    os.makedirs('templates', exist_ok=True)
    with open('templates/dashboard.html', 'w') as f:
        f.write(dashboard_html)
    
    print("🌐 Starting web dashboard at http://localhost:5000")
    app.run(debug=True)
```

## ✅ Lab Completion Checklist

- [ ] Set up Python environment with required packages
- [ ] Created secure connection to Snowflake using Python
- [ ] Integrated pandas for data analysis
- [ ] Built automated ETL pipelines
- [ ] Implemented data quality validation
- [ ] Created scheduled data processing jobs
- [ ] Built command-line interface for Snowflake
- [ ] Developed web dashboard for data visualization

## 🎉 Congratulations!

You've mastered Python integration with Snowflake! Key achievements:
- Established secure connections using Python
- Performed advanced data analysis with pandas
- Built robust ETL pipelines with error handling
- Created automated scheduling for data processing
- Developed interactive applications and dashboards

## 🔜 Next Steps

Continue with [Lab 06: Data Sharing & Security](../lab06/) to explore:
- Secure data sharing mechanisms
- Role-based access control
- Data governance practices
- Privacy and compliance features

Or jump ahead to [Lab 07: Advanced Python LangChain OpenAI Assistant](../lab07/) to build:
- AI-powered natural language database queries
- Intelligent conversation interfaces
- Advanced file processing and analysis
- Enterprise-ready AI assistant with Snowflake integration

## 🆘 Troubleshooting

### Common Issues:

**Issue**: Connection timeout or failure
**Solution**: Check network connectivity and verify account identifier format

**Issue**: pandas read_sql fails
**Solution**: Ensure query syntax is correct and connection is active

**Issue**: Large dataset memory issues
**Solution**: Use chunking or server-side processing for large datasets

**Issue**: Authentication errors
**Solution**: Verify credentials and account permissions

## 📚 Additional Resources

- [Snowflake Python Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [Pandas Integration Guide](https://docs.snowflake.com/en/user-guide/python-connector-pandas.html)
- [Python Best Practices](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect.html)