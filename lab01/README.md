# Lab 01: Getting Started with Snowflake

## 🎯 Objectives
By the end of this lab, you will:
- Have a working Snowflake trial account
- Understand the Snowflake web interface
- Execute your first SQL queries
- Understand basic Snowflake concepts

## ⏱️ Estimated Time: 30 minutes

## 📋 Prerequisites
- A valid email address
- Internet access
- Modern web browser (Chrome, Firefox, Safari, Edge)

## 🚀 Step 1: Setting Up Your Snowflake Trial Account

### 1.1 Sign Up for Free Trial
1. Navigate to [https://trial.snowflake.com/](https://trial.snowflake.com/)
2. Click "START FOR FREE"
3. Fill in your details:
   - First Name and Last Name
   - Email address
   - Company name (can be personal/learning)
   - Job title (can be "Student" or "Learner")

### 1.2 Choose Your Cloud Provider
Select your preferred cloud provider:
- **AWS (Amazon Web Services)** - Most popular, extensive global presence
- **Microsoft Azure** - Good integration with Microsoft ecosystem
- **Google Cloud Platform (GCP)** - Strong in analytics and ML

**Recommendation**: Choose AWS for this tutorial unless you have specific requirements.

### 1.3 Select Region
Choose a region closest to your location for better performance:
- North America: US East (N. Virginia), US West (Oregon)
- Europe: Europe (Ireland), Europe (Frankfurt)
- Asia Pacific: Asia Pacific (Sydney), Asia Pacific (Tokyo)

### 1.4 Complete Registration
1. Agree to the terms of service
2. Click "GET STARTED"
3. Check your email for activation link
4. Click the activation link in your email
5. Set your username and password

## 🏠 Step 2: Exploring the Snowflake Web Interface

### 2.1 Login to Snowflake
1. Use the URL provided in your welcome email
2. Login with your credentials
3. You'll see the Snowflake web interface (SnowSight)

### 2.2 Interface Overview
**Main Navigation (Left Sidebar):**
- **Worksheets**: Where you write and execute SQL
- **Dashboards**: For data visualization
- **Data**: Browse databases, schemas, and tables
- **Marketplace**: Access shared data
- **Admin**: Account and user management

**Top Navigation:**
- Account selector (if you have multiple accounts)
- User profile and settings
- Help and documentation

### 2.3 Key Concepts
- **Account**: Your Snowflake instance
- **Warehouse**: Compute resources for queries
- **Database**: Container for schemas
- **Schema**: Container for tables and other objects
- **Role**: Security mechanism for access control

## 📊 Step 3: Your First SQL Queries

### 3.1 Create a Worksheet
1. Click on "Worksheets" in the left navigation
2. Click "+ Worksheet" to create a new worksheet
3. Name your worksheet "Lab01_Getting_Started"

### 3.2 Create a Virtual Warehouse
A virtual warehouse provides the compute power for your queries.

```sql
-- Create a virtual warehouse for our labs
CREATE WAREHOUSE IF NOT EXISTS LEARN_WH
WITH WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
COMMENT = 'Warehouse for Snowflake learning labs';

-- Use the warehouse
USE WAREHOUSE LEARN_WH;
```

### 3.3 Create a Database and Schema
```sql
-- Create a database for our labs
CREATE DATABASE IF NOT EXISTS LEARN_SNOWFLAKE
COMMENT = 'Database for Snowflake learning labs';

-- Use the database
USE DATABASE LEARN_SNOWFLAKE;

-- Create a schema
CREATE SCHEMA IF NOT EXISTS SANDBOX
COMMENT = 'Schema for practice and experimentation';

-- Use the schema
USE SCHEMA SANDBOX;
```

### 3.4 Create Your First Table
```sql
-- Create a simple employee table
CREATE OR REPLACE TABLE employees (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    name STRING,
    department STRING,
    salary NUMBER,
    hire_date DATE
);

-- Insert some sample data
INSERT INTO employees (name, department, salary, hire_date) VALUES
('John Smith', 'Engineering', 75000, '2023-01-15'),
('Jane Doe', 'Marketing', 65000, '2023-02-20'),
('Bob Johnson', 'Sales', 60000, '2023-03-10'),
('Alice Brown', 'Engineering', 80000, '2023-01-25'),
('Charlie Wilson', 'HR', 55000, '2023-04-05');
```

### 3.5 Query Your Data
```sql
-- View all employees
SELECT * FROM employees;

-- Count employees by department
SELECT department, COUNT(*) as employee_count
FROM employees
GROUP BY department
ORDER BY employee_count DESC;

-- Find highest paid employees
SELECT name, department, salary
FROM employees
WHERE salary > 70000
ORDER BY salary DESC;

-- Calculate average salary by department
SELECT department, 
       AVG(salary) as avg_salary,
       MIN(salary) as min_salary,
       MAX(salary) as max_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC;
```

## 🔍 Step 4: Exploring Snowflake Features

### 4.1 Query History
1. Click on "Activity" in the top navigation
2. Select "Query History"
3. Review all the queries you've executed
4. Notice the execution time and data scanned

### 4.2 Data Preview
1. Navigate to "Data" in the left sidebar
2. Expand your database and schema
3. Click on the "employees" table
4. Explore the table structure and sample data

### 4.3 Warehouse Monitoring
```sql
-- Check warehouse usage
SHOW WAREHOUSES;

-- View warehouse usage history
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'LEARN_WH'
ORDER BY START_TIME DESC
LIMIT 10;
```

## 📈 Step 5: Understanding Costs and Credits

### 5.1 Monitoring Credits
Snowflake provides $400 worth of free credits for your trial:
- Virtual warehouses consume credits when running
- Storage has minimal cost during trial
- Credits are consumed per second of warehouse usage

### 5.2 Best Practices for Credit Management
```sql
-- Always suspend warehouses when not in use
ALTER WAREHOUSE LEARN_WH SUSPEND;

-- Resume when needed
ALTER WAREHOUSE LEARN_WH RESUME;

-- Check current warehouse status
SHOW WAREHOUSES LIKE 'LEARN_WH';
```

## 🧹 Step 6: Cleanup (Optional)

If you want to clean up the resources created in this lab:

```sql
-- Drop the table
DROP TABLE IF EXISTS employees;

-- Suspend the warehouse
ALTER WAREHOUSE LEARN_WH SUSPEND;

-- Note: We'll keep the database and warehouse for future labs
```

## ✅ Lab Completion Checklist

- [ ] Successfully created Snowflake trial account
- [ ] Logged into Snowflake web interface
- [ ] Created virtual warehouse (LEARN_WH)
- [ ] Created database (LEARN_SNOWFLAKE) and schema (SANDBOX)
- [ ] Created and populated employees table
- [ ] Executed various SQL queries
- [ ] Explored query history and data browser
- [ ] Understood credit consumption basics

## 🎉 Congratulations!

You've successfully completed Lab 01! You now have:
- A working Snowflake environment
- Basic understanding of the interface
- Experience with fundamental SQL operations
- Knowledge of warehouse and credit management

## 🔜 Next Steps

Ready for more? Continue with [Lab 02: Snowflake Architecture & Basic SQL](../lab02/) to deepen your understanding of Snowflake's unique architecture and advanced SQL capabilities.

## 🆘 Troubleshooting

### Common Issues:

**Issue**: Can't access Snowflake after signup
**Solution**: Check your email (including spam folder) for the activation link

**Issue**: Queries taking too long
**Solution**: Ensure your warehouse is running: `ALTER WAREHOUSE LEARN_WH RESUME;`

**Issue**: Permission errors
**Solution**: Make sure you're using the correct role. Try: `USE ROLE ACCOUNTADMIN;`

**Issue**: Can't see created objects
**Solution**: Verify you're in the correct context:
```sql
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
```

## 📚 Additional Resources

- [Snowflake Getting Started Guide](https://docs.snowflake.com/en/user-guide-getting-started.html)
- [Snowflake Trial Guide](https://docs.snowflake.com/en/user-guide/getting-started-tutorial.html)
- [SQL Reference](https://docs.snowflake.com/en/sql-reference.html)