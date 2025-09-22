# Lab 01 Presentation: Getting Started with Snowflake

## Slide 1: Welcome to Snowflake! ❄️

**Lab 01: Getting Started with Snowflake**
- Your first steps into the data cloud
- Setting up trial account
- Understanding the interface
- Running your first queries

---

## Slide 2: What is Snowflake? 🤔

**Snowflake is a cloud data platform that provides:**
- Data warehouse capabilities
- Data lake functionality  
- Data sharing and collaboration
- Built for the cloud (AWS, Azure, GCP)
- Separate compute and storage
- Automatic scaling and optimization

**Key Benefits:**
- No infrastructure management
- Pay for what you use
- Instant scalability
- Zero maintenance

---

## Slide 3: Snowflake Architecture 🏗️

**Three Key Layers:**

1. **Storage Layer**
   - Compressed, columnar storage
   - Automatically managed
   - Separate from compute

2. **Compute Layer (Virtual Warehouses)**
   - Independent compute clusters
   - Auto-suspend/resume
   - Multiple sizes available

3. **Cloud Services Layer**
   - Security and metadata
   - Query optimization
   - Access control

---

## Slide 4: Key Concepts 📚

**Virtual Warehouse**
- Compute resources for queries
- Can be suspended to save costs
- Multiple warehouses can run simultaneously

**Database → Schema → Table**
- Hierarchical organization
- Database: Top-level container
- Schema: Logical grouping within database
- Table: Where your data lives

**Roles & Security**
- Role-based access control
- Principle of least privilege
- Built-in roles: ACCOUNTADMIN, SYSADMIN, etc.

---

## Slide 5: Trial Account Benefits 🎁

**What you get with trial:**
- $400 in free credits
- 30-day access
- Full Snowflake functionality
- No credit card required
- Access to sample datasets

**Credit Usage:**
- Warehouses consume credits when running
- Storage costs are minimal
- Credits consumed per second
- Auto-suspend saves money

---

## Slide 6: Snowflake Interface Tour 🖥️

**SnowSight Web Interface:**

**Left Navigation:**
- Worksheets (SQL editor)
- Dashboards (visualization)
- Data (browse objects)
- Marketplace (shared data)
- Admin (account management)

**Key Features:**
- Intelligent autocomplete
- Query history
- Result visualization
- Collaborative worksheets

---

## Slide 7: Your First Warehouse 🏭

**Creating a Virtual Warehouse:**

```sql
CREATE WAREHOUSE LEARN_WH
WITH WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
```

**Best Practices:**
- Start with X-SMALL for learning
- Use AUTO_SUSPEND to save credits
- AUTO_RESUME for convenience
- Suspend when not in use

---

## Slide 8: Database & Schema Setup 🗂️

**Organizational Structure:**

```sql
-- Create database
CREATE DATABASE LEARN_SNOWFLAKE;

-- Create schema  
CREATE SCHEMA SANDBOX;

-- Set context
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;
USE WAREHOUSE LEARN_WH;
```

**Why This Matters:**
- Logical organization
- Security boundaries
- Easy management
- Clear naming conventions

---

## Slide 9: Your First Table 📊

**Creating and Populating Data:**

```sql
-- Create table with auto-increment ID
CREATE TABLE employees (
    id NUMBER AUTOINCREMENT,
    name STRING,
    department STRING,
    salary NUMBER,
    hire_date DATE
);

-- Insert sample data
INSERT INTO employees VALUES
('John Smith', 'Engineering', 75000, '2023-01-15'),
('Jane Doe', 'Marketing', 65000, '2023-02-20');
```

---

## Slide 10: Basic Queries 🔍

**Essential SQL Operations:**

```sql
-- View all data
SELECT * FROM employees;

-- Aggregate by department
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department;

-- Filter and sort
SELECT name, salary 
FROM employees 
WHERE salary > 70000
ORDER BY salary DESC;
```

---

## Slide 11: Monitoring & Management 📈

**Keep Track of Usage:**

```sql
-- Check warehouse status
SHOW WAREHOUSES;

-- View query history
-- Use Activity → Query History in UI

-- Monitor credit usage
-- Use Admin → Usage in UI
```

**Cost Control:**
- Suspend warehouses when done
- Monitor credit consumption
- Use appropriate warehouse sizes

---

## Slide 12: Next Steps 🚀

**You've Accomplished:**
✅ Set up Snowflake trial account  
✅ Created virtual warehouse  
✅ Built database structure  
✅ Created and queried tables  
✅ Learned basic cost management  

**Coming Up in Lab 02:**
- Snowflake architecture deep dive
- Advanced SQL functions
- Data types and constraints
- Performance considerations

---

## Slide 13: Key Takeaways 💡

**Remember These Fundamentals:**

1. **Snowflake = Compute + Storage + Services**
2. **Virtual Warehouses = Your Compute Power**
3. **Credits = Your Currency (use wisely!)**
4. **Context Matters** (Database.Schema.Table)
5. **Auto-suspend = Money Saver**

**Pro Tip:** Always set your context properly:
```sql
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;
```

---

## Slide 14: Questions & Resources 🤝

**Need Help?**
- Snowflake Documentation: docs.snowflake.com
- Community: community.snowflake.com
- University: university.snowflake.com
- Support: Through Snowflake web interface

**Lab Resources:**
- All SQL scripts in `/sql` folder
- Sample data in `/data` folder
- Next lab: Architecture & Advanced SQL

**Ready for Lab 02?** Let's dive deeper! 🏊‍♂️