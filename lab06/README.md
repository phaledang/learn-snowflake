# Lab 06: Data Sharing & Security

## 🎯 Objectives
By the end of this lab, you will:
- Understand Snowflake's data sharing capabilities
- Implement role-based access control (RBAC)
- Create secure views and data masking
- Set up data governance practices
- Configure row-level security
- Implement data classification and tagging

## ⏱️ Estimated Time: 45 minutes

## 📋 Prerequisites
- Completed Labs 01-05
- Understanding of security concepts
- Administrative privileges in Snowflake

## 🔐 Step 1: Role-Based Access Control (RBAC)

### 1.1 Create Custom Roles

```sql
-- Set up environment
USE WAREHOUSE LEARN_WH;
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;

-- Create custom roles for different user types
USE ROLE ACCOUNTADMIN;

-- Data Analyst Role
CREATE ROLE IF NOT EXISTS DATA_ANALYST
COMMENT = 'Role for data analysts with read-only access to analytics';

-- Data Engineer Role  
CREATE ROLE IF NOT EXISTS DATA_ENGINEER
COMMENT = 'Role for data engineers with data pipeline permissions';

-- Business User Role
CREATE ROLE IF NOT EXISTS BUSINESS_USER
COMMENT = 'Role for business users with limited access to reports';

-- Data Scientist Role
CREATE ROLE IF NOT EXISTS DATA_SCIENTIST
COMMENT = 'Role for data scientists with ML and advanced analytics access';

-- Grant roles to current user for testing
GRANT ROLE DATA_ANALYST TO USER CURRENT_USER();
GRANT ROLE DATA_ENGINEER TO USER CURRENT_USER();
GRANT ROLE BUSINESS_USER TO USER CURRENT_USER();
GRANT ROLE DATA_SCIENTIST TO USER CURRENT_USER();

-- View created roles
SHOW ROLES;
```

### 1.2 Grant Permissions to Roles

```sql
-- Grant warehouse usage to roles
GRANT USAGE ON WAREHOUSE LEARN_WH TO ROLE DATA_ANALYST;
GRANT USAGE ON WAREHOUSE LEARN_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE LEARN_WH TO ROLE BUSINESS_USER;
GRANT USAGE ON WAREHOUSE LEARN_WH TO ROLE DATA_SCIENTIST;

-- Grant database and schema access
GRANT USAGE ON DATABASE LEARN_SNOWFLAKE TO ROLE DATA_ANALYST;
GRANT USAGE ON DATABASE LEARN_SNOWFLAKE TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE LEARN_SNOWFLAKE TO ROLE BUSINESS_USER;
GRANT USAGE ON DATABASE LEARN_SNOWFLAKE TO ROLE DATA_SCIENTIST;

GRANT USAGE ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE BUSINESS_USER;
GRANT USAGE ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_SCIENTIST;

-- Data Engineer: Full access to create and modify objects
GRANT CREATE TABLE ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_ENGINEER;
GRANT CREATE VIEW ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_ENGINEER;
GRANT CREATE FUNCTION ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_ENGINEER;

-- Data Analyst: Read access to all tables and ability to create views
GRANT SELECT ON ALL TABLES IN SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_ANALYST;
GRANT CREATE VIEW ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_ANALYST;

-- Business User: Limited read access
GRANT SELECT ON ALL TABLES IN SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE BUSINESS_USER;

-- Data Scientist: Read access plus ability to create functions for ML
GRANT SELECT ON ALL TABLES IN SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_SCIENTIST;
GRANT CREATE FUNCTION ON SCHEMA LEARN_SNOWFLAKE.SANDBOX TO ROLE DATA_SCIENTIST;
```

## 🎭 Step 2: Data Masking and Secure Views

### 2.1 Create Sample Sensitive Data

```sql
-- Create table with sensitive customer information
CREATE OR REPLACE TABLE sensitive_customer_data (
    customer_id NUMBER PRIMARY KEY,
    full_name STRING,
    email STRING,
    phone STRING,
    ssn STRING,
    credit_card STRING,
    salary NUMBER,
    address STRING,
    created_date DATE
);

-- Insert sample sensitive data
INSERT INTO sensitive_customer_data VALUES
(1, 'John Smith', 'john.smith@email.com', '555-123-4567', '123-45-6789', '4532-1234-5678-9012', 75000, '123 Main St, Seattle, WA', '2023-01-15'),
(2, 'Jane Doe', 'jane.doe@company.org', '555-987-6543', '987-65-4321', '4532-9876-5432-1098', 82000, '456 Oak Ave, Portland, OR', '2023-02-20'),
(3, 'Bob Johnson', 'bob.j@domain.net', '555-456-7890', '456-78-9123', '4532-2468-1357-9024', 68000, '789 Pine Rd, San Francisco, CA', '2023-03-10');

-- Show the sensitive data (only visible to privileged roles)
SELECT * FROM sensitive_customer_data;
```

### 2.2 Create Masking Functions

```sql
-- Function to mask SSN
CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'DATA_SCIENTIST', 'ACCOUNTADMIN') 
        THEN ssn
        ELSE 'XXX-XX-' || RIGHT(ssn, 4)
    END
$$;

-- Function to mask credit card
CREATE OR REPLACE FUNCTION mask_credit_card(cc STRING)
RETURNS STRING  
LANGUAGE SQL
AS
$$
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') 
        THEN cc
        ELSE '**** **** **** ' || RIGHT(cc, 4)
    END
$$;

-- Function to mask email
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    CASE 
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'DATA_SCIENTIST', 'ACCOUNTADMIN')
        THEN email
        ELSE LEFT(email, 2) || '*****@' || SPLIT_PART(email, '@', 2)
    END
$$;

-- Function to mask salary for certain roles
CREATE OR REPLACE FUNCTION mask_salary(salary NUMBER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    CASE 
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST', 'DATA_SCIENTIST', 'ACCOUNTADMIN')
        THEN salary
        ELSE NULL
    END
$$;
```

### 2.3 Create Secure Views

```sql
-- Create secure view with masked data
CREATE OR REPLACE SECURE VIEW customer_data_secure AS
SELECT 
    customer_id,
    full_name,
    mask_email(email) as email,
    phone,
    mask_ssn(ssn) as ssn,
    mask_credit_card(credit_card) as credit_card,
    mask_salary(salary) as salary,
    address,
    created_date,
    CURRENT_ROLE() as accessed_by_role
FROM sensitive_customer_data;

-- Test the secure view with different roles
USE ROLE BUSINESS_USER;
SELECT * FROM customer_data_secure;

USE ROLE DATA_ANALYST;
SELECT * FROM customer_data_secure;

USE ROLE ACCOUNTADMIN;
SELECT * FROM customer_data_secure;
```

## 🏷️ Step 3: Data Classification and Tagging

### 3.1 Create Classification Tags

```sql
-- Switch back to ACCOUNTADMIN for tag creation
USE ROLE ACCOUNTADMIN;

-- Create tags for data classification
CREATE TAG IF NOT EXISTS data_classification
ALLOWED_VALUES ('PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED');

CREATE TAG IF NOT EXISTS pii_level
ALLOWED_VALUES ('NONE', 'LOW', 'MEDIUM', 'HIGH');

CREATE TAG IF NOT EXISTS data_owner
COMMENT = 'Identifies the business owner of the data';

-- Apply tags to tables and columns
ALTER TABLE sensitive_customer_data SET TAG data_classification = 'CONFIDENTIAL';
ALTER TABLE sensitive_customer_data SET TAG data_owner = 'Customer Success Team';

-- Tag specific columns with PII levels
ALTER TABLE sensitive_customer_data MODIFY COLUMN ssn SET TAG pii_level = 'HIGH';
ALTER TABLE sensitive_customer_data MODIFY COLUMN credit_card SET TAG pii_level = 'HIGH';
ALTER TABLE sensitive_customer_data MODIFY COLUMN email SET TAG pii_level = 'MEDIUM';
ALTER TABLE sensitive_customer_data MODIFY COLUMN phone SET TAG pii_level = 'MEDIUM';
ALTER TABLE sensitive_customer_data MODIFY COLUMN salary SET TAG pii_level = 'MEDIUM';
ALTER TABLE sensitive_customer_data MODIFY COLUMN full_name SET TAG pii_level = 'LOW';

-- View tag information
SELECT 
    table_name,
    column_name,
    tag_name,
    tag_value
FROM INFORMATION_SCHEMA.TAG_REFERENCES
WHERE object_name = 'SENSITIVE_CUSTOMER_DATA';
```

### 3.2 Tag-Based Access Policies

```sql
-- Create a view that filters based on tags
CREATE OR REPLACE VIEW pii_audit_view AS
SELECT 
    table_catalog,
    table_schema,
    table_name,
    column_name,
    tag_name,
    tag_value,
    'Review access controls' as recommendation
FROM INFORMATION_SCHEMA.TAG_REFERENCES
WHERE tag_name = 'PII_LEVEL' 
  AND tag_value IN ('HIGH', 'MEDIUM')
ORDER BY tag_value DESC, table_name, column_name;

-- View PII audit results
SELECT * FROM pii_audit_view;
```

## 🔒 Step 4: Row-Level Security

### 4.1 Create Multi-Tenant Data

```sql
-- Create table with multi-tenant structure
CREATE OR REPLACE TABLE sales_data_multitenent (
    sale_id NUMBER,
    tenant_id STRING,
    customer_id NUMBER,
    product_name STRING,
    amount NUMBER,
    sale_date DATE,
    sales_rep STRING
);

-- Insert sample data for different tenants
INSERT INTO sales_data_multitenent VALUES
(1, 'TENANT_A', 101, 'Laptop', 1200, '2024-01-15', 'John Smith'),
(2, 'TENANT_A', 102, 'Mouse', 25, '2024-01-16', 'John Smith'),
(3, 'TENANT_B', 201, 'Keyboard', 75, '2024-01-17', 'Jane Doe'),
(4, 'TENANT_B', 202, 'Monitor', 300, '2024-01-18', 'Jane Doe'),
(5, 'TENANT_C', 301, 'Tablet', 500, '2024-01-19', 'Bob Wilson'),
(6, 'TENANT_C', 302, 'Phone', 800, '2024-01-20', 'Bob Wilson');

-- Create mapping table for user-tenant relationships
CREATE OR REPLACE TABLE user_tenant_mapping (
    user_name STRING,
    tenant_id STRING,
    access_level STRING
);

INSERT INTO user_tenant_mapping VALUES
(CURRENT_USER(), 'TENANT_A', 'READ'),
(CURRENT_USER(), 'TENANT_B', 'READ'),
('ANALYST_USER', 'TENANT_A', 'READ'),
('MANAGER_USER', 'TENANT_A', 'FULL'),
('MANAGER_USER', 'TENANT_B', 'FULL');
```

### 4.2 Implement Row-Level Security

```sql
-- Create row access policy
CREATE OR REPLACE ROW ACCESS POLICY tenant_access_policy AS (tenant_id STRING) RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN TRUE
        WHEN EXISTS (
            SELECT 1 FROM user_tenant_mapping 
            WHERE user_name = CURRENT_USER() 
            AND tenant_id = tenant_id
            AND access_level IN ('READ', 'FULL')
        ) THEN TRUE
        ELSE FALSE
    END;

-- Apply row access policy to the table
ALTER TABLE sales_data_multitenent ADD ROW ACCESS POLICY tenant_access_policy ON (tenant_id);

-- Test row-level security
SELECT * FROM sales_data_multitenent;

-- Show row access policies
SHOW ROW ACCESS POLICIES;
```

## 🤝 Step 5: Data Sharing (Simulation)

### 5.1 Prepare Data for Sharing

```sql
-- Create a share-ready dataset
CREATE OR REPLACE DATABASE SHARED_ANALYTICS;
USE DATABASE SHARED_ANALYTICS;

CREATE SCHEMA PUBLIC_DATA;
USE SCHEMA PUBLIC_DATA;

-- Create aggregated, non-sensitive data suitable for sharing
CREATE OR REPLACE TABLE regional_sales_summary AS
SELECT 
    'Q' || QUARTER(sale_date) || '_' || YEAR(sale_date) as quarter,
    CASE 
        WHEN UNIFORM(1, 4, RANDOM()) = 1 THEN 'North'
        WHEN UNIFORM(1, 4, RANDOM()) = 2 THEN 'South'
        WHEN UNIFORM(1, 4, RANDOM()) = 3 THEN 'East'
        ELSE 'West'
    END as region,
    COUNT(*) as total_transactions,
    SUM(UNIFORM(100, 5000, RANDOM())) as total_revenue,
    AVG(UNIFORM(100, 5000, RANDOM())) as avg_transaction_value
FROM TABLE(GENERATOR(ROWCOUNT => 50))
GROUP BY quarter, region;

-- Create product performance data
CREATE OR REPLACE TABLE product_performance_summary AS
SELECT 
    'Product_' || UNIFORM(1, 20, RANDOM()) as product_name,
    CASE 
        WHEN UNIFORM(1, 3, RANDOM()) = 1 THEN 'Electronics'
        WHEN UNIFORM(1, 3, RANDOM()) = 2 THEN 'Clothing'
        ELSE 'Home'
    END as category,
    UNIFORM(50, 500, RANDOM()) as units_sold,
    ROUND(UNIFORM(1000, 50000, RANDOM()), 2) as revenue
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Create a secure view for sharing
CREATE OR REPLACE SECURE VIEW shareable_analytics AS
SELECT 
    r.quarter,
    r.region,
    r.total_transactions,
    r.total_revenue,
    r.avg_transaction_value,
    COUNT(p.product_name) as products_in_region
FROM regional_sales_summary r
CROSS JOIN product_performance_summary p
GROUP BY r.quarter, r.region, r.total_transactions, r.total_revenue, r.avg_transaction_value;

-- Grant access to the sharing objects
GRANT USAGE ON DATABASE SHARED_ANALYTICS TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA SHARED_ANALYTICS.PUBLIC_DATA TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA SHARED_ANALYTICS.PUBLIC_DATA TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SHARED_ANALYTICS.PUBLIC_DATA TO ROLE DATA_ANALYST;
```

### 5.2 Data Sharing Commands (Reference)

```sql
-- Note: These commands require ACCOUNTADMIN role and are for reference
-- In a real scenario, you would execute these to create actual shares

/*
-- Create a share
CREATE SHARE analytics_share
COMMENT = 'Public analytics data for external partners';

-- Grant access to database and schema to the share
GRANT USAGE ON DATABASE shared_analytics TO SHARE analytics_share;
GRANT USAGE ON SCHEMA shared_analytics.public_data TO SHARE analytics_share;

-- Grant access to specific tables/views
GRANT SELECT ON TABLE shared_analytics.public_data.regional_sales_summary TO SHARE analytics_share;
GRANT SELECT ON VIEW shared_analytics.public_data.shareable_analytics TO SHARE analytics_share;

-- Add accounts to the share (would be actual account identifiers)
ALTER SHARE analytics_share ADD ACCOUNTS = ('partner_account_1', 'partner_account_2');

-- Show shares
SHOW SHARES;
*/

-- Query to show what would be shared
SELECT 
    'SHARED_ANALYTICS' as shared_database,
    'PUBLIC_DATA' as shared_schema,
    table_name,
    table_type,
    'External partners would have read access' as access_level
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'PUBLIC_DATA';
```

## 🔍 Step 6: Security Monitoring and Auditing

### 6.1 Create Security Monitoring Views

```sql
-- Switch back to main database for monitoring
USE DATABASE LEARN_SNOWFLAKE;
USE SCHEMA SANDBOX;

-- Create security audit log
CREATE OR REPLACE VIEW security_audit_log AS
SELECT 
    user_name,
    role_name,
    execution_status,
    query_type,
    query_text,
    start_time,
    total_elapsed_time,
    bytes_scanned,
    database_name,
    schema_name
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND query_type NOT IN ('SELECT')  -- Focus on DDL/DML operations
ORDER BY start_time DESC;

-- Failed login attempts monitoring
CREATE OR REPLACE VIEW failed_access_attempts AS
SELECT 
    user_name,
    role_name,
    execution_status,
    error_code,
    error_message,
    start_time,
    query_text
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE execution_status = 'FAIL'
  AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;

-- Data access patterns
CREATE OR REPLACE VIEW data_access_patterns AS
SELECT 
    user_name,
    role_name,
    database_name,
    schema_name,
    COUNT(*) as query_count,
    MIN(start_time) as first_access,
    MAX(start_time) as last_access,
    SUM(bytes_scanned) as total_bytes_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
  AND query_type = 'SELECT'
GROUP BY user_name, role_name, database_name, schema_name
ORDER BY query_count DESC;

-- View the monitoring data
SELECT * FROM security_audit_log LIMIT 10;
SELECT * FROM data_access_patterns LIMIT 10;
```

### 6.2 Security Best Practices Implementation

```sql
-- Create procedure for regular security checks
CREATE OR REPLACE PROCEDURE security_health_check()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result STRING DEFAULT '';
    user_count NUMBER;
    role_count NUMBER;
    failed_logins NUMBER;
BEGIN
    -- Check user count
    SELECT COUNT(*) INTO user_count FROM INFORMATION_SCHEMA.USERS WHERE disabled = FALSE;
    result := result || 'Active Users: ' || user_count || '\n';
    
    -- Check role count  
    SELECT COUNT(*) INTO role_count FROM INFORMATION_SCHEMA.ROLES;
    result := result || 'Total Roles: ' || role_count || '\n';
    
    -- Check failed logins in last 24 hours
    SELECT COUNT(*) INTO failed_logins 
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
    WHERE execution_status = 'FAIL'
      AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP());
    result := result || 'Failed Queries (24h): ' || failed_logins || '\n';
    
    -- Add recommendations
    IF failed_logins > 10 THEN
        result := result || 'WARNING: High number of failed queries detected!\n';
    END IF;
    
    result := result || 'Security check completed at: ' || CURRENT_TIMESTAMP();
    
    RETURN result;
END;
$$;

-- Run security health check
CALL security_health_check();
```

## ✅ Lab Completion Checklist

- [ ] Created custom roles with appropriate permissions
- [ ] Implemented data masking functions and secure views
- [ ] Applied data classification tags to sensitive data
- [ ] Set up row-level security for multi-tenant data
- [ ] Prepared data for secure sharing
- [ ] Created security monitoring and audit views
- [ ] Implemented security health check procedures
- [ ] Applied security best practices

## 🎉 Congratulations!

You've mastered Snowflake security and data sharing! You now understand:
- Role-based access control implementation
- Data masking and secure view creation
- Data classification and tagging strategies
- Row-level security for multi-tenant environments
- Data sharing best practices
- Security monitoring and auditing

## 🔜 Next Steps

Continue with [Lab 07: Advanced Python LangChain OpenAI Assistant](../lab07/) to build:
- AI-powered natural language database queries
- Intelligent conversation interfaces with Snowflake
- Advanced file processing and analysis capabilities
- Enterprise-ready AI assistant with governance and security

Or proceed to [Lab 08: Real-World Data Engineering Project](../lab08/) for:
- Complete end-to-end data pipeline implementation
- Production-scale e-commerce analytics platform
- Advanced monitoring and alerting systems

## 🆘 Troubleshooting

### Common Issues:

**Issue**: Permission denied errors
**Solution**: Ensure you're using the correct role with appropriate privileges

**Issue**: Row-level security not working
**Solution**: Verify the row access policy logic and user mappings

**Issue**: Masking functions not applying
**Solution**: Check that the secure view is being used, not the base table

## 📚 Additional Resources

- [Snowflake Security Guide](https://docs.snowflake.com/en/user-guide/security.html)
- [Data Sharing Documentation](https://docs.snowflake.com/en/user-guide/data-sharing.html)
- [Access Control Guide](https://docs.snowflake.com/en/user-guide/security-access-control.html)