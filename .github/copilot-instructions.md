# Copilot Instructions for Learn Snowflake Repository

This repository contains step-by-step Snowflake learning labs designed to teach data cloud concepts from beginner to advanced levels.

## Repository Structure

This is an educational repository organized into sequential labs:

- **Lab directories** (`lab01/`, `lab02/`, etc.) - Each contains a complete learning module
- **README.md files** - Provide step-by-step instructions for each lab
- **SQL scripts** - Located in `sql/` subdirectories within labs
- **Python code** - Located in `python/` subdirectories within labs
- **Sample data** - Located in `data/` subdirectories where applicable

## Code Standards

### SQL Code
- Use **uppercase** for SQL keywords (SELECT, FROM, WHERE, etc.)
- Use **snake_case** for table and column names
- Include clear comments explaining complex queries
- Follow Snowflake-specific syntax and best practices
- Use proper indentation for readability
- Include database/schema context (USE DATABASE, USE SCHEMA)

### Python Code
- Follow PEP 8 style guidelines
- Use meaningful variable names
- Include docstrings for functions and classes
- Use the Snowflake Connector for Python when connecting to Snowflake
- Handle credentials securely (use environment variables)
- Include error handling for database connections

### Markdown Documentation
- Use clear headings with emoji icons (🎯, 🚀, 📊, etc.)
- Include estimated completion times for labs
- Add troubleshooting sections for common issues
- Use code blocks with proper syntax highlighting
- Include step-by-step instructions with numbered lists
- Add "Prerequisites" and "Objectives" sections

## Lab Structure Pattern

Each lab should follow this consistent structure:

```markdown
# Lab XX: [Topic Name]

## 🎯 Objectives
## ⏱️ Estimated Time: XX minutes
## 📋 Prerequisites
## 🚀 Step 1: [Main Steps]
## 🔍 Step 2: [Additional Steps]
## ✅ Lab Completion Checklist
## 🎉 Congratulations!
## 🔜 Next Steps
## 🆘 Troubleshooting
## 📚 Additional Resources
```

## Snowflake-Specific Guidelines

### Database Objects
- Use descriptive names: `LEARN_SNOWFLAKE` database, `SANDBOX` schema
- Create warehouses with appropriate sizing (start with X-SMALL)
- Use `LEARN_WH` as the standard warehouse name for consistency
- Always include warehouse management (SUSPEND/RESUME) examples

### Query Patterns
- Always set context at the beginning of scripts:
  ```sql
  USE WAREHOUSE LEARN_WH;
  USE DATABASE LEARN_SNOWFLAKE;
  USE SCHEMA SANDBOX;
  ```
- Include examples of Snowflake-specific functions (VARIANT data, time travel, etc.)
- Demonstrate best practices for credit management
- Show both UI and SQL approaches when applicable

### Data Loading
- Use internal stages for simplicity in learning labs
- Demonstrate CSV, JSON, and Parquet formats
- Include error handling and data validation examples
- Show both bulk loading and streaming scenarios

## Educational Focus

### Learning Progression
- **Beginner (Labs 1-3)**: Account setup, basic SQL, data loading
- **Intermediate (Labs 4-6)**: Advanced functions, Python integration, security
- **Advanced (Labs 7-8)**: Performance optimization, real-world projects

### Code Examples
- Keep examples realistic but simple enough for learning
- Use business-relevant scenarios (sales data, customer analytics, etc.)
- Include both working examples and common mistakes to avoid
- Provide sample datasets that are meaningful but not overwhelming

### Documentation Style
- Write for learners who may be new to Snowflake or data warehousing
- Include context and explanation, not just code
- Use consistent terminology throughout all labs
- Add visual elements (tables, diagrams) when helpful

## File Naming Conventions

- Lab directories: `lab01/`, `lab02/`, etc.
- SQL files: `snake_case.sql` (e.g., `setup_environment.sql`)
- Python files: `snake_case.py` (e.g., `snowflake_connection.py`)
- Data files: descriptive names with format extension (e.g., `sample_sales.csv`)

## Best Practices for Contributors

- Test all code examples in a real Snowflake environment
- Include cleanup instructions to help learners manage credits
- Provide troubleshooting tips for common issues
- Link to official Snowflake documentation for deeper learning
- Keep content current with latest Snowflake features and syntax
- Consider different learning styles (visual, hands-on, conceptual)

## Common Patterns to Maintain

### Error Handling
```sql
-- Always include context checks
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
```

### Credit Management
```sql
-- Suspend warehouses when done
ALTER WAREHOUSE LEARN_WH SUSPEND;
```

### Data Validation
```sql
-- Include count checks after operations
SELECT COUNT(*) FROM table_name;
```

When working on this repository, prioritize educational clarity and consistency across all labs while maintaining technical accuracy and current best practices for Snowflake development.