#!/usr/bin/env python3
"""
Simple FastAPI Server for Snowflake Data
Uses connection string configuration
"""
import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict, Any
import uvicorn

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))
from snowflake_connection import get_snowflake_connection, get_connection_info

app = FastAPI(
    title="Snowflake Data API",
    description="Simple REST API for querying Snowflake data using connection string",
    version="1.0.0"
)

# Response models
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    connection_config: Dict[str, Any]

class Employee(BaseModel):
    id: int
    name: str
    department: str
    salary: int
    hire_date: str

class QueryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    row_count: int
    message: str

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint with connection info."""
    try:
        config = get_connection_info()
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(),
            connection_config=config
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.get("/employees", response_model=List[Employee])
async def get_employees():
    """Get all employees from Snowflake."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ID, NAME, DEPARTMENT, SALARY, HIRE_DATE 
            FROM EMPLOYEES 
            ORDER BY ID
        """)
        results = cursor.fetchall()
        
        employees = []
        for row in results:
            employees.append(Employee(
                id=row[0],
                name=row[1],
                department=row[2],
                salary=row[3],
                hire_date=str(row[4])
            ))
        
        cursor.close()
        return employees
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching employees: {str(e)}")

@app.get("/employees/{employee_id}", response_model=Employee)
async def get_employee(employee_id: int):
    """Get a specific employee by ID."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ID, NAME, DEPARTMENT, SALARY, HIRE_DATE 
            FROM EMPLOYEES 
            WHERE ID = %s
        """, (employee_id,))
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        employee = Employee(
            id=result[0],
            name=result[1],
            department=result[2],
            salary=result[3],
            hire_date=str(result[4])
        )
        
        cursor.close()
        return employee
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching employee: {str(e)}")

@app.get("/departments")
async def get_departments():
    """Get all departments with employee counts."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DEPARTMENT, 
                   COUNT(*) as employee_count,
                   AVG(SALARY) as avg_salary,
                   MIN(SALARY) as min_salary,
                   MAX(SALARY) as max_salary
            FROM EMPLOYEES 
            GROUP BY DEPARTMENT
            ORDER BY DEPARTMENT
        """)
        results = cursor.fetchall()
        
        departments = []
        for row in results:
            departments.append({
                "department": row[0],
                "employee_count": row[1],
                "avg_salary": float(row[2]),
                "min_salary": row[3],
                "max_salary": row[4]
            })
        
        cursor.close()
        return departments
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching departments: {str(e)}")

@app.get("/tables")
async def get_tables():
    """Get all available tables."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name, table_type, row_count, comment
            FROM information_schema.tables 
            WHERE table_schema = CURRENT_SCHEMA()
            ORDER BY table_name
        """)
        results = cursor.fetchall()
        
        tables = []
        for row in results:
            tables.append({
                "table_name": row[0],
                "table_type": row[1],
                "row_count": row[2],
                "comment": row[3]
            })
        
        cursor.close()
        return tables
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tables: {str(e)}")

@app.post("/query", response_model=QueryResponse)
async def execute_query(query: str):
    """Execute a custom SQL query (SELECT only for security)."""
    try:
        # Basic security check
        query_upper = query.upper().strip()
        if not query_upper.startswith('SELECT'):
            raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        # Convert results to dictionaries
        data = []
        for row in results:
            row_dict = {}
            for i, value in enumerate(row):
                row_dict[columns[i]] = value
            data.append(row_dict)
        
        cursor.close()
        
        return QueryResponse(
            success=True,
            data=data,
            row_count=len(data),
            message=f"Query executed successfully, returned {len(data)} rows"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {str(e)}")

if __name__ == "__main__":
    print("*** Starting Simple Snowflake Data API Server ***")
    print(f"Connection config: {get_connection_info()}")
    print("*** Available endpoints:")
    print("  - GET  /health          - Health check with connection info")
    print("  - GET  /employees       - Get all employees")
    print("  - GET  /employees/{id}  - Get specific employee")
    print("  - GET  /departments     - Get departments with stats")
    print("  - GET  /tables          - Get available tables")
    print("  - POST /query           - Execute custom SELECT query")
    print("*** Starting server on http://localhost:8080")
    print("*** API docs available at http://localhost:8080/docs")
    
    uvicorn.run(app, host="localhost", port=8080)