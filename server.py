#!/usr/bin/env python3

from typing import Any
from fastmcp import FastMCP
from database.config import load_config
from database.connection import create_connection, execute_read_query


mcp = FastMCP("Postgres MCP Server")

config = load_config("./config/database/postgres.yaml")
conn = create_connection(config)

@mcp.tool()
def execute_query(query: str) -> dict[str, Any]:
    """
    Execute a SQL query on the PostgreSQL database and return the results.
    
    Args:
        query: The SQL query to execute (SELECT statements only for safety)
    
    Returns:
        A dictionary containing the query results and metadata
    """

    # Execute the query
    results = execute_read_query(conn, query)
    
    if results is None:
        return {
            "success": False,
            "error": "Query returned no results or failed to execute",
            "data": None,
            "row_count": 0
        }
    
    # Convert results to a more JSON-friendly format
    if isinstance(results, list):
        row_count = len(results)
        data = results
    else:
        row_count = 1
        data = [results]
    
    return {
        "success": True,
        "data": data,
        "row_count": row_count,
        "query": query
    }


@mcp.tool()
def get_table_names(schema: str = "public") -> dict[str, Any]:
    """
    Get all table names in the specified PostgreSQL database schema.
    
    Args:
        schema: The database schema to query (default: "public")
    
    Returns:
        A dictionary containing the list of table names and metadata
    """
    try:
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}' 
        ORDER BY table_name;
        """
        
        results = execute_read_query(conn, query)
        
        if results is None:
            return {
                "success": False,
                "error": f"Failed to retrieve table names from schema '{schema}'",
                "schema": schema,
                "tables": [],
                "table_count": 0
            }
        
        # Extract table names from the results
        table_names = []
        if isinstance(results, list):
            table_names = [row[0] if isinstance(row, (tuple, list)) else row.get('table_name', row) for row in results]
        else:
            table_names = [results[0] if isinstance(results, (tuple, list)) else results.get('table_name', results)]
        
        return {
            "success": True,
            "schema": schema,
            "tables": table_names,
            "table_count": len(table_names)
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "schema": schema,
            "tables": [],
            "table_count": 0
        }


@mcp.tool()
def get_table_schema(table_name: str, schema: str = "public") -> dict[str, Any]:
    """
    Get the schema information for a specific table including columns, data types, constraints, etc.

    Args:
        table_name: The name of the table to get schema information for
        schema: The database schema to query (default: "public")

    Returns:
        A dictionary containing the table schema information
    """
    try:
        query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable, 
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table_name}';
        """
        
        results = execute_read_query(conn, query)

        if results is None:
            return {
                "success": False,
                "error": f"Failed to retrieve schema for table '{table_name}' in schema '{schema}'",
                "schema": schema,
                "table": table_name,
                "columns": []
            }

        columns = []
        for row in results:
            # Assumes each row is a tuple in the order: column_name, data_type, is_nullable, character_maximum_length
            columns.append({
                "column_name": row[0],
                "data_type": row[1],
                "is_nullable": row[2],
                "character_maximum_length": row[3]
            })

        return {
            "success": True,
            "schema": schema,
            "table": table_name,
            "columns": columns,
            "column_count": len(columns)
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "schema": schema,
            "table": table_name,
            "columns": []
        }


if __name__ == "__main__":
    mcp.run()