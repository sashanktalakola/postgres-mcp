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


if __name__ == "__main__":
    mcp.run(transport="streamable-http")