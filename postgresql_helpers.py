import datetime


def create_postgresql_upsertion_queries(
        table_name: str,
        column_names: list[str],
        values: list[tuple],
        rows_per_query: int = 10,
        update_columns: list[str] = [],
        formatted: bool = True
) -> list[str]:
    query_template: str = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES "
    starting_row: int = 0
    queries: list[str] = []
    
    while starting_row < len(values):
        query: str = query_template
    
        for row in values[starting_row:starting_row + rows_per_query]:
            if len(row) != len(column_names):
                print(f"Length of row did not equal number of columns. ({row})")
                continue
                
            if formatted:
                query += "\n\t"
        
            query += "("
            
            for value in row:
                if type(value) == datetime.date:
                    value = str(value)
                
                query += f"{repr(value)}, "
            
            query = f"{query[:-2]}), "
        
        query = query[:-2]
        
        if formatted:
            query += "\n"
        
        query += f"ON CONFLICT ON CONSTRAINT pk_{table_name} DO "
        
        if len(update_columns) <= 0:
            query += "NOTHING;"
        else:
            query += "UPDATE SET "
            
            for column in update_columns:
                if formatted:
                    query += "\n\t"
                
                query += f"{column} = EXCLUDED.{column},"
            
            query = f"{query[:-1]};"
        
        queries.append(query)
        starting_row += rows_per_query
    
    return queries