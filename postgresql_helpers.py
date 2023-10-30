import datetime

from typing import Union


def create_postgresql_upsertion_queries(
        table_name: str,
        column_names: list[str],
        values: list[tuple],
        rows_per_query: int = 10,
        update_columns: list[str] = [],
        formatted: bool = True,
        constraint_name: Union[str, None] = None
) -> list[str]:
    queries: list[str] = []
    starting_row: int = 0
    columns: str = ", ".join(column_names)
    constraint: str = f"pk_{table_name}"
    formatted_splitter: str = ", \n\t" if formatted else ", "
    print(formatted_splitter)
    
    if constraint_name is not None:
        constraint = constraint_name
    
    conflict_action: str = "NOTHING"
    
    if len(update_columns) > 0:
        conflict_action = "UPDATE SET "
        
        if formatted:
            conflict_action += "\n\t"
        
        conflict_action += formatted_splitter.join([
            f"{update_column} = EXCLUDED.{update_column}"
            for update_column in update_columns
        ])
    
    while starting_row < len(values):
        row_batch: list[tuple] = values[starting_row:starting_row + rows_per_query]
        values_string: str = formatted_splitter.join([f"({', '.join([repr((str(value) if type(value) == datetime.date else value)) for value in row])})" for row in row_batch])
        
        if formatted:
            values_string = f"\n\t{values_string}\n"
        
        query: str = f"INSERT INTO {table_name} ({columns}) VALUES {values_string} ON CONFLICT ON CONSTRAINT {constraint} DO {conflict_action};"
        queries.append(query)
        starting_row += rows_per_query
    
    return queries