from airflow.providers.sqlite.operators.sqlite import SqliteOperator

class TableCreator:
    def __init__(self, table_creation_queries):
        self.table_creation_queries = table_creation_queries

    def create_tables(self):
        create_tables = []
        for query_number, query in enumerate(self.table_creation_queries):
            task_id = f"create_table_{query_number}"
            create_table = SqliteOperator(
                task_id=task_id,
                sqlite_conn_id="sqlite_default",
                sql=query
            )
            create_tables.append(create_table)
        return create_tables