from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


class DatabaseManager:
    def __init__(self, table_creation_queries=None):
        self.table_creation_queries = table_creation_queries
        self.sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')


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

    def insert(self, data, table):
        self.sqlite_hook.insert_rows(table=table, rows=[data])

    def insert_many(self, data, table):
        self.sqlite_hook.insert_rows(table=table, rows=data)


