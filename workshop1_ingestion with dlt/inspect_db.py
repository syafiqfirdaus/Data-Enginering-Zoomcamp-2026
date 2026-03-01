import duckdb

conn = duckdb.connect('taxi_pipeline.duckdb')
print('schemas', conn.sql("SELECT schema_name FROM information_schema.schemata").fetchall())
print('tables', conn.sql("SELECT table_schema, table_name FROM information_schema.tables").fetchall())
