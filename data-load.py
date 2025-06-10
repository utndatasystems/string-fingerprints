import re
import os
import utils

DATA_DIR = 'metadata/imdb/'
SCHEMA_FILE = os.path.join(DATA_DIR, 'schema.sql')
LOADER_FILE = os.path.join(DATA_DIR, 'load.sql')

# The data name.
data_name = os.path.basename(os.path.normpath(DATA_DIR))
print(f'data_name={data_name}')

# The database file.
if not os.path.isdir('dbs'):
  os.mkdir('dbs')
DB_FILE = f'dbs/{data_name}.duckdb'

# Remove the existing database file if it exists (just to be sure we don't use old stuff).
if os.path.isfile(DB_FILE):
  os.remove(DB_FILE)

con = utils.open_duckdb(db_path=DB_FILE, read_only=False, num_threads=os.cpu_count())

create_stmts = open(SCHEMA_FILE, 'r').read()

# Gather the stmts.
table_names = re.findall(r'CREATE TABLE\s+(\w+)', create_stmts, re.IGNORECASE)
drop_tables = '\n'.join([f'DROP TABLE IF EXISTS {table};' for table in table_names])
load_stmts = open(LOADER_FILE, 'r').read()

# And run.
import time
start_time = time.time_ns()
con.execute(drop_tables)
con.execute(create_stmts)

for command in load_stmts.split(';'):
  command = command.strip()
  if command:
    print(f'command={command}')
    con.execute(command)
end_time = time.time_ns()

print(con.execute("SHOW TABLES").fetchall())
con.close()


