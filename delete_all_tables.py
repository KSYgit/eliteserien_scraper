import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('data/eliteserien_data.db') #path to database
cursor = conn.cursor()

# Get the names of all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# Drop each table, skipping 'sqlite_sequence'
for table_name in tables:
    table_name = table_name[0]  # Get the table name from the tuple
    if table_name != 'sqlite_sequence':  # Skip the internal table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Deleted table: {table_name}")

# Commit changes and close connection
conn.commit()
conn.close()

print("All non-internal tables have been deleted.")
