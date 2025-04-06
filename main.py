#main.py
import parser 
import sys 
import os
from pathlib import Path
from glob import glob
import catalog

def initialize_db_directory():
    # Create dbfiles file if it doesnt exist
    if not os.path.exists('dbfiles/'):
        os.makedirs('dbfiles/')
        print("Created database directory")

    # Print available tables
    print("Available tables:")
    table_files = glob('dbfiles/*.csv')
    
    if not table_files:
        print("  No tables found")
    else:
        for name in table_files:
            print(f"  {Path(name).stem}")

# Initialize database directory first
initialize_db_directory()

#load existing tables
try:
    catalog.load_all_tables()
except Exception as e:
    print(f"Error loading tables: {e}")

print("Simple SQL Database System")
print("Type 'exit' or 'quit' to exit")

while True:
    try:
        user_input = input("db> ")
        if user_input.lower() in ("exit", "quit"):
            print("Goodbye!")
            break
        parser.user_query_input(user_input)
    except Exception as e:
        print(f"Error: {e}")
