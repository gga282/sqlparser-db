#catalog.py
import schema
import sys 
import os
from glob import glob
from pathlib import Path

# Global dictionary to store table objects
dbfile = {}

def load_all_tables():
    if not os.path.exists('dbfiles'):
        raise FileNotFoundError("No db folder found")
    else:
        for name in glob('dbfiles/*.csv'):
            table_name = Path(name).stem
            dbfile[table_name] = schema.TableSchema(name)
            print(f"Loaded table: {table_name}")

def get_table(table_name):
    if table_name in dbfile:
        return dbfile[table_name]
    else:
        raise FileNotFoundError(f"Table {table_name} Not Found!!")

def get_schema(table_name):
    table = get_table(table_name)
    if table:
        return table.get_schema_dict()
    else:
        raise ValueError(f"Table {table_name} not found!")
