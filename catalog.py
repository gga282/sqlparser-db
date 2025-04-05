#catalog.py
import schema
import sys 
import os
from glob import glob
from pathlib import Path

dbfile={}
def load_all_tables():
    
    if not os.path.exists('/dbfiles'):
        raise FileNotFoundError("No db folder found")
    else:
        for name in glob.glob('/dbfiles/*.csv'):
            table_name=Path(name).stem
            dbfile[name]=schema.TableSchema(name)
            print(f"Loaded table: {name}")

    

def get_table(table_name):
    if table_name in dbfile:
        return dbfile[table_name]
    else:
        raise FileNotFoundError("Table Not Found!!")




def get_schema(table_name):
    table=get_table(table_name)
    if table:
        return table.get_schema_dict()
    else:
        raise ValueError(f"Table {table_name} not found!")