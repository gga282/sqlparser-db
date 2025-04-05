#main.py
import parser 
import sys 
import os
from pathlib import Path
from glob import glob

def initialize_db_directory():
    dbfile=[]
    if not os.path.exists('/dbfiles'):
        os.makedirs('/dbfiles')

    print("Available tables:")
    for name in glob.glob('/dbfiles/*.csv'):
        dbfile.append(name)

    for name in dbfile:
        print(name)


initialize_db_directory()

while True:
    user_input=input("db> ")
    if user_input.lower() in ("exit","quit"):
        break
    try:
        parser.user_query_input(user_input)
    except Exception as e:
        print(f"Error: {e}")


