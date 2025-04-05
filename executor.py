import pandas as pd
from pathlib import Path
import os
import schema

EQKEYWORDS=['>','<','>=','<=','!=']

def executor_query():
    #tokens and AST will be send here and SQL execution will be done here


def execute_select(query,colIndex):
    # we have to check columns if exist or not, will be done in schema and executor
    table=schema.TableSchema("dbfiles/" + query["table"] + ".csv")
    
    query["table"]
    query["column"]
    query["where"]
    query["eqOP"]
    query["secVAR"]

    if query["where"]:
        if query["eqOP"] in EQKEYWORDS:
            if query["eqOP"]=='<':
                return table.df[table.df[query["column"]]<query["secVAR"]]    
            elif query["eqOP"]=='>':
                return table.df[table.df[query["column"]]<query["secVAR"]]
            elif query["eqOP"]=='=':
                return table.df[table.df[query["column"]]==query["secVAR"]]
            elif query["eqOP"]=='>=':
                return table.df[table.df[query["column"]]>=query["secVAR"]]
            elif query["eqOP"]=='<=':
                return table.df[table.df[query["column"]]<=query["secVAR"]]
            elif query["eqOP"]=='!=':
                return table.df[table.df[query["column"]]!=query["secVAR"]]
            else:
                print('WRONG operator',query["eqOP"],' not exist')

def execute_create(query):
    table_name=query["table"]
    if table_exists(table_name):
        raise Exception(f"Table {table_name} already exists!")
    else:
        #CREATE TABLE CODES will be here
        if table_name:
            

def execute_drop(query):
    table_name=query["table"]
    path=os.path.join("dbfiles",table_name+".csv")
    if table_exists(table_name):
        os.remove(path) 
    else:
        raise Exception(f"Table {table_name} not existed!")

def execute_truncate(query):
    table_name=query["table"]
    path=os.path.join("dbfiles",table_name+".csv")
    if  table_exists(table_name):
        df=pd.read_csv(path)
        df.iloc[0:0].to_csv(path,index=False)
    else:
        raise Exception(f"Table {table_name} not existed!")
    

def execute_insert(query):
    table_name=query["table"]


def table_exists(table_name):
    path=os.path.join("dbfiles",table_name+".csv")
    return os.path.exists(path)

def execute_update(query):
    table_name=query["table"]
    set_column=query["set_column"]
    set_value=query["set_value"]

def execute_delete(query):

