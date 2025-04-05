#parser.py
import catalog
import schema
import pandas as pd
import executor

catalog.load_all_tables()

DQLKEYWORDS=['SELECT','FROM','WHERE','AND','OR','NOT','AS']
DDLKEYWORDS=['CREATE DATABASE','DROP','ALTER','TRUNCATE','LIST DATABASE']
DMLKEYWORDS=['INSERT INTO','UPDATE','DELETE','VALUES','SET']
KEYWORDS=['FROM','WHERE','AND','OR','NOT','AS']
EQKEYWORDS=['>','<','>=','<=','!=']
#AST 
token_keys=['type','table','column','where','eqOP','secVAR']
token_dict={}

def user_query_input(query):
    query=query.strip().split()
    tokens=query.split()

    if 'SELECT' in tokens:
        if tokens[0]=='SELECT':
            dql_query(tokens)
        else:
            print('Wrong command')
    elif DDLKEYWORDS in tokens:
        if tokens[0] in ('CREATE','DROP','ALTER','TRUNCATE'):
            ddl_query(tokens)
        else:
            print('Wrong command')
    elif DMLKEYWORDS in tokens:
        if tokens[0] in ('INSERT','UPDATE','DELETE'):
            dml_query(tokens)
        else:
            print('Wrong command')
    else:
        print('Wrong command')

    
def dql_query(query):
    #tokens[1] should be col name - need to get this info from table or schema
    #tokens[] should be table name
    i=0
    newOrder=[]

    if 'FROM' in query:
        fromIndex=query.index('FROM')
        tableIndex=fromIndex+1
        newOrder[i]=query[fromIndex]
        i+=1
        newOrder[i]=query[tableIndex]
        i+=1
    if 'WHERE' in query:
        #After this i have to add loop in order to match with eq. operators.
        whereIndex=query.index('WHERE')
        cond_col_index=whereIndex+1
        newOrder[i]=query[cond_col_index]
        i+=1
        op_index=cond_col_index+1
        newOrder[i]=query[op_index]
        i+=1
        col_name=query[cond_col_index]
        col_types=schema.get_column_type()
        col_type=col_types[col_name]
        newOrder[i]=query[colIndex+1]
        i+=1
        val_index=cond_col_index+2
        newOrder[i]=query[val_index]


#        if query[colIndex+1] in EQKEYWORDS:
#            if query[colIndex+1]=='<':
#                if query[colIndex]<query[colIndex+2]:
#            elif query[colIndex+1]=='>':
#                if query[colIndex]>query[colIndex+2]:
                #do it later
#            elif query[colIndex+1]=='=':
#                if query[colIndex]==query[colIndex+2]:
                #do it later
#            elif query[colIndex+1]=='>=':
#                if query[colIndex]>=query[colIndex+2]:
                #do it later
#            elif query[colIndex+1]=='<=':
#                if query[colIndex]<=query[colIndex+2]:
                #do it later
#            elif query[colIndex+1]=='!=':
#                if query[colIndex]!=query[colIndex+2]:
                #do it later
#            else:
#                print('WRONG operator',query[colIndex+1],' not exist')

    if 'SELECT' in query:
        selectIndex=query.index('SELECT')
        colIndex=selectIndex+1
        newOrder[i]=query[selectIndex]
        i+=1
        newOrder[i]=query[colIndex]

    token_dict["type"]="SELECT"
    token_dict["table"]=query[tableIndex]
    token_dict["column"]=query[colIndex]
    token_dict["where"]=col_name
    token_dict["eqOP"] = query[colIndex + 1]
    token_dict["secVAR"]=parse_literal(query[val_index],col_type)


    executor.execute_select(token_dict)


       
def ddl_query(query):
    #tokens[1] should be col name - need to get this info from table or schema
    i=0
    newOrder=[]
    if 'CREATE' in query:
        createIndex=query.index('CREATE')
        dbName=query[createIndex+2]
        token_dict["type"]="CREATE DATABASE"
        token_dict["table"]=dbName
        executor.execute_create(token_dict)
        #check if DB exist or not
    if 'DROP' in query:
        dropIndex=query.index('DROP')
        dbName=query[dropIndex+1]
        token_dict["type"]="DROP"
        token_dict["table"]=dbName
        executor.execute_drop(token_dict)
        #check if DB exist or not
    if 'ALTER' in query:
        alterIndex=query.index('ALTER')
        dbName=query[alterIndex+1]
        #check if DB exist or not
    if 'TRUNCATE' in query:
        trunIndex=query.index('TRUNCATE')
        dbName=query[trunIndex+1]
        token_dict["type"]="TRUNCATE"
        token_dict["table"]=dbName
        executor.execute_drop(token_dict)
        #check if DB exist or not
    if 'LIST DATABASE' in query:
        #show all databases, list them.




def dml_query(query):
    #tokens[1] should be col name - need to get this info from table or schema
    values=[]
    token_dict["type"]="INSERT INTO"
    if 'INSERT' in query:
        insertIndex=query.index('INSERT')
        tableName=query[insertIndex+2]
        token_dict["table"]=tableName
        colIndex=query.index('(')
        colIndexEnd=query.index(')')
        token_dict["column"]=token_dict[colIndex:colIndexEnd]
        valIndex=query.index('VALUES')
        val_index_start=query.index('(')
        val_index_ends=query.index(')')
        single_or_multi=val_index_ends-val_index_ends
        if single_or_multi>1:
            values.append([val for val in query[val_index_start:val_index_ends]])
        else:
            values.append(query[val_index_start:val_index_ends])
            
        token_dict["values"]=values
        executor.execute_insert(token_dict)
        #schema.get_columns(query[])
    if 'UPDATE' in query:
        updateIndex=query.index('UPDATE')
        tableIndex=updateIndex+1
        setIndex=query.index('SET')
        colIndex=setIndex+1
        opIndex=query.index('=')
        rightVal=opIndex+1

        token_dict["set_column"]=query[colIndex]
        token_dict["set_value"]=query[rightVal]
        if 'WHERE' in query:
            whereIndex=query.index('WHERE')
            token_dict["where"]=where_clause(query,whereIndex)
        executor.execute_update(token_dict)

             


    if 'DELETE' in query:
        delIndex=query.index('DELETE')
        tableIndex=query.index('FROM')

        if 'WHERE' in query:
            whereIndex=query.index('WHERE')
            token_dict["where"]=where_clause(query,whereIndex)
            token_dict["column"]=query[whereIndex+1]
            token_dict["eqOP"]=query[whereIndex+2]
            



        executor.execute_delete()
        

def where_clause(query,start_index):
    cond_col_index=query[start_index+1]
    op_index=query[cond_col_index+1]
    val_index = query[op_index+1]

    return {"column":cond_col_index,
            "operator":op_index,
            "value":val_index}





"""
def parse_literal_primitive(secValue):
    #for finding datatype of the righ variable but idk i think it would be much easier,readable first getting datatype of first var and convert.
    if secValue.isdigit():
        return int(secValue)
    else:
        if '.' in secValue:
            indexValueAfteDot=secValue.index('.')
            valueAfterDot=secValue[indexValueAfteDot:]
            secValue=int(secValue)
            secValue+=float(valueAfterDot)
            return secValue
        else:
            if (secValue.startswith('"') and secValue.endswith('"')) or (secValue.startswith("'") and secValue.endswith("'")):
                return secValue[1:-1]
            else:
                if secValue=="TRUE":
                    return 1
                elif secValue=="FALSE":
                    return 0
                else:
                    if secValue==1:
                        return "TRUE"
                    elif secValue==0:
                        return "FALSE"
                    else:
                        raise Exception(f"INVALID LITERAL {secValue}")"
                        """

def parse_literal(secValue,col_type):
    if "int" in col_type:
        return int(secValue)
    elif "float" in col_type:
        indexValueAfterDot=secValue.index()
        valueAfterDot=secValue[indexValueAfterDot:]
        secValue=int(secValue)
        secValue+=float(valueAfterDot)
        return secValue
    elif "object" in col_type:
        if (secValue.startswith('"') and secValue.endswith('"')) or (secValue.startswith("'") and secValue.endswith("'")):
            return secValue[1:-1]
        else:
            return secValue
    elif "string" in col_type:
        return secValue[1:-1]
    elif "bool" in col_type:
        return "TRUE" if secValue == 1 else "FALSE"
    elif "datetime" in col_type:
        try:
            return pd.to_datetime(secValue,format="%D-%M-%Y")
        except Exception:
            raise Exception(f"INVALID DATETIME FORMAT for {secValue}")
    elif "timedelta" in col_type:
        try:
            return pd.to_timedelta(secValue)
        except Exception:
            raise Exception(f"INVALID TIMEDELTA FORMAT for {secValue}")
    else:
        raise Exception(f"INVALID LITERAL {secValue}")



                    