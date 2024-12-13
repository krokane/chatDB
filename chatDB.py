#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 13:05:31 2024

@author: kevin
"""

import argparse
import logging
import nltk
import pandas as pd
import psycopg2 as psy
import os
import random
import re
import sys
import time
from collections import defaultdict
from nltk.corpus import stopwords
from nltk.stem import LancasterStemmer, PorterStemmer, WordNetLemmatizer
from nltk.tokenize import word_tokenize
from sqlalchemy import create_engine, text


nltk.download('wordnet')
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')

chosen_query = 'Query: SELECT * FROM coffee_shop_sales LIMIT 5;'

MAX = ['maximum','max','high','highest','most', 'top', 'peak']
MIN = ['minimum', 'min', 'low', 'lowest', 'least', 'bottom', 'smallest', 'fewest']
SUM = ['total', 'all', 'sum', 'aggregate', 'whole', 'entirety', 'entire', 'complete', 'grand total']
COUNT = ['count', 'many', 'number', 'quantity', 'tally', 'amount']
AVG = ['average', 'middle', 'mean', 'median', 'norm', 'standard', 'typical']

HAVING = ['having', 'with', 'group filter', 'aggregate filter', 'group condition']
GROUP_BY = ['by','group by', 'group', 'aggregation', 'bucket', 'category', 'per']
ORDER_BY = ['order by', 'sort', 'arrange', 'rank', 'sequence', 'direction']
LIMIT = ['limit', 'rows']
WHERE = ['filter','where', 'include','condition', 'filter', 'criteria', 'predicate', 'restriction', 'in', 'located']

GREATER = ['greater', 'great']
LESS = ['less']

#coffee
unit_price = ["price", "cost", "amount", "value", "rate"]
transaction_qty = ["quantity", "number", "count"]
transaction_id = ["ID", "reference", "number", "code", "identifier", "transactions", "transaction"]
transaction_date = ["date", "day", "time", "moment", "period", "timestamp", "recent"]
store_location = ["store", "location", "place", "address", "region", "site", "area", "city", "zip", "code"]
product_category = ["category", "type", "group", "class", "division", "section", "product"]
sales_amount = ["sales", "total", "revenue", "earnings", "income", "selling", "sell"]

NLP_library = {
    'unit_price': unit_price,
    'transaction_qty': transaction_qty,
    'transaction_id': transaction_id,
    'transaction_date': transaction_date,
    'store_location': store_location,
    'product_category': product_category,
    'sales_amount': sales_amount,
    'MAX': MAX,
    'MIN': MIN,
    'SUM': SUM,
    'COUNT': COUNT,
    'AVG': AVG,
    'HAVING': HAVING,
    'GROUP_BY': GROUP_BY,
    'ORDER_BY': ORDER_BY,
    'LIMIT': LIMIT,
    'WHERE': WHERE,
    'GREATER': GREATER,
    'LESS': LESS
}

postgres_connection = "postgresql+psycopg2://kevin:@localhost:5432/chatdb"

#Capability 4 -- Add data to PostgreSQL database
def upload_file_postgres(file, postgres_connection=postgres_connection):
    try:
        if file.endswith('.xlsx') or file.endswith('.xls'):
            data = pd.read_excel(file)
            file2 = './excel_to_csv.csv'
            data.to_csv(file2, index=False)
        elif file.endswith('.csv'):
            file2 = file
        else: raise ValueError('Unsupported File Type')
    
        table_name = os.path.basename(file).split('.')[0]
        table_name = table_name.replace(" ", "_").lower()
        engine = create_engine(postgres_connection)
        data = pd.read_csv(file2)
        
        data.to_sql(name=table_name, con=engine, if_exists = 'replace', index = False)
        print(f"Data upload successful! Table '{table_name}' created in the database.")
        
    except Exception as e:
        print("Connection failed:", e)
#upload_file_postgres('/Users/kevin/Desktop/DSCI551/ChatDB/Data/Artists.csv')


#Interface No 5 -- Get metadata about a table on the PostgreSQL server
def get_tables(postgres_connection=postgres_connection):
    engine = create_engine(postgres_connection)
    with engine.connect() as connection:
        result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"))
        tables = result.fetchall()
        table_set = tuple(table[0] for table in tables)
    connection.close()
    return table_set
#get_tables()

def get_columns(tables=get_tables(), for_sql=False, postgres_connection=postgres_connection):
    engine = create_engine(postgres_connection)
    with engine.connect() as connection:
        if len(tables) == 1:
            columns = connection.execute(text(f'SELECT table_name, column_name FROM information_schema.columns WHERE table_name = \'{tables[0]}\' ORDER BY table_name;'))
        elif (type(tables) == str) == True:
            columns = connection.execute(text(f"SELECT table_name, column_name FROM information_schema.columns WHERE table_name = '{tables}' ORDER BY table_name;"))
        else:
            columns = connection.execute(text(f'SELECT table_name, column_name FROM information_schema.columns WHERE table_name IN {tables} ORDER BY table_name;'))
        result_dict = defaultdict(list)
        result_list = columns.fetchall()
        for table_name, columns_name in result_list:
            result_dict[table_name].append(columns_name)
    connection.close()
    result_list = []
    if for_sql == True:
        return result_dict[tables]
    else:
        for table, columns in result_dict.items():
            result_list.append(f"{table}: {', '.join(columns)}")
            return result_list
#get_columns()

def get_metadata(postgres_connection=postgres_connection):
    response = input(f'Which data tables would you like metadata for? Select from below, seperated by a comma: \n {get_tables()} \n').split(',')
    for i in range(len(response)): response[i] = response[i].strip()
    response = tuple(response)
    
    engine = create_engine(postgres_connection)
    with engine.connect() as connection:
        for db in response:
            result = connection.execute(text(f"SELECT * FROM {db} LIMIT 5;"))
            print(f'\nTable: {db}')
            pd.set_option('display.max_columns', 5)
            df = (pd.DataFrame(result.fetchall())).head(5)
            print(f'\nColumns: {list(df.columns)}')
            print(f'\n{df.head()}')
    return 
#get_metadata()

#Interface No 2 -- Get sample queries from database
def get_agg_variables(table, postgres_connection=postgres_connection):
    engine = create_engine(postgres_connection)
    with engine.connect() as connection:
        columns = connection.execute(text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY data_type;"))
        result_list = columns.fetchall()    
    
    non_agg_list = []
    agg_list = []

    for i in range(len(result_list)):
        if result_list[i][1] == 'text': non_agg_list.append(result_list[i][0])
        elif result_list[i][0].find('_id') != -1: non_agg_list.append(result_list[i][0])
        elif result_list[i][0].find('_ID') != -1: non_agg_list.append(result_list[i][0])
        elif result_list[i][0].find('ID') != -1: non_agg_list.append(result_list[i][0])
        else: agg_list.append(result_list[i][0])
    return non_agg_list, agg_list
#get_agg_variables('coffee_shop_sales')

def gen_random_values(database = 'coffee_shop_sales', postgres_connection=postgres_connection):
    engine = create_engine(postgres_connection)
    with engine.connect() as connection:
        columns = connection.execute(text(f"SELECT * FROM {database} ORDER BY RANDOM() LIMIT 100;"))
    df = pd.DataFrame(columns.fetchall())
    if df.dropna().empty:
        df = df.iloc[0:1]
    else:
        df = df.dropna()
        df = df.iloc[0:1]
    return df
#gen_random_values()

def run_sql_query(query=chosen_query, postgres_connection=postgres_connection):
    query = query.split(':',1)[1]
    query = query.replace('\n','').strip()
    engine = create_engine(postgres_connection)
    with engine.connect() as connection:
        result = connection.execute(text(query))
        df = pd.DataFrame(result.fetchall())
    return df
#run_sql_query()

def gen_sql_query(construct, database='coffee_shop_sales',print_all=True):
    global query1
    global query2
    global query3
    global chosen_query
    
    if construct == 'where' or construct == '1':
        columns = get_columns(database, for_sql=True)
        columns = random.sample(columns,7)
        random_values_df = gen_random_values(database)
        columns_clean = []
        for i in range(len(columns)):
            name = columns[i].strip().split('_')
            name = ' '.join(name)
            columns_clean.append((re.sub(r'([a-z])([A-Z]+)', r'\1 \2', name)).capitalize())
        query1 = f'''{columns_clean[0]} and {columns_clean[1]}, filtered by {columns_clean[1]}: \nSELECT "{columns[0]}", "{columns[1]}" \nFROM {database} \nWHERE "{columns[1]}" = '{random_values_df[columns[1]][0]}';\n'''
        query2 = f'''{columns_clean[2]} and {columns_clean[3]}, filtered by {columns_clean[3]}: \nSELECT "{columns[2]}", "{columns[3]}" \nFROM {database} \nWHERE "{columns[3]}" = '{random_values_df[columns[3]][0]}';\n'''
        query3 = f'''{columns_clean[4]} and {columns_clean[5]}, filtered by {columns_clean[6]}: \nSELECT "{columns[4]}", "{columns[5]}" \nFROM {database} \nWHERE "{columns[6]}" = '{random_values_df[columns[6]][0]}';\n'''
        print(query1)
        if print_all == True:
            print(query2)
            print(query3)
            chosen_query = input('Would you like to run any of these queries? If yes, input 1, 2, or 3.\n')
            chosen_query = 'query' + chosen_query
            chosen_query = globals()[chosen_query]
            return chosen_query
    if construct == 'order by' or construct == '4':
        columns = get_columns(database, for_sql=True)
        columns = random.sample(columns,7)
        random_values_df = gen_random_values(database)
        columns_clean = []
        for i in range(len(columns)):
            name = columns[i].strip().split('_')
            name = ' '.join(name)
            columns_clean.append((re.sub(r'([a-z])([A-Z]+)', r'\1 \2', name)).capitalize())
        query1 = f'''{columns_clean[0]} and {columns_clean[1]}, sorted by {columns_clean[1]}: \nSELECT "{columns[0]}", "{columns[1]}" \nFROM {database} \nORDER BY "{columns[1]}";\n'''
        query2 = f'''{columns_clean[2]} and {columns_clean[3]}, sorted by {columns_clean[3]} descending: \nSELECT "{columns[2]}", "{columns[3]}" \nFROM {database} \nORDER BY "{columns[3]}" DESC;\n'''
        query3 = f'''{columns_clean[4]} and {columns_clean[5]}, sorted by {columns_clean[6]}: \nSELECT "{columns[4]}", "{columns[5]}" \nFROM {database} \nORDER BY "{columns[5]}";\n'''
        print(query1)
        if print_all == True:
            print(query2)
            print(query3)
            chosen_query = input('Would you like to run any of these queries? If yes, input 1, 2, or 3.\n')
            chosen_query = 'query' + chosen_query
            chosen_query = globals()[chosen_query]
            run_sql_query()
    if construct == 'limit' or construct == '5':
        columns = get_columns(database, for_sql=True)
        numbers = [1,2,3,4,5,6,7,8,9,10]
        columns = random.sample(columns,7)
        numbers = random.sample(numbers,3)
        random_values_df = gen_random_values(database)
        columns_clean = []
        for i in range(len(columns)):
            name = columns[i].strip().split('_')
            name = ' '.join(name)
            columns_clean.append((re.sub(r'([a-z])([A-Z]+)', r'\1 \2', name)).capitalize())
        query1 = f'''{numbers[0]} Rows of {columns_clean[0]} and {columns_clean[1]}: \nSELECT "{columns[0]}", "{columns[1]}" \nFROM {database} \nLIMIT '{numbers[0]}';\n'''
        query2 = f'''{numbers[1]} Rows of {columns_clean[2]} and {columns_clean[3]}: \nSELECT "{columns[2]}", "{columns[3]}" \nFROM {database} \nLIMIT '{numbers[1]}';\n'''
        query3 = f'''{numbers[2]} Rows of {columns_clean[4]} and {columns_clean[5]}: \nSELECT "{columns[4]}", "{columns[5]}" \nFROM {database} \nLIMIT '{numbers[2]}';\n'''
        print(query1)        
        if print_all == True:
            print(query2)
            print(query3)
            chosen_query = input('Would you like to run any of these queries? If yes, input 1, 2, or 3.\n')
            chosen_query = 'query' + chosen_query
            chosen_query = globals()[chosen_query]
            run_sql_query()
    if construct == 'gb' or construct == '2':
        na_columns, a_columns = get_agg_variables(database)
        na_columns = random.choices(na_columns,k=3)
        a_columns = random.choices(a_columns,k=3)
        aggs = ['MIN','MAX','AVG','COUNT','SUM']
        aggs = random.sample(aggs,3)
        a_columns_clean = []
        na_columns_clean = []
        aggs_clean = []
        for i in range(len(na_columns)):
            name = na_columns[i].strip().split('_')
            name = ' '.join(name)
            na_columns_clean.append((re.sub(r'([a-z])([A-Z]+)', r'\1 \2', name)).capitalize())
        for i in range(len(a_columns)):
            name = a_columns[i].strip().split('_')
            name = ' '.join(name)
            a_columns_clean.append((re.sub(r'([a-z])([A-Z]+)', r'\1 \2', name)).capitalize())
        for i in range(len(aggs)):
            if aggs[i] == 'MIN': aggs_clean.append('minimum')
            elif aggs[i] == 'MAX': aggs_clean.append('maximum')
            elif aggs[i] == 'SUM': aggs_clean.append('sum')
            elif aggs[i] == 'COUNT': aggs_clean.append('count')
            else: aggs_clean.append('average')
        query1 = f'''{na_columns_clean[0]} by the {aggs_clean[0]} of {a_columns_clean[0]}: \nSELECT "{na_columns[0]}", {aggs[0]}("{a_columns[0]}") \nFROM {database} \nGROUP BY "{na_columns[0]}";\n'''
        query2 = f'''{na_columns_clean[1]} by the {aggs_clean[1]} of {a_columns_clean[1]}: \nSELECT "{na_columns[1]}", {aggs[1]}("{a_columns[1]}") \nFROM {database} \nGROUP BY "{na_columns[1]}";\n'''
        query3 = f'''{na_columns_clean[2]} by the {aggs_clean[2]} of {a_columns_clean[2]}: \nSELECT "{na_columns[2]}", {aggs[2]}("{a_columns[2]}") \nFROM {database} \nGROUP BY "{na_columns[2]}";\n'''
        print(query1)
        if print_all == True:
            print(query2)
            print(query3)
            chosen_query = input('Would you like to run any of these queries? If yes, input 1, 2, or 3.\n')
            chosen_query = 'query' + chosen_query
            chosen_query = globals()[chosen_query]
            run_sql_query()
    if construct == 'having' or construct == '3':
        na_columns, a_columns = get_agg_variables(database)
        na_columns = random.choices(na_columns,k=3)
        a_columns = random.choices(a_columns,k=3)
        aggs = ['MIN','MAX','AVG','COUNT','SUM']
        aggs = random.sample(aggs,3)
        random_values_df = gen_random_values(database)
        a_columns_clean = []
        na_columns_clean = []
        aggs_clean = []
        for i in range(len(na_columns)):
            name = na_columns[i].strip().split('_')
            name = ' '.join(name)
            na_columns_clean.append((re.sub(r'([a-z])([A-Z]+)', r'\1 \2', name)).capitalize())
        for i in range(len(a_columns)):
            name = a_columns[i].strip().split('_')
            name = ' '.join(name)
            a_columns_clean.append((re.sub(r'([a-z])([A-Z]+)', r'\1 \2', name)).capitalize())
        for i in range(len(aggs)):
            if aggs[i] == 'MIN': aggs_clean.append('minimum')
            elif aggs[i] == 'MAX': aggs_clean.append('maximum')
            elif aggs[i] == 'SUM': aggs_clean.append('sum')
            elif aggs[i] == 'COUNT': aggs_clean.append('count')
            else: aggs_clean.append('average')
        query1 = f'''{na_columns_clean[0]} by the {aggs_clean[0]} of {a_columns_clean[0]} greater than or equal to {random_values_df[a_columns[0]][0]}: \nSELECT "{na_columns[0]}", {aggs[0]}("{a_columns[0]}") \nFROM {database} \nGROUP BY "{na_columns[0]}"\n HAVING {aggs[0]}("{a_columns[0]}") >= '{random_values_df[a_columns[0]][0]}';\n'''
        query2 = f'''{na_columns_clean[1]} by the {aggs_clean[1]} of {a_columns_clean[1]} less than or equal to {random_values_df[a_columns[1]][0]}: \nSELECT "{na_columns[1]}", {aggs[1]}("{a_columns[1]}") \nFROM {database} \nGROUP BY "{na_columns[1]}"\n HAVING {aggs[1]}("{a_columns[1]}") <= '{random_values_df[a_columns[1]][0]}';\n'''
        query3 = f'''{na_columns_clean[2]} by the {aggs_clean[2]} of {a_columns_clean[2]} not equal to {random_values_df[a_columns[2]][0]}: \nSELECT "{na_columns[2]}", {aggs[2]}("{a_columns[2]}") \nFROM {database} \nGROUP BY "{na_columns[2]}"\n HAVING {aggs[2]}("{a_columns[2]}") != '{random_values_df[a_columns[2]][0]}';\n'''
        print(query1)
        if print_all == True:
            print(query2)
            print(query3)
            chosen_query = input('Would you like to run any of these queries? If yes, input 1, 2, or 3.\n')
            chosen_query = 'query' + chosen_query
            chosen_query = globals()[chosen_query]
            run_sql_query()
    if print_all == False:
        return query1
    else: return
#gen_sql_query(construct='gb', print_all =False)

def get_random_sql(database='coffee_shop_sales'):
    global chosen_query
    
    random_nums = ['1','2','3','4','5']
    random_nums = random.sample(random_nums,3)
    queries = []
    for i in range(3):
        query = gen_sql_query(construct=random_nums[i], database=database,print_all=False)
        queries.append(query)
    chosen_query = input('Would you like to run any of these queries? If yes, input 1, 2, or 3.\n')
    chosen_query = queries[int(chosen_query)-1]
    return 
#get_random_sql()

def manage_NL_question(question, database='coffee_shop_sales'):
    question = word_tokenize(question.lower())
    stop_words = set(stopwords.words('english'))
    stop_words.remove('by')
    stop_words.remove('having')
    sw_list = [word for word in question if word.lower() not in stop_words]
    lemmatizer = WordNetLemmatizer()
    lemmas = []
    for i in range(len(sw_list)):
        lemmas.append(lemmatizer.lemmatize(sw_list[i]))
    return lemmas
#manage_NL_question("What is the average revenue per store having revenue greater than #4.5")

def question_to_sql_list(question):
    lemmas1 = manage_NL_question(question,database='coffee_shop_sales')
    sql_list =[]
    for i in range(len(lemmas1)):
        for var, lemmas in NLP_library.items():
            if lemmas1[i] in lemmas:
                sql_list.append(var)
        if lemmas1[i].startswith("'"):
            sql_list.append(lemmas1[i].strip("'"))
    contains_hash = any('#' in item for item in lemmas1)
    if contains_hash is True: sql_list.append(lemmas1[-1])
    return sql_list
#question_to_sql_list("What is the average revenue per store having revenue greater than #4.5")

def sql_list_to_query(question, database='coffee_shop_sales'):
    global query199
    sql_list = question_to_sql_list(question)
    select = []
    where = []
    group = []
    having = []
    order = []
    
    #gb
    try: 
        gb_pos = sql_list.index('GROUP_BY')
        select.append(sql_list[gb_pos+1])
        group.append(sql_list[gb_pos+1])
    except: 
        gb_pos = -19000
    #ob
    try: 
        ob_pos = sql_list.index('ORDER_BY')
        select.append(sql_list[ob_pos+1])
        order.append(sql_list[ob_pos+1])
    except: 
        ob_pos = -19000
    #where
    try: 
        w_pos = sql_list.index('WHERE')
        select.append(sql_list[w_pos-1])
        select.append(sql_list[w_pos+1])
        where.append(sql_list[w_pos+1])
        where.append(sql_list[w_pos+2])
    except: 
        w_pos = -19000
    #having
    try: 
        h_pos = sql_list.index('HAVING')
        having.append(f"AVG({sql_list[h_pos+1]})")
        if sql_list[h_pos+2] == 'GREATER':
            having.append(f"> '{sql_list[h_pos+3]}'")
        elif sql_list[h_pos+2] == "LESS":
            having.append(f"< '{sql_list[h_pos+3]}'")
    except: 
        ob_pos = -19000
    #max
    try: 
        max_pos = sql_list.index('MAX')
        select.append(f'MAX({sql_list[max_pos+1]})')
    except: 
        max_pos = -19000
    #min
    try: 
        min_pos = sql_list.index('MIN')
        select.append(f'MIN({sql_list[min_pos+1]})')
    except: 
        min_pos = -19000
    #avg
    try: 
        avg_pos = sql_list.index('AVG')
        select.append(f'AVG({sql_list[avg_pos+1]})')
    except: 
        avg_pos = -19000
    #sum
    try: 
       sum_pos = sql_list.index('SUM')
       select.append(f'SUM({sql_list[sum_pos+1]})')
    except: 
        sum_pos = -19000
    #count
    try: 
        c_pos = sql_list.index('COUNT')
        select.append(f'COUNT({sql_list[c_pos+1]})')
    except: 
        c_pos = -19000        
    
    query199 = f'Query: SELECT {", ".join(select)} FROM {database}'
    if where: query199 += f" WHERE LOWER({where[0]}) = '{where[1]}'"
    if group: query199 += f' GROUP BY {group[0]}'
    if order: query199 += f' ORDER BY {order[0]}'
    if having: query199 += f' HAVING {having[0]} {having[1]}'
    query199 += ';'
    #print(query199)
    return 
#sql_list_to_query(question="What is the average revenue per store having revenue greater than #4.6")
    
    

#Interface Creation
def process_question():
    response = input(f'\n\n\n\n\n\n\n\n\n\n\n\n What would you like to do? You can: \n1.Ask a database a question\n2.Get sample queries from a database\n3.Get sample queries with sepecific PostgreSQL constructs\n4.Add data to the PostgreSQL server\n5.Get metadata about a table on the PostgreSQL server\nReturn the associated number with your choice!\n').strip()
    if response not in ['1','2','3','4','5']: 
        raise ValueError('Please select a choice from 1-5')
        
    elif response == '1':
        question1 = input('What question would you like to ask? \n Note: if you would like to filter your query by a number, include a # infront of the number.\n Note: If you have a specific variable you would like to filter for, surround that word in single quotes.\n')
        sql_list_to_query(question=question1)
        df = run_sql_query(query199)
        print(df)
        
    elif response == '2':
        print(get_tables())
        q2_db = input('Great -- Which database from above would you like to look at? Please only choose 1.\n').strip()
        print('Here are some example queries below!')
        get_random_sql(database=q2_db)
        df = run_sql_query(chosen_query)
        print(df)
    
    elif response == '3':
        print(get_tables())
        q3_db = input('Great -- Which database from above would you like to look at? Please only choose 1.\n').strip()
        q3_sql_construct = input(f"Awesome, we'll look at {q3_db}! What SQL constructs would you like to explore? \nChoose one below.\nYou're options are: \n1.Where Clause\n2.Group By Clause\n3.Having Clause\n4.Order By Clause\n5.Limit Clause\n")
        print('\n\nHere are some example queries below!\n\n\n')
        gen_sql_query(construct = q3_sql_construct, database = q3_db)
        df = run_sql_query(chosen_query)
        print(df)
        
    elif response == '4':
        q4_path = input("Great -- please provide the file path to the file you'd like to upload\n")
        upload_file_postgres(q4_path)

    elif response == '5':
        get_metadata()
    return

def main():
    global postgres_connection
    parser = argparse.ArgumentParser(description="Add local postgresql connection")
    parser.add_argument('--postgres_connection', type=str, help="Optionally specify a postgresql connection string")
    args = parser.parse_args()

    if args.postgres_connection:
        postgres_connection = args.postgres_connection
    
    print(f'Using PostgreSQL connection:{postgres_connection}')
    process_question()

if __name__ == "__main__":
    main()
