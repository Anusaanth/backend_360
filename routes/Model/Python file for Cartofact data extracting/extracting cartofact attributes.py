#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 23:51:57 2023

@author: amberhuang
"""
import psycopg2
import pandas as pd
import os



### Check connection to cartofact
# Database connection parameters
db_params = {
    'host': 'cartofact.com',
    'port': '5441',
    'database': 'cartofact_360',
    'user': 'three_sixty',
    'password': 'IMnz9tyMoFsq'
}

# Establish a connection to the PostgreSQL database
try:
    connection = psycopg2.connect(**db_params)
    print("Connected to the database!")

    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    cursor.close()
    connection.close()

except psycopg2.Error as e:
    print("Unable to connect to the database.")
    print(e)
    
    
### Obtain the attribute data from database save as "attribute_df"
current_directory = os.getcwd()
xlsx_file_path = os.path.join(current_directory, 'lic.xlsx')
lic=pd.read_excel(xlsx_file_path)
lic=lic.values.tolist()
lic=[item[0] for item in lic]
lic = [str(val).zfill(7) for val in lic]
at_table_file = os.path.join(current_directory, 'at_table.xlsx')
df=pd.read_excel(at_table_file)
column_name = df['attribute'].tolist()
table_name = df['table'].iloc[0]


query = f"""
SELECT {', '.join(column_name)}
FROM {table_name}
WHERE Licence IN %s;"""

# Establish a connection to the PostgreSQL database
try:
    connection = psycopg2.connect(**db_params)
    print("Connected to the database!")

    cursor = connection.cursor()

    cursor.execute(query, (tuple(lic),))

    result_data = cursor.fetchall()

    attribute_df = pd.DataFrame(result_data, columns=column_name)
    print(attribute_df[:5])
    csv_file_path = os.path.join(current_directory, 'attribute_df.csv')
    attribute_df.to_csv(csv_file_path, index=False)


except psycopg2.Error as e:
    print("Unable to connect to the database or execute the query.")
    print(e)

finally:
    # Close the cursor and connection when you're done
    if cursor:
        cursor.close()
    if connection:
        connection.close()