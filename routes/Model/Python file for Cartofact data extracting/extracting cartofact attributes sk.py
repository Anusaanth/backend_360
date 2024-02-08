#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 11:15:30 2023

@author: qianhuang
"""
import psycopg2
import pandas as pd



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
lic=pd.read_excel("/Users/qianhuang/Desktop/360/model/extracting attributes/lic.xlsx")
lic=lic['lic'].values.tolist()
df=pd.read_excel("/Users/qianhuang/Desktop/360/model/extracting attributes/at_table.xlsx")
column_name = df['attribute'].tolist()
table_name = df['table'].iloc[0]


query = f"""
SELECT {', '.join(column_name)}
FROM {table_name}
WHERE licence IN %s;"""

# Establish a connection to the PostgreSQL database
try:
    connection = psycopg2.connect(**db_params)
    print("Connected to the database!")

    cursor = connection.cursor()

    cursor.execute(query, (tuple(lic),))

    result_data = cursor.fetchall()

    attribute_df = pd.DataFrame(result_data, columns=column_name)
    print(attribute_df[:5])
    attribute_df.to_csv('/Users/qianhuang/Desktop/360/model/extracting attributes/attribute_df.csv', index=False)


except psycopg2.Error as e:
    print("Unable to connect to the database or execute the query.")
    print(e)

finally:
    # Close the cursor and connection when you're done
    if cursor:
        cursor.close()
    if connection:
        connection.close()