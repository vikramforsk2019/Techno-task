"""
Environment:
MySQL Database 
Tables:

Table #1
Cols: F1, F2, F3, F4

Table #2
Cols: C1, C2, C3, C4

django
Task: Import CSV and copy fields from Table 1 to Table 2 based on the columns mapping in CSV. 
CSV Format attached

Copy From,Copy To
F1,C2
F2,C3
F3,C4
F4,C1
"""

import pandas as pd
from pandas import DataFrame
import mysql.connector
#Read csv file
df = pd.read_csv("CSV Import.csv")




conn = mysql.connector.connect ( user='root', password='', host='localhost')
c = conn.cursor()

# STEP 0 if exists
c.execute("DROP DATABASE task;")

# STEP 1
c.execute("CREATE DATABASE task;")

# STEP 2
c.execute("USE task;")

# STEP 3
c.execute ("""CREATE TABLE first(
          F1 TEXT,
          F2  TEXT,
          F3 TEXT,
          F4 TEXT
          )""")

# STEP 4
c.execute("INSERT INTO first VALUES (1,'vikram', 'singh', 500)")
c.execute("INSERT INTO first VALUES (2,'ram', 'singh', 600)")
c.execute("INSERT INTO first VALUES (3,'veer', 'singh', 700)")
conn.commit()

c.execute ("""CREATE TABLE second(
          C1 TEXT,
          C2  TEXT,
          C3 TEXT,
          C4 TEXT
          )""")


c.execute("SELECT * FROM first")
df2 =DataFrame(c.fetchall())  # putting the result into Dataframe
df2.columns =df['Copy To'].tolist()
df2=df2.loc[:,['C1','C2','C3','C4']]

#df3.to_sql(name = 'second', con=c, if_exists = 'append', index = True)



# creating column list for insertion
cols = "`,`".join([str(i) for i in df2.columns.tolist()])

# Insert DataFrame recrds one by one.
for i,row in df2.iterrows():
    sql = "INSERT INTO `second` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    c.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()


# Table first
print('From Table')
sql = "SELECT * FROM `first`"
c.execute(sql)
# Fetch all the records and use a for loop to print them one line at a time
result = c.fetchall()
for i in result:
    print(i)

# Table Second
print('Copy To')
sql = "SELECT * FROM `second`"
c.execute(sql)
# Fetch all the records and use a for loop to print them one line at a time
result = c.fetchall()
for i in result:
    print(i)
conn.close()










