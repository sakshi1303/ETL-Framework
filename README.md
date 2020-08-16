# ETL-Framework

Configurations Tables -

1. Job Type
2. Job Log
3. Data Config
4. Job Dependency


Job Config Table - 

CREATE TABLE JOB_CONFIG(
JOB_CONFIG_ID    	INT,
JOB_ID           	INT,
JOB_NAME         	CHAR(50),
JOB_TYPE         	CHAR(20),
SOURCE_FORMAT_TYPE      CHAR(20),
SOURCE_FORMAT_VALUE	CHAR(100),
LOAD_TABLE       	CHAR(50),
LOAD_QUERY       	CHAR(500),
JOB_DEPENDENCY_LEVEL   	INT,
JOB_DEPENDENCY_ID      	INT
);

INSERT INTO JOB_CONFIG VALUES(1, 1, 'TEST_EMP_STG_LOAD', 'EX', 'FILE', 'C:\Users\Public\Sakshi\ETL\Source\emp.csv', 'EMP_STG', 'emp_src.sql', 0, 0);

```python

import pandas as pd
import sqlite3

conn = sqlite3.connect('C:\\Users\\Public\\Sakshi\\sqlite\\etl.db')
c = conn.cursor()

job_id = (1,)
c.execute('SELECT  SOURCE_FORMAT_VALUE, LOAD_TABLE, LOAD_QUERY FROM JOB_CONFIG WHERE JOB_ID = ?', job_id)
##print(c.fetchall())
rows = c.fetchall()
li = []
for row in rows:
    for col in row:
        li.append(col)
##print(li, ' : ', type(li))

df = pd.read_csv(li[0])
##print(df.head)

df.to_sql(li[1], conn, if_exists='replace', index=False)

```
