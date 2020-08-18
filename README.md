# ETL-Framework

Configurations Tables -

1. Job Type
2. Job Log
3. Data Config
4. Job Dependency


Job Config Table - 

```sql
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

INSERT INTO JOB_CONFIG VALUES(1, 1, 'TEST_EMP_STG_LOAD', 'EX', 'FILE', 'C:\Users\Public\Sakshi\ETL\Source\emp.csv', 'EMP_STG', 'emp_src.csv', 0, 0);
INSERT INTO JOB_CONFIG VALUES(2, 2, 'TEST_EMP_TRANSFORM', 'TR', 'FILE', 'C:\Users\Public\Sakshi\ETL\Trans\qry_emp_trans.sql', 'emp_tgt.csv', 'qry_emp_trans.sql', 1, 1);
INSERT INTO JOB_CONFIG VALUES(3, 3, 'TEST_EMP_MAIN_LOAD', 'LD', 'FILE', 'C:\Users\Public\Sakshi\ETL\Trans\emp_tgt.csv', 'EMP', 'emp_tgt.csv', 2, 2);
```

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

```sql
C:\Users\Public\Sakshi\sqlite>sqlite3
SQLite version 3.17.0 2017-02-13 16:02:40
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> .open etl.db
sqlite> .tables
sqlite> select * from sqlite_master;
sqlite> CREATE TABLE JOB_CONFIG
   ...> (
   ...> JOB_CONFIG_ID    INT,
   ...> JOB_ID           INT,
   ...> JOB_NAME         CHAR(50),
   ...> JOB_TYPE         CHAR(20),
   ...> SOURCE_FORMAT_TYPE      CHAR(20),
   ...> SOURCE_FORMAT_VALUE     CHAR(100),
   ...> LOAD_TABLE       CHAR(50),
   ...> LOAD_QUERY       CHAR(500),
   ...> JOB_DEPENDENCY_LEVEL   INT,
   ...> JOB_DEPENDENCY_ID      INT
   ...> );
sqlite>
sqlite> .tables
JOB_CONFIG
sqlite> INSERT INTO JOB_CONFIG VALUES(1, 1, 'TEST_EMP_STG_LOAD', 'EX', 'FILE', 'C:\Users\Public\Sakshi\ETL\Source\emp.csv', 'EMP_STG', 'emp_src.sql', 0, 0);
sqlite> select * from job_config;
1|1|TEST_EMP_STG_LOAD|EX|FILE|C:\Users\Public\Sakshi\ETL\Source\emp.csv|EMP_STG|emp_src.sql|0|0
sqlite> select * from job_config;
1|1|TEST_EMP_STG_LOAD|EX|FILE|C:\Users\Public\Sakshi\ETL\Source\emp.csv|EMP_STG|emp_src.sql|0|0
sqlite> .tables
EMP_STG     JOB_CONFIG
sqlite> select * from emp_stg;
1|John Doe|30|London|john.doe@mail.com|987654321|HR|50000
1|Jane Doe|25|London|jane.doe@mail.com|999999999|HR|50000
1|Mary Hulk|27|New York|mary.hulk@mail.com|9876123546|IT|40000
1|Tom Gibbs|29|Canada|tom.gibbs@mail.com|8769342104|IT|55000
1|Tony Stark|33|New York|tony.stark@mail.com|2345671267|FINANCE|45000
sqlite>

```
Transform



Load

```sql
CREATE TABLE EMP
( EMP_ID   	INT,
  EMP_NAME 	CHAR(100),
  EMP_AGE 	INT,
  EMP_ADDRESS   CHAR(500),
  EMAIL  	CHAR(100),
  CONTACT_NO  	INT,
  DEPT  	CHAR(20),
  SALARY 	INT
)

```
