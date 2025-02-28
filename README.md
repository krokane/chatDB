## ChatDB
ChatDB is a program designed to replicate simple tasks of the ChatGPT chatbot,
specifically in regards to a dataset inserted into the program. The program is a beginner’s
tool to understanding SQL querying of a database, allowing for five main functionalities
– uploading a data table to a PostgreSQL database, understanding a data table through
looking at metadata, looking at example SQL queries of that table to understand basic
SQL clauses, asking natural language questions of that data table, and ultimately using
the provided SQL queries to pull specific data from the database.


### Required Packages
argparse
, logging
, nltk
, pandas
, psycopg2
, os
, random
, re
, sys
, time
, collections
, sqlalchemy

These packages can be installed using pip in your command line (example syntax: `pip install pandas`)

### To run the program:
Navigate to your chatDB.py file using cd in the command line (example syntax: `cd desktop/dsci551`).

Input `python chatDB.py` into your command line to run the program.

If you have a local PostgreSQL instance running, you can connect to it using the command line argument:
`--postgres_connection {your_connection_string}`.
