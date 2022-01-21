import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import psycopg2
from scripts.templating import choose_template 
from table_creation.sql_queries import execute_queries

def get_env_content(contains="DATABASE"):
    """
    - Fetch all envs that contains word above
    - Returns a dict
    """
    import os
    
    env_dict = {}

    for each_declared_env in os.environ:
        if contains in each_declared_env:
            env_dict[each_declared_env]=os.getenv(each_declared_env)
    
    return env_dict


def generate_uri(template="postgres", env_contains="DATABASE", override_env={}):
    """
    - Generate the URI to connect in the database
    """
    
    TEMPLATE=choose_template(template)
    
    ENV=get_env_content(env_contains)
    
    for key, val in override_env.items():
        ENV[key]=val 
    
    return TEMPLATE.format(**ENV)

def drop_and_create_database(cur, database_name="sparkifydb", utf8=None):
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS {}".format(database_name))
    if utf8:
        cur.execute("CREATE DATABASE {} WITH ENCODING 'utf8' TEMPLATE template0".format(database_name))

def get_connector_and_cursor(**kwargs):
    """
    - Generate connector and cursor to connect with database
    """
    conn = psycopg2.connect(generate_uri(**kwargs))
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    return conn, cur

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    conn , cur = get_connector_and_cursor()

    drop_and_create_database(cur, "sparkifydb", utf8=True)

    # close connection to default database
    conn.close() 
    
    conn, cur = get_connector_and_cursor(override_env={"DATABASE_NAME":"sparkifydb"})

    for query in execute_queries:
        cur.execute(query)
        
    conn.close()


if __name__ == "__main__":
    main()