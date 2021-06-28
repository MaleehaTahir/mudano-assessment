from configparser import ConfigParser
from psycopg2 import OperationalError
from sqlalchemy import create_engine
import psycopg2

# should ideally be sitting in the helper folder


def config(ini_file: str):
    parser = ConfigParser()
    parser.read(ini_file)
    config_info = {param[0]: param[1] for param in parser.items("postgresql")}

    return config_info


# ideally, put all methods in an object class, and call them accordingly, but i am taking a few shortcuts here
def validate_database_objects(ini_file: str):
    _config = config(ini_file)
    connection_string = f"user={_config['user']} password={_config['password']}"
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    conn.autocommit = True
    try:
        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname='{_config['database']}'")
        exists = cur.fetchone()
        if not exists:
            sql_query = f"CREATE DATABASE {_config['database']}"
            cur.execute(sql_query)
    except OperationalError as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")
        cur.close()
    else:
        conn.autocommit = False


def table_exists(ini_file: str, table_str):
    _config = config(ini_file)
    conn = psycopg2.connect(f"dbname={_config['database']} user={_config['user']} "
                            f"host={_config['host']} user={_config['user']}")
    try:
        cur = conn.cursor()
        cur.execute("select * from information_schema.tables where table_name=%s", (table_str,))
        exists = bool(cur.rowcount)
        print(exists)
        cur.close()
    except psycopg2.Error as e:
        raise e

    return exists


"""
--using alchemy to load table
--will load it as a one off but for production, logic needs ot be adjusted to allow for either an insert only
  or an upsert operation 
"""


def df2_to_postgres(fileConfig, table_name, df):
    connection_string = f"postgresql://{fileConfig['user']}:{fileConfig['password']}" \
                        f"@{fileConfig['host']}:5432/{fileConfig['database']}"
    engine = create_engine(connection_string)
    try:
        if table_exists('../database.ini', table_name):
            print("loading table as a one off")
            pass
        else:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
    except OperationalError as e:
        print(f"{type(e).__name__}: {e}")

    return engine


def export_from_postgres(fileConfig, table_name, load_date, target_filename):
    import os
    conn = psycopg2.connect(f"dbname={fileConfig['database']} user={fileConfig['user']} host={fileConfig['host']} user={fileConfig['user']}")
    cursor = conn.cursor()

    try:
        query = """SELECT * FROM {} """.format(table_name)

        try:
            output_query = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
            target_path = "../outputs/export/" + load_date
            os.makedirs(target_path, exist_ok=True)
            with open(target_path+'/'+target_filename, 'w') as f:
                cursor.copy_expert(output_query, f)

        except psycopg2.Error as e:
            raise e

    except Exception as e:
        raise e

    finally:

        cursor.close()
        conn.close()





