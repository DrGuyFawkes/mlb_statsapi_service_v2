from sqlalchemy import create_engine
import pandas as pd
from dataclasses import asdict
import psycopg2

def insert_csv_into_stage(redshift_credentials, stage_name, df_csv):
  
    username = redshift_credentials['username']
    password = redshift_credentials['password']
    db_host = redshift_credentials['host']
    db_port = redshift_credentials['port']
    db_name = redshift_credentials['database']

    conn = create_engine(f'postgresql://{username}:{password}@{db_host}:{db_port}/{db_name}')

    return df_csv.to_sql(stage_name, conn, index=False, if_exists='append',chunksize=1000, method='multi')


def upsert_data(redshift_credentials, target_table, stage_table, columns):
    if 'playbyplay' in stage_table:
        primary_key = 'id'


    #update = ', '.join([f'{key} = EXCLUDED.{key}' for key in columns.split(',') ])
    update = ', '.join([f'{key} = EXCLUDED.{key}' for key in columns])
    columns_sql = ', '.join([f'{key}' for key in columns])
    sql_statement = f"""
                    INSERT INTO {target_table} ({columns_sql})
                    SELECT DISTINCT ON ({primary_key}) * from {stage_table} ORDER BY {primary_key}, ingestion_timestamp_utc DESC NULLS LAST
                    ON CONFLICT ({primary_key}) 
                    DO UPDATE SET {update}
                    WHERE EXCLUDED.ingestion_timestamp_utc > {target_table}.ingestion_timestamp_utc;
                    """

    con=connect_redshift(redshift_credentials)

    cur = con.cursor()
    cur.execute(sql_statement)
    con.commit()

    cur.close() 
    con.close()

    return print('successfully ran sql statement: upsert_data')

def get_column_name(redshift_credentials, object_key):

    stage_name = object_key.split('/')[-1].split('.')[0]

    sql_statement = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{stage_name}'
                    """
    
    con=connect_redshift(redshift_credentials)

    cur = con.cursor()
    cur.execute(sql_statement)
    result = cur.fetchall()

    columns = []

    for column in result:
        column_name = column[0]
        columns.append(column_name)


    cur.close() 
    con.close()

    return columns

def drop_table(redshift_credentials, stage_table):
    sql_statement = f"""
                    DROP TABLE IF EXISTS {stage_table}
                    """
    con=connect_redshift(redshift_credentials)

    cur = con.cursor()
    cur.execute(sql_statement)
    con.commit()

    cur.close() 
    con.close()
    return print('successfully ran sql statement: drop_table')

def merge_operation_by_replacing_existing_rows(redshift_credentials, target_table, stage_table):
    if 'playbyplay' in stage_table:
        primary_key = 'id'

    sql_statement = f"""
                    begin transaction;

                    delete from {target_table} 
                    using {stage_table}
                    where {target_table}.{primary_key} = {stage_table}.{primary_key}; 

                    insert into {target_table}  
                    select * from {stage_table};

                    end transaction;

                    """

    con=connect_redshift(redshift_credentials)

    cur = con.cursor()
    cur.execute(sql_statement)
    con.commit()

    cur.close() 
    con.close()

    return print('successfully ran sql statement: upsert_data')

def connect_redshift(redshift_credentials):
    con=psycopg2.connect(dbname= redshift_credentials['database'], 
                    host=redshift_credentials['host'], 
                    port= redshift_credentials['port'], 
                    user= redshift_credentials['username'], 
                    password= redshift_credentials['password']
                    )
    return con

