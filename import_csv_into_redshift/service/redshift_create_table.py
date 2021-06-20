from sqlalchemy import create_engine
import pandas as pd
from dataclasses import asdict
import psycopg2

def create_stage_sql_statement(target_table, stage_table):
    if target_table == 'playbyplay':
        sql_statement = f'''
                        CREATE TABLE {stage_table} (
                            inning bigint NULL,
                            halfinning text NULL,
                            gamepk text NULL,
                            id text NULL,
                            starttime timestamp with time zone NULL,
                            endtime timestamp with time zone NULL,
                            ispitch boolean NULL,
                            type text NULL,
                            index bigint NULL,
                            details_description text NULL,
                            details_event text NULL,
                            details_eventtype text NULL,
                            details_call_code text NULL,
                            details_call_description text NULL,
                            details_isinplay boolean NULL,
                            details_isstrike boolean NULL,
                            details_isball boolean NULL,
                            details_type_code text NULL,
                            details_type_description text NULL,
                            details_hasreview boolean NULL,
                            details_runnergoing boolean NULL,
                            pitcherid bigint NULL,
                            pitcher text NULL,
                            pitcherthrows text NULL,
                            batterid bigint NULL,
                            batter text NULL,
                            batterstands text NULL,
                            count_balls bigint NULL,
                            count_strikes bigint NULL,
                            count_outs bigint NULL,
                            pitchdata_startspeed double precision NULL,
                            pitchdata_endspeed double precision NULL,
                            pitchdata_strikezonetop double precision NULL,
                            pitchdata_strikezonebottom double precision NULL,
                            pitchdata_coordinates_ay double precision NULL,
                            pitchdata_coordinates_az double precision NULL,
                            pitchdata_coordinates_pfxx double precision NULL,
                            pitchdata_coordinates_pfxz double precision NULL,
                            pitchdata_coordinates_px double precision NULL,
                            pitchdata_coordinates_pz double precision NULL,
                            pitchdata_coordinates_vx0 double precision NULL,
                            pitchdata_coordinates_vy0 double precision NULL,
                            pitchdata_coordinates_vz0 double precision NULL,
                            pitchdata_coordinates_x double precision NULL,
                            pitchdata_coordinates_y double precision NULL,
                            pitchdata_coordinates_x0 double precision NULL,
                            pitchdata_coordinates_y0 double precision NULL,
                            pitchdata_coordinates_z0 double precision NULL,
                            pitchdata_coordinates_ax double precision NULL,
                            pitchdata_breaks_breakangle double precision NULL,
                            pitchdata_breaks_breaklength double precision NULL,
                            pitchdata_breaks_breaky double precision NULL,
                            pitchdata_breaks_spinrate double precision NULL,
                            pitchdata_breaks_spindirection double precision NULL,
                            pitchdata_zone double precision NULL,
                            pitchdata_typeconfidence double precision NULL,
                            pitchdata_platetime double precision NULL,
                            pitchdata_extension double precision NULL,
                            hitdata_launchspeed double precision NULL,
                            hitdata_launchangle double precision NULL,
                            hitdata_totaldistance double precision NULL,
                            hitdata_trajectory text NULL,
                            hitdata_hardness text NULL,
                            hitdata_location double precision NULL,
                            hitdata_coordinates_coordx double precision NULL,
                            hitdata_coordinates_coordy double precision NULL,
                            ingestion_timestamp_utc timestamp without time zone NULL
                        );
                    '''
    


    return sql_statement



def create_stage_table(redshift_credentials, target_table, stage_table):

    sql_statement = create_stage_sql_statement(target_table, stage_table)

    con=connect_redshift(redshift_credentials)

    cur = con.cursor()
    cur.execute(sql_statement)
    con.commit()

    cur.close() 
    con.close()

    return print('successfully ran sql statement: create_stage')

def connect_redshift(redshift_credentials):
    con=psycopg2.connect(dbname= redshift_credentials['database'], 
                    host=redshift_credentials['host'], 
                    port= redshift_credentials['port'], 
                    user= redshift_credentials['username'], 
                    password= redshift_credentials['password']
                    )
    return con

