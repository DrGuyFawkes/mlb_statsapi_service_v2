from sqlalchemy import create_engine
import pandas as pd
from dataclasses import asdict
import psycopg2

def create_stage_sql_statement(target_table, stage_table):
    if target_table == 'playbyplay':
        sql_statement = f'''
                        CREATE TABLE IF NOT EXISTS  {stage_table} (
                            inning bigint NULL,
                            halfinning text NULL,
                            gamepk text NULL,
                            id text NOT NULL,
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
                            pitcherid text NULL,
                            pitcher text NULL,
                            pitcherthrows text NULL,
                            batterid text NULL,
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
                            ingestion_timestamp_utc timestamp without time zone NULL,
                            primary key (id)
                        );
                    '''
    
    elif target_table == 'players':
        sql_statement = f'''
                        CREATE TABLE IF NOT EXISTS  {stage_table} (
                                ingestion_timestamp_utc timestamp NULL,
                                usename text NULL,
                                nameslug text NULL,
                                namefirstlast text NULL,
                                middlename text NULL,
                                lastname text NULL,
                                lastinitname text NULL,
                                lastfirstname text NULL,
                                initlastname text NULL,
                                fullname text NULL,
                                fulllfmname text NULL,
                                fullfmlname text NULL,
                                firstname text NULL,
                                firstlastname text NULL,
                                pitchhand_description text NULL,
                                pitchhand_code text NULL,
                                batside_description text NULL,
                                batside_code text NULL,
                                primaryposition_type text NULL,
                                primaryposition_name text NULL,
                                primaryposition_code text NULL,
                                primaryposition_abbreviation text NULL,
                                strikezonebottom double precision NULL,
                                strikezonetop double precision NULL,
                                primarynumber text NULL,
                                mlbdebutdate timestamp NULL,
                                link text NULL,
                                isverified boolean NULL,
                                isplayer boolean NULL,
                                draftyear text NULL,
                                weight text NULL,
                                height text NULL,
                                gender text NULL,
                                active boolean NULL,
                                currentage bigint NULL,
                                boxscorename text NULL,
                                birthstateprovince text NULL,
                                birthdate timestamp NULL,
                                birthcountry text NULL,
                                birthcity text NULL,
                                id text NOT NULL,
                                primary key (id)                    
                        );
                    '''

    elif target_table == 'teams':
        sql_statement = f'''
                        CREATE TABLE IF NOT EXISTS  {stage_table} (
                        ingestion_timestamp_utc timestamp NULL,
                        springvenue_link text NULL,
                        springvenue_id text NULL,
                        springleague_name text NULL,
                        springleague_link text NULL,
                        springleague_id text NULL,
                        springleague_abbreviation text NULL,
                        sport_name text NULL,
                        sport_link text NULL,
                        sport_id text NULL,
                        division_name text NULL,
                        division_link text NULL,
                        division_id text NULL,
                        league_name text NULL,
                        league_link text NULL,
                        league_id text NULL,
                        venue_name text NULL,
                        venue_link text NULL,
                        venue_id text NULL,
                        active boolean NULL,
                        abbreviation text NULL,
                        allstarstatus text NULL,
                        clubname text NULL,
                        filecode text NULL,
                        firstyearofplay bigint NULL,
                        franchisename text NULL,
                        link text NULL,
                        season text NULL,
                        locationname text NULL,
                        name text NULL,
                        teamcode text NULL,
                        shortname text NULL,
                        teamname text NULL,
                        id text NOT NULL,
                        primary key (id)             
                        );
                    '''

    elif target_table == 'livefeed':
        sql_statement = f'''
                        CREATE TABLE IF NOT EXISTS  {stage_table} (
                        ingestion_timestamp_utc TIMESTAMP NULL,
                        teams_home_runs bigint NULL,
                        teams_home_leftonbase bigint NULL,
                        teams_home_hits bigint NULL,
                        teams_home_errors bigint NULL,
                        teams_away_runs bigint NULL,
                        teams_away_leftonbase bigint NULL,
                        teams_away_hits bigint NULL,
                        teams_away_errors bigint NULL,
                        flags_perfectgame boolean NULL,
                        flags_nohitter boolean NULL,
                        flags_hometeamperfectgame boolean NULL,
                        flags_hometeamnohitter boolean NULL,
                        flags_awayteamperfectgame boolean NULL,
                        flags_awayteamnohitter boolean NULL,
                        weather_wind text NULL,
                        weather_temp text NULL,
                        weather_condition text NULL,
                        venue_timezone_tz text NULL,
                        venue_timezone_offset text NULL,
                        venue_timezone_id text NULL,
                        venue_name text NULL,
                        venue_location_stateabbrev text NULL,
                        venue_location_state text NULL,
                        venue_location_postalcode text NULL,
                        venue_location_phone text NULL,
                        venue_location_defaultcoordinates_latitude text NULL,
                        venue_location_country text NULL,
                        venue_location_city text NULL,
                        venue_location_address2 text NULL,
                        venue_location_address1 text NULL,
                        venue_link text NULL,
                        venue_id text NULL,
                        venue_fieldinfo_turftype text NULL,
                        venue_fieldinfo_rooftype text NULL,
                        venue_fieldinfo_rightline text NULL,
                        venue_fieldinfo_rightcenter text NULL,
                        venue_fieldinfo_leftline text NULL,
                        venue_fieldinfo_leftcenter text NULL,
                        venue_fieldinfo_center text NULL,
                        venue_fieldinfo_capacity text NULL,
                        venue_active boolean NULL,
                        teams_home_link text NULL,
                        teams_home_name text NULL,
                        teams_home_id text NULL,
                        teams_away_link text NULL,
                        teams_away_name text NULL,
                        teams_away_id text NULL,
                        status_statuscode text NULL,
                        status_starttimetbd boolean NULL,
                        status_detailedstate text NULL,
                        status_codedgamestate text NULL,
                        status_abstractgamestate text NULL,
                        status_abstractgamecode text NULL,
                        officialvenue_link text NULL,
                        officialvenue_id text NULL,
                        primarydatacaster_link text NULL,
                        primarydatacaster_id text NULL,
                        primarydatacaster_fullname text NULL,
                        officialscorer_link text NULL,
                        officialscorer_id text NULL,
                        officialscorer_fullname text NULL,
                        game_type text NULL,
                        game_tiebreaker text NULL,
                        game_seasondisplay text NULL,
                        game_season text NULL,
                        game_id text NULL,
                        game_gamedaytype text NULL,
                        game_gamenumber text NULL,
                        game_doubleheader text NULL,
                        game_calendareventid text NULL,
                        gameinfo_gamedurationminutes bigint NULL,
                        gameinfo_firstpitch TIMESTAMPTZ NULL,
                        gameinfo_attendance bigint NULL,
                        datetime_time TIME NULL,
                        datetime_officialdate TIMESTAMP NULL,
                        datetime_daynight text NULL,
                        datetime_datetime TIMESTAMPTZ NULL,
                        datetime_ampm text NULL,
                        id text NOT NULL,
                        primary key (id)
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

