import json
import requests
import pandas as pd


def get_playbyplay_statsapi(gamepk):

    apicall = 'https://statsapi.mlb.com/api/v1/game/' + str(gamepk) + '/playByPlay'
    statapi = requests.get(apicall)
    playbyplay = json.loads(statapi.text)

    pbp = []
    
    if len(playbyplay['allPlays']) > 0:
        for play in playbyplay['allPlays']:    
            
            if len(play['playEvents']) > 0:
                for play_event in play['playEvents']:      

                    # normarlized json and convert all keys to lowercase
                    json_normalized = pd.io.json.json_normalize(play_event, sep='_').to_dict(orient='records')[0]
                    normalized_row = dict((k.lower(), v) for k, v in json_normalized.items())

                    # only type == 'pitch', other type don't have a playid which is a unique primary key. 
                    if normalized_row.get("type", None) == 'pitch':

                        row = {}
                        
                        # game 
                        row['gamepk'] = str(gamepk)

                        # event
                        row['id'] = normalized_row.get("playid", None)  # rename primary key playid --> id
                        row['starttime'] = normalized_row.get("starttime", None)
                        row['endtime'] = normalized_row.get("endtime", None)
                        row['ispitch'] = normalized_row.get("ispitch", None)
                        row['type'] = normalized_row.get("type", None)
                        row['index'] = normalized_row.get("index", None)

                        # inning
                        row['halfinning'] = play['about'].get("halfInning", None) ### add
                        row['inning'] = play['about'].get("inning", None) ### add

                        # matchup
                        row['pitcherid'] = str(play['matchup']['pitcher'].get("id", None))
                        row['pitcher'] = str(play['matchup']['pitcher'].get("fullName", None)) ### add
                        row['pitcherthrows'] = play['matchup']['pitchHand'].get("code", None)
                        row['batterid'] = str(play['matchup']['batter'].get("id", None))
                        row['batter'] = str(play['matchup']['batter'].get("fullName", None)) ### add
                        row['batterstands'] = play['matchup']['batSide'].get("code", None)


                        # detail
                        row['details_description'] = normalized_row.get("details_description", None)
                        row['details_event'] = normalized_row.get("details_event", None)
                        row['details_eventtype'] = normalized_row.get("details_eventtype", None)
                        row['details_call_code'] = normalized_row.get("details_call_code", None)
                        row['details_call_description'] = normalized_row.get("details_call_description", None)
                        row['details_isinplay'] = normalized_row.get("details_isinplay", None)
                        row['details_isstrike'] = normalized_row.get("details_isstrike", None)
                        row['details_isball'] = normalized_row.get("details_isball", None)
                        row['details_type_code'] = normalized_row.get("details_type_code", None)
                        row['details_type_description'] = normalized_row.get("details_type_description", None)
                        row['details_hasreview'] = normalized_row.get("details_hasreview", None)
                        row['details_runnergoing'] = normalized_row.get("details_runnergoing", None)

                        # counts
                        row['count_balls'] = normalized_row.get("count_balls", None)
                        row['count_strikes'] = normalized_row.get("count_strikes", None)
                        row['count_outs'] = normalized_row.get("count_outs", None)

                        # pitch
                        row['pitchdata_startspeed'] = normalized_row.get("pitchdata_startspeed", None)
                        row['pitchdata_endspeed'] = normalized_row.get("pitchdata_endspeed", None)
                        row['pitchdata_strikezonetop'] = normalized_row.get("pitchdata_strikezonetop", None)
                        row['pitchdata_strikezonebottom'] = normalized_row.get("pitchdata_strikezonebottom", None)
                        row['pitchdata_coordinates_ay'] = normalized_row.get("pitchdata_coordinates_ay", None)
                        row['pitchdata_coordinates_az'] = normalized_row.get("pitchdata_coordinates_az", None)
                        row['pitchdata_coordinates_pfxx'] = normalized_row.get("pitchdata_coordinates_pfxx", None)
                        row['pitchdata_coordinates_pfxz'] = normalized_row.get("pitchdata_coordinates_pfxz", None)
                        row['pitchdata_coordinates_px'] = normalized_row.get("pitchdata_coordinates_px", None)
                        row['pitchdata_coordinates_pz'] = normalized_row.get("pitchdata_coordinates_pz", None)
                        row['pitchdata_coordinates_vx0'] = normalized_row.get("pitchdata_coordinates_vx0", None)
                        row['pitchdata_coordinates_vy0'] = normalized_row.get("pitchdata_coordinates_vy0", None)
                        row['pitchdata_coordinates_vz0'] = normalized_row.get("pitchdata_coordinates_vz0", None)
                        row['pitchdata_coordinates_x'] = normalized_row.get("pitchdata_coordinates_x", None)
                        row['pitchdata_coordinates_y'] = normalized_row.get("pitchdata_coordinates_y", None)
                        row['pitchdata_coordinates_x0'] = normalized_row.get("pitchdata_coordinates_x0", None)
                        row['pitchdata_coordinates_y0'] = normalized_row.get("pitchdata_coordinates_y0", None)
                        row['pitchdata_coordinates_z0'] = normalized_row.get("pitchdata_coordinates_z0", None)
                        row['pitchdata_coordinates_ax'] = normalized_row.get("pitchdata_coordinates_ax", None)
                        row['pitchdata_breaks_breakangle'] = normalized_row.get("pitchdata_breaks_breakangle", None)
                        row['pitchdata_breaks_breaklength'] = normalized_row.get("pitchdata_breaks_breaklength", None)
                        row['pitchdata_breaks_breaky'] = normalized_row.get("pitchdata_breaks_breaky", None)
                        row['pitchdata_breaks_spinrate'] = normalized_row.get("pitchdata_breaks_spinrate", None)
                        row['pitchdata_breaks_spindirection'] = normalized_row.get("pitchdata_breaks_spindirection", None)
                        row['pitchdata_zone'] = normalized_row.get("pitchdata_zone", None)
                        row['pitchdata_typeconfidence'] = normalized_row.get("pitchdata_typeconfidence", None)
                        row['pitchdata_platetime'] = normalized_row.get("pitchdata_platetime", None)
                        row['pitchdata_extension'] = normalized_row.get("pitchdata_extension", None)

                        # hit
                        row['hitdata_launchspeed'] = normalized_row.get("hitdata_launchspeed", None)
                        row['hitdata_launchangle'] = normalized_row.get("hitdata_launchangle", None)
                        row['hitdata_totaldistance'] = normalized_row.get("hitdata_totaldistance", None)
                        row['hitdata_trajectory'] = normalized_row.get("hitdata_trajectory", None)
                        row['hitdata_hardness'] = normalized_row.get("hitdata_hardness", None)
                        row['hitdata_location'] = normalized_row.get("hitdata_location", None)
                        row['hitdata_coordinates_coordx'] = normalized_row.get("hitdata_coordinates_coordx", None)
                        row['hitdata_coordinates_coordy'] = normalized_row.get("hitdata_coordinates_coordy", None)

                        pbp.append(row)

    return pbp



def get_livefeed_statsapi(link):

    apicall = 'https://statsapi.mlb.com' + link
    statapi = requests.get(apicall)
    livefeed_endpoint = json.loads(statapi.text)

    ## livefeed: gameData
    livefeed_gamedata = livefeed_endpoint['gameData']

    json_normalized = pd.io.json.json_normalize(livefeed_gamedata, sep='_').to_dict(orient='records')[0]
    normalized_row = dict((k.lower(), v) for k, v in json_normalized.items())

    livefeed = []

    row = {}

    # game
    row['id'] = normalized_row.get("game_pk", None)
    row['datetime_ampm'] = normalized_row.get("datetime_ampm", None)
    row['datetime_datetime'] = normalized_row.get("datetime_datetime", None)
    row['datetime_daynight'] = normalized_row.get("datetime_daynight", None)
    row['datetime_officialdate'] = normalized_row.get("datetime_officialdate", None)
    row['datetime_time'] = normalized_row.get("datetime_time", None)
    row['gameinfo_attendance'] = normalized_row.get("gameinfo_attendance", None)
    row['gameinfo_firstpitch'] = normalized_row.get("gameinfo_firstpitch", None)
    row['gameinfo_gamedurationminutes'] = normalized_row.get("gameinfo_gamedurationminutes", None)
    row['game_calendareventid'] = normalized_row.get("game_calendareventid", None)
    row['game_doubleheader'] = normalized_row.get("game_doubleheader", None)
    row['game_gamenumber'] = normalized_row.get("game_gamenumber", None)
    row['game_gamedaytype'] = normalized_row.get("game_gamedaytype", None)
    row['game_id'] = normalized_row.get("game_id", None)
    row['game_season'] = normalized_row.get("game_season", None)
    row['game_seasondisplay'] = normalized_row.get("game_seasondisplay", None)
    row['game_tiebreaker'] = normalized_row.get("game_tiebreaker", None)
    row['game_type'] = normalized_row.get("game_type", None)

    row['officialscorer_fullname'] = normalized_row.get("officialscorer_fullname", None)
    row['officialscorer_id'] = normalized_row.get("officialscorer_id", None)
    row['officialscorer_link'] = normalized_row.get("officialscorer_link", None)

    row['primarydatacaster_fullname'] = normalized_row.get("primarydatacaster_fullname", None)
    row['primarydatacaster_id'] = normalized_row.get("primarydatacaster_id", None)
    row['primarydatacaster_link'] = normalized_row.get("primarydatacaster_link", None)

    row['officialvenue_id'] = normalized_row.get("officialvenue_id", None)
    row['officialvenue_link'] = normalized_row.get("officialvenue_link", None)

    row['status_abstractgamecode'] = normalized_row.get("status_abstractgamecode", None)
    row['status_abstractgamestate'] = normalized_row.get("status_abstractgamestate", None)
    row['status_codedgamestate'] = normalized_row.get("status_codedgamestate", None)
    row['status_detailedstate'] = normalized_row.get("status_detailedstate", None)
    row['status_starttimetbd'] = normalized_row.get("status_starttimetbd", None)
    row['status_statuscode'] = normalized_row.get("status_statuscode", None)

    # teams
    row['teams_away_id'] = normalized_row.get("teams_away_id", None)
    row['teams_away_name'] = normalized_row.get("teams_away_name", None)
    row['teams_away_link'] = normalized_row.get("teams_away_link", None)
    row['teams_home_id'] = normalized_row.get("teams_home_id", None)
    row['teams_home_name'] = normalized_row.get("teams_home_name", None)
    row['teams_home_link'] = normalized_row.get("teams_home_link", None)

    # venue
    row['venue_active'] = normalized_row.get("venue_active", None)
    row['venue_fieldinfo_capacity'] = normalized_row.get("venue_fieldinfo_capacity", None)
    row['venue_fieldinfo_center'] = normalized_row.get("venue_fieldinfo_center", None)
    row['venue_fieldinfo_leftcenter'] = normalized_row.get("venue_fieldinfo_leftcenter", None)
    row['venue_fieldinfo_leftline'] = normalized_row.get("venue_fieldinfo_leftline", None)
    row['venue_fieldinfo_rightcenter'] = normalized_row.get("venue_fieldinfo_rightcenter", None)
    row['venue_fieldinfo_rightline'] = normalized_row.get("venue_fieldinfo_rightline", None)
    row['venue_fieldinfo_rooftype'] = normalized_row.get("venue_fieldinfo_rooftype", None)
    row['venue_fieldinfo_turftype'] = normalized_row.get("venue_fieldinfo_turftype", None)
    row['venue_id'] = normalized_row.get("venue_id", None)
    row['venue_link'] = normalized_row.get("venue_link", None)
    row['venue_location_address1'] = normalized_row.get("venue_location_address1", None)
    row['venue_location_address2'] = normalized_row.get("venue_location_address2", None)
    row['venue_location_city'] = normalized_row.get("venue_location_city", None)
    row['venue_location_country'] = normalized_row.get("venue_location_country", None)
    row['venue_location_defaultcoordinates_latitude'] = normalized_row.get("venue_location_defaultcoordinates_latitude", None)
    row['venue_location_phone'] = normalized_row.get("venue_location_phone", None)
    row['venue_location_postalcode'] = normalized_row.get("venue_location_postalcode", None)
    row['venue_location_state'] = normalized_row.get("venue_location_state", None)
    row['venue_location_stateabbrev'] = normalized_row.get("venue_location_stateabbrev", None)
    row['venue_name'] = normalized_row.get("venue_name", None)
    row['venue_timezone_id'] = normalized_row.get("venue_timezone_id", None)
    row['venue_timezone_offset'] = normalized_row.get("venue_timezone_offset", None)
    row['venue_timezone_tz'] = normalized_row.get("venue_timezone_tz", None)

    # weather
    row['weather_condition'] = normalized_row.get("weather_condition", None)
    row['weather_temp'] = normalized_row.get("weather_temp", None)
    row['weather_wind'] = normalized_row.get("weather_wind", None)

    # flag
    row['flags_awayteamnohitter'] = normalized_row.get("flags_awayteamnohitter", None)
    row['flags_awayteamperfectgame'] = normalized_row.get("flags_awayteamperfectgame", None)
    row['flags_hometeamnohitter'] = normalized_row.get("flags_hometeamnohitter", None)
    row['flags_hometeamperfectgame'] = normalized_row.get("flags_hometeamperfectgame", None)
    row['flags_nohitter'] = normalized_row.get("flags_nohitter", None)
    row['flags_perfectgame'] = normalized_row.get("flags_perfectgame", None)


    ## livefeed: liveData -linescore
    livefeed_livedata = livefeed_endpoint['liveData']['linescore']

    json_normalized = pd.io.json.json_normalize(livefeed_livedata, sep='_').to_dict(orient='records')[0]
    normalized_row = dict((k.lower(), v) for k, v in json_normalized.items())

    # score
    row['teams_away_errors'] = normalized_row.get("teams_away_errors", None)
    row['teams_away_hits'] = normalized_row.get("teams_away_hits", None)
    row['teams_away_leftonbase'] = normalized_row.get("teams_away_leftonbase", None)
    row['teams_away_runs'] = normalized_row.get("teams_away_runs", None)
    row['teams_home_errors'] = normalized_row.get("teams_home_errors", None)
    row['teams_home_hits'] = normalized_row.get("teams_home_hits", None)
    row['teams_home_leftonbase'] = normalized_row.get("teams_home_leftonbase", None)
    row['teams_home_runs'] = normalized_row.get("teams_home_runs", None)

    livefeed.append(row)


    ## players
    players = []
    
    for key, item in livefeed_endpoint['gameData']['players'].items():
        players.append(item['link'])

    return livefeed, players



def get_players_statsapi(link):

    apicall = 'https://statsapi.mlb.com' + link
    statapi = requests.get(apicall)
    people_endpoint = json.loads(statapi.text)

    player = people_endpoint['people'][0]

    json_normalized = pd.io.json.json_normalize(player, sep='_').to_dict(orient='records')[0]
    normalized_row = dict((k.lower(), v) for k, v in json_normalized.items())

    players = []

    row = {}

    # bio
    row['id'] = normalized_row.get("id", None)
    row['birthcity'] = normalized_row.get("birthcity", None)
    row['birthcountry'] = normalized_row.get("birthcountry", None)
    row['birthdate'] = normalized_row.get("birthdate", None)
    row['birthstateprovince'] = normalized_row.get("birthstateprovince", None)
    row['boxscorename'] = normalized_row.get("boxscorename", None)
    row['currentage'] = normalized_row.get("currentage", None)
    row['active'] = normalized_row.get("active", None)
    row['gender'] = normalized_row.get("gender", None)
    row['height'] = normalized_row.get("height", None)
    row['weight'] = normalized_row.get("weight", None)
    row['draftyear'] = normalized_row.get("draftyear", None)
    row['isplayer'] = normalized_row.get("isplayer", None)
    row['isverified'] = normalized_row.get("isverified", None)
    row['link'] = normalized_row.get("link", None)
    row['mlbdebutdate'] = normalized_row.get("mlbdebutdate", None)
    row['primarynumber'] = normalized_row.get("primarynumber", None)
    row['strikezonetop'] = normalized_row.get("strikezonetop", None)
    row['strikezonebottom'] = normalized_row.get("strikezonebottom", None)

    # primary position
    row['primaryposition_abbreviation'] = normalized_row.get("primaryposition_abbreviation", None)
    row['primaryposition_code'] = normalized_row.get("primaryposition_code", None)
    row['primaryposition_name'] = normalized_row.get("primaryposition_name", None)
    row['primaryposition_type'] = normalized_row.get("primaryposition_type", None)

    # handedness
    row['batside_code'] = normalized_row.get("batside_code", None)
    row['batside_description'] = normalized_row.get("batside_description", None)
    row['pitchhand_code'] = normalized_row.get("pitchhand_code", None)
    row['pitchhand_description'] = normalized_row.get("pitchhand_description", None)

    # name
    row['firstlastname'] = normalized_row.get("firstlastname", None)
    row['firstname'] = normalized_row.get("firstname", None)
    row['fullfmlname'] = normalized_row.get("fullfmlname", None)
    row['fulllfmname'] = normalized_row.get("fulllfmname", None)
    row['fullname'] = normalized_row.get("fullname", None)
    row['initlastname'] = normalized_row.get("initlastname", None)
    row['lastfirstname'] = normalized_row.get("lastfirstname", None)
    row['lastinitname'] = normalized_row.get("lastinitname", None)
    row['lastname'] = normalized_row.get("lastname", None)
    row['middlename'] = normalized_row.get("middlename", None)
    row['namefirstlast'] = normalized_row.get("namefirstlast", None)
    row['nameslug'] = normalized_row.get("nameslug", None)
    row['usename'] = normalized_row.get("usename", None)

    players.append(row)

    return players


def get_teams_statsapi(link):

    apicall = 'https://statsapi.mlb.com' + link
    statapi = requests.get(apicall)
    teams_endpoint = json.loads(statapi.text)

    team = teams_endpoint['teams'][0]

    json_normalized = pd.io.json.json_normalize(team, sep='_').to_dict(orient='records')[0]
    normalized_row = dict((k.lower(), v) for k, v in json_normalized.items())

    teams = []

    row = {}

    # club info
    row['id'] = normalized_row.get("id", None)
    row['teamname'] = normalized_row.get("teamname", None)
    row['shortname'] = normalized_row.get("shortname", None)
    row['teamcode'] = normalized_row.get("teamcode", None)
    row['name'] = normalized_row.get("name", None)
    row['locationname'] = normalized_row.get("locationname", None)
    row['season'] = normalized_row.get("season", None)
    row['link'] = normalized_row.get("link", None)
    row['franchisename'] = normalized_row.get("franchisename", None)
    row['firstyearofplay'] = normalized_row.get("firstyearofplay", None)
    row['filecode'] = normalized_row.get("filecode", None)
    row['clubname'] = normalized_row.get("clubname", None)
    row['allstarstatus'] = normalized_row.get("allstarstatus", None)
    row['abbreviation'] = normalized_row.get("abbreviation", None)
    row['active'] = normalized_row.get("active", None)

    # venue
    row['venue_id'] = normalized_row.get("venue_id", None)
    row['venue_link'] = normalized_row.get("venue_link", None)
    row['venue_name'] = normalized_row.get("venue_name", None)

    # league
    row['league_id'] = normalized_row.get("league_id", None)
    row['league_link'] = normalized_row.get("league_link", None)
    row['league_name'] = normalized_row.get("league_name", None)

    # division
    row['division_id'] = normalized_row.get("division_id", None)
    row['division_link'] = normalized_row.get("division_link", None)
    row['division_name'] = normalized_row.get("division_name", None)

    # sport
    row['sport_id'] = normalized_row.get("sport_id", None)
    row['sport_link'] = normalized_row.get("sport_link", None)
    row['sport_name'] = normalized_row.get("sport_name", None)

    # springleague
    row['springleague_abbreviation'] = normalized_row.get("springleague_abbreviation", None)
    row['springleague_id'] = normalized_row.get("springleague_id", None)
    row['springleague_link'] = normalized_row.get("springleague_link", None)
    row['springleague_name'] = normalized_row.get("springleague_name", None)
    row['springvenue_id'] = normalized_row.get("springvenue_id", None)
    row['springvenue_link'] = normalized_row.get("springvenue_link", None)

    teams.append(row)

    return teams