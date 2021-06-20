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



def get_playbyplay_statsapi(gamepk):

    apicall = 'https://statsapi.mlb.com/api/v1/game/' + str(gamepk) + '/playByPlay'
    statapi = requests.get(apicall)
    playbyplay = json.loads(statapi.text)

    venue = []
    
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
            

                        venue.append(row)

    return venue