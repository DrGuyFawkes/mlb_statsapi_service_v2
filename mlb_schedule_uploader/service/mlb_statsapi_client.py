import json
import requests


def get_schedule_statsapi(record):

    game_pk = record['game_pk']
    sport_id = record['sport_id']
        
    schedule_endpoint = "https://statsapi.mlb.com/api/v1/schedule/?sportId=" + sport_id + "&gamePk=" + game_pk
    statapi = requests.get(schedule_endpoint)
    schedule = json.loads(statapi.text)

    games = []
    for date_games in schedule['dates']:
        if 'games' in date_games.keys():
            for game in date_games['games']:
                game['gameLevel'] = sports_abbreviation(sport_id)
                game_status = game['status']
                game['gameStatus'] = game['status']['abstractGameState']

                # Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: status
                del game['status']

                games.append(game)
    
    return games

def sports_abbreviation(level_code):
    sportid_endpoint = "https://statsapi.mlb.com/api/v1/sports/" + level_code
    statapi = requests.get(sportid_endpoint)
    sportid = json.loads(statapi.text)
    
    abbreviation = sportid['sports'][0]['abbreviation']

    return abbreviation