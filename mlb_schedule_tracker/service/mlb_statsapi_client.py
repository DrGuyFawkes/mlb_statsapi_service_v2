import json
import requests


def get_schedule_statsapi(date_str):
        
    # Get schedule
    # list up all levels
    # MLB = '1'
    # AAA = '11'
    # AA = '12'
    # High A = '13'
    # Low A = '14'
    # ShortSeason A = '15'
    # Rookie Advanced = '5442'
    # Rookie = '16':

    #levels = ['1','11','12','13','14','15','5442','16']
    levels = ['1']

    for level in levels:
        schedule_endpoint = "https://statsapi.mlb.com/api/v1/schedule/?sportId=" + level + "&date=" + date_str
        statapi = requests.get(schedule_endpoint)
        schedule = json.loads(statapi.text)

        games = []
        for date_games in schedule['dates']:
            if 'games' in date_games.keys():
                for game in date_games['games']:
                    game['gameLevel'] = sports_abbreviation(level)
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