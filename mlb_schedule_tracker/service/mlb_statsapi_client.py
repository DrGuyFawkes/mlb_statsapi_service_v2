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
        schedule_api = "https://statsapi.mlb.com/api/v1/schedule/?sportId=" + level + "&date=" + date_str
        statapi = requests.get(schedule_api)
        schedule = json.loads(statapi.text)

        games = []
        for date_games in schedule['dates']:
            if 'games' in date_games.keys():
                for game in date_games['games']:
                    game['gameLevel'] = level_name(level)
                    game_status = game['status']
                    game['gameStatus'] = game['status']['abstractGameState']

                    # Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: status
                    del game['status']

                    games.append(game)
    
    return games


def level_name(level_code):
    if level_code == '1':
        level_name = "MLB"
    elif level_code == '11':
        level_name = "AAA"
    elif level_code == '12':
        level_name = "AA"
    elif level_code == '13':
        level_name = "High A"
    elif level_code == '14':
        level_name = "Low A"
    elif level_code == '15':
        level_name = "ShortSeason A"
    elif level_code == '5442':
        level_name = "Rookie Advanced"
    elif level_code == '16':
        level_name = "Rookie"
    else:
        level_name = "unknown"
    return level_name