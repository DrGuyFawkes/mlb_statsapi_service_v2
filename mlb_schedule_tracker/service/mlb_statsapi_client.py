import json
import requests


def get_schedule_statsapi(sport_ids, date_str):

    for sport_id in sport_ids:
        schedule_endpoint = "https://statsapi.mlb.com/api/v1/schedule/?sportId=" + sport_id + "&date=" + date_str
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

def sports_abbreviation(sport_id):
    sportid_endpoint = "https://statsapi.mlb.com/api/v1/sports/" + sport_id
    statapi = requests.get(sportid_endpoint)
    sportid = json.loads(statapi.text)
    
    abbreviation = sportid['sports'][0]['abbreviation']

    return abbreviation