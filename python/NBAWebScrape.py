import requests

class NBAWebScrape(object):
    def obtainPlayerIDs(self):
        player_info_url = 'http://stats.nba.com/stats/commonallplayers?IsOnlyCurrentSeason=1&LeagueID=00&Season=2015-16'
        player_info = requests.get(player_info_url, headers={'User-Agent': 'firefox'}).json()
        all_players = []
        for player in player_info['resultSets'][0]['rowSet']:
            info = []
            full_name = str(player[5]).split('_')

        # THE FOLLOWING IF ELSE STATEMENTS WILL HANDLE THE DISCREPANCIES BETWEEN PLAYERS NAMES
            if full_name[0] == 'HISTADD':
                if len(full_name) < 3:
                    first_name = full_name[1]
                    middle_name = "NULL"
                    last_name = "NULL"
                elif len(full_name) > 3:
                    first_name = full_name[1]
                    middle_name = full_name[2]
                    last_name = full_name[3]
                else:
                    first_name = full_name[1]
                    middle_name = "NULL"
                    last_name = full_name[2]

            else:
                if len(full_name) < 2:
                    first_name = full_name[0]
                    middle_name = "NULL"
                    last_name = "NULL"
                elif len(full_name) > 2:
                    first_name = full_name[0]
                    middle_name = full_name[1]
                    last_name = full_name[2]
                else:
                    first_name = full_name[0]
                    middle_name = "NULL"
                    last_name = full_name[1]

            player_id = player[0]
            from_year = player[3]
            to_year = player[4]

            info.append(player_id)
            info.append(first_name)
            info.append(middle_name)
            info.append(last_name)
            info.append(from_year)
            info.append(to_year)
            all_players.append(info)
        return all_players

    def getGameLogsOfPlayer(self, playerID, season='2015-16'):
        game_log_url = 'http://stats.nba.com/stats/playergamelog?' \
                 'LeagueID=00&' \
                 'PerMode=PerGame&' \
                 'PlayerID=' + str(playerID) + '&' \
                 'Season=' + season + '&' \
                 'SeasonType=Regular+Season'
        game_log_json = requests.get(game_log_url, headers={'User-Agent': 'firefox'}).json()
        game_log_result = game_log_json['resultSets'][0]['rowSet']
        return game_log_result
