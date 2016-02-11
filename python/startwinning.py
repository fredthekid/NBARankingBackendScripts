# this file contains the base of the api.
# only 3 functions should be public: update_gamelogs(), update_averages(), and update_rankings()
# everything else should not be used outside this class

# there are 4 logical components in this file.
# 1. calculations (averages, standard deviation, rankings)
# 2. CRUD operations using SQL (to alleviate traffic from using Django REST Framework)
# 3. Information scrapped NBAWebScrapeAPI
# 4. Ranking Update was previously made as it's own class. too lazy to integrate in so a new object is created when ranking needs to be updated

import MySQLdb
import datetime as dt
from NBAWebScrape import NBAWebScrape
from RankingUpdate import RankingCalc


class StartWinningAPI(object):
    def __init__(self, season):
        self.season = season

        # table names
        self.gamelogs = "GAMELOG_" + str(season)

        # key = table name, value = time period
        self.average_dict = {
            "AVERAGE_ALL_" + str(season): 'ALL',
            "AVERAGE_3M_" + str(season): '3M',
            "AVERAGE_2M_" + str(season): '2M',
            "AVERAGE_1M_" + str(season): '1M',
            "AVERAGE_3W_" + str(season): '3W',
            "AVERAGE_2W_" + str(season): '2W',
            "AVERAGE_1W_" + str(season): '1W'
        }

        # key = ranking table names, value = average table names
        self.ranking_dict = {
            "RANKING_ALL_" + str(season): "AVERAGE_ALL_" + str(season),
            "RANKING_3M_" + str(season): "AVERAGE_3M_" + str(season),
            "RANKING_2M_" + str(season): "AVERAGE_2M_" + str(season),
            "RANKING_1M_" + str(season): "AVERAGE_1M_" + str(season),
            "RANKING_3W_" + str(season): "AVERAGE_3W_" + str(season),
            "RANKING_2W_" + str(season): "AVERAGE_2W_" + str(season),
            "RANKING_1W_" + str(season): "AVERAGE_1W_" + str(season)
        }

    ####################
    # PUBLIC FUNCTIONS #
    ####################
    # this function should be updated later after an automation tool is deployed.
    # for now, just delete all logs and reload.
    def update_gamelogs(self):
        self._clear_gamelogs()
        nbaAPI = NBAWebScrape()
        player_ids = self._getPlayerIDsThatPlayedThisSeason()
        for player_id in player_ids:
            print player_id
            self._storeGameLog(nbaAPI.getGameLogsOfPlayer(player_id))

    # this function will update averages based off the gamelog table, so make sure the gamelog table is updated
    def update_averages(self):
        self._clear_averages()
        player_ids = self._getPlayerIDsThatPlayedThisSeason()
        for k in self.average_dict:
            for player_id in player_ids:
                print player_id
                self._storePlayerAverage(self._getPlayerAverage(player_id, self.average_dict[k]), k)

    # i wrote an update rankings as a class a long time ago, im too lazy to integrate it into this class.
    # as far as i know, there isnt a reason to, so imma be lazy as fk and use the old code :)
    def update_rankings(self):
        self._clear_rankings()
        rankingUpdate = RankingCalc()
        player_ids = self._getPlayerIDsThatPlayedThisSeason()
        for k in self.ranking_dict:
            rankingUpdate.refactorRankings(self.ranking_dict[k])
            for player in player_ids:
                ranking = rankingUpdate.calculateRanking(player)
                print ranking
                rankingUpdate.storeRankingInDB(ranking, k)

    # this function only needs to be run once when the database is initiated.
    # gets the mapping of id (based on NBA.com) to player names
    def get_Id_Of_Players(self):
        nbaAPI = NBAWebScrape()
        self._storePlayerId(nbaAPI.obtainPlayerIDs())



    #############################################################
    # PRIVATE FUNCTIONS (DONT USE THIS SHIT OUTSIDE THIS CLASS) #
    #############################################################

    # stores player id into mysql database
    def _storePlayerId(self, player_data, host_name="localhost", user_name="fred", database="startwinning", table="id_player"):
        db = MySQLdb.connect(host=host_name, user=user_name, db=database)
        cur = db.cursor()
        for player in player_data:
            try:
                cur.execute("INSERT INTO " + table + " VALUES( %s, %s, %s, %s, %s, %s);", (str(player[0]), player[1], player[2], player[3], str(player[4]), str(player[5])))
                db.commit()
            except:
                print("Error storing in database")


    # this function parses the JSON data and stores it in MySQL
    def _storeGameLog(self, game_log_result, host_name="localhost", user_name="fred", database="startwinning", table="GAMELOG_2015"):
        db = MySQLdb.connect(host=host_name, user=user_name, db=database)
        cur = db.cursor()
        for game in game_log_result:
            # Season,Game,Player IDs
            season_id = str(game[0])
            player_id = str(game[1])
            game_id = str(game[2])

            # Date Played
            date = str(game[3]).replace(",", "").split(" ")
            game_date = str(date[2]) + "-" + str(self._strDateToInteger(date[0])) + "-" + str(date[1])

            # Teams involved
            teams = str(game[4]).split(" ")
            # print teams
            home_away = 1  # 1 == home, 0 == away
            if teams[1] == '@':
                home_away = 0
            team = teams[0]
            opp_team = teams[2]

            #Win/Loss
            game_result = str(game[5])
            win_loss = '1'
            if game_result == 'L':
                win_loss = '0'

            # In-Game Stats
            minutes = str(game[6])
            fgm = str(game[7])
            fga = str(game[8])
            fg_pct = str(game[9])
            fg3m = str(game[10])
            fg3a = str(game[11])
            fg3_pct = str(game[12])
            if fg3a == "0" or fg3a == "0.0":
                fg3_pct = None
            ftm = str(game[13])
            fta = str(game[14])
            ft_pct = str(game[15])
            if fta == "0" or fta == "0.0":
                ft_pct = None
            oreb = str(game[16])
            dreb = str(game[17])
            reb = str(game[18])
            ast = str(game[19])
            stl = str(game[20])
            blk = str(game[21])
            tov = str(game[22])
            aot = ""
            if tov != "0":
                aot = str(float(float(ast)/float(tov)))
            else:
                aot = None
            foul = str(game[23])
            pts = str(game[24])
            plus_minus = str(game[25])
            try:
                cur.execute("INSERT INTO " + table + "("
                    "season_id, player_id, game_id, game_date,"
                    "home_away, team, opp_team, win_loss, min, fgm,"
                    "fga, fg_pct, fg3m, fg3a, fg3_pct, ftm,"
                    "fta, ft_pct, oreb,dreb, reb, ast,"
                    "stl, blk, tov, aot, foul, pts, plus_minus)"
                    "VALUES("
                    "%s, %s, %s, %s,"
                    " %s, %s, %s, %s, %s, %s,"
                    " %s, %s, %s, %s, %s, %s,"
                    " %s, %s, %s, %s, %s, %s,"
                    " %s, %s, %s, %s, %s, %s, %s);",
                    (season_id, player_id, game_id, game_date,
                     home_away, team, opp_team, win_loss, minutes, fgm,
                     fga, fg_pct, fg3m, fg3a, fg3_pct, ftm,
                     fta,  ft_pct, oreb, dreb, reb, ast,
                     stl, blk, tov, aot, foul, pts, plus_minus))
                db.commit()
            except:
                print("Error storing in database")

    def _getPlayerAverage(self, playerID, timeframe, host_name='localhost', user_name='fred', database='startwinning'):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        end_date = self._getTodaysDate()
        start_date = self._getDateOffSet(timeframe)
        cur.execute("SELECT first_name, last_name "
                    "FROM id_player "
                    "WHERE player_id = " + str(playerID) + ";")
        db_read = cur.fetchone()
        first_name = db_read[0]
        last_name = db_read[1]
        q = "SELECT COUNT(*), AVG(min), AVG(fgm), AVG(fga), SUM(fgm)/SUM(fga), " \
            + "AVG(fg3m), AVG(fg3a), SUM(fg3m)/SUM(fg3a), AVG(ftm), AVG(fta), SUM(ftm)/SUM(fta), " \
            + "AVG(oreb), AVG(dreb), AVG(reb), AVG(ast), AVG(stl), AVG(blk)," \
            + "AVG(tov), AVG(ast)/AVG(tov), AVG(foul), AVG(pts), AVG(plus_minus) " \
            + "FROM GAMELOG_2015 " \
            + "WHERE player_id=" + str(playerID) + " and game_date between \"" + start_date + "\" and \"" + end_date + "\";"
        cur.execute(q)
        db_read = cur.fetchone()
        db_average = []
        db_average.append(playerID)
        db_average.append(first_name)
        db_average.append(last_name)
        for element in db_read:
            db_average.append(element)
        return db_average

    def _storePlayerAverage(self, averages, table, host_name='localhost', user_name='fred', database='startwinning'):
        db = MySQLdb.connect(host=host_name, user=user_name, db=database)
        cur = db.cursor()
        averages_refact = []
        for stat in averages:
            if stat is not None:
                averages_refact.append(str(stat))
            else:
                averages_refact.append(None)

        try:
            cur.execute( "INSERT INTO " + table + "("
                    "player_id, first_name, last_name, games_played, min,"
                    "fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,"
                    "ftm, fta, ft_pct, oreb, dreb, reb,"
                    "ast, stl, blk, tov, aot, foul,""pts, plus_minus)"
                    "VALUES("
                    "%s, %s, %s, %s, %s,"
                    " %s, %s, %s, %s, %s, %s,"
                    " %s, %s, %s, %s, %s, %s,"
                    " %s, %s, %s, %s, %s, %s,"
                    " %s, %s);",
                     (averages_refact[0], averages_refact[1], averages_refact[2], averages_refact[3], averages_refact[4],
                      averages_refact[5], averages_refact[6], averages_refact[7], averages_refact[8], averages_refact[9], averages_refact[10],
                      averages_refact[11], averages_refact[12], averages_refact[13], averages_refact[14], averages_refact[15], averages_refact[16],
                      averages_refact[17], averages_refact[18], averages_refact[19], averages_refact[20], averages_refact[21], averages_refact[22],
                      averages_refact[23], averages_refact[24]))
            db.commit()
        except:
            print("Error storing in database")

    def _clear_gamelogs(self):
        print "Clearing Game Logs..."
        db = MySQLdb.connect(host='localhost', user='fred', db='startwinning')
        cur = db.cursor()
        cur.execute("DELETE FROM " + self.gamelogs + ";")
        db.commit()
        print "Done."

    def _clear_averages(self):
        print "Clearing Averages..."
        db = MySQLdb.connect(host='localhost', user='fred', db='startwinning')
        cur = db.cursor()
        for k in self.average_dict:
            cur.execute("DELETE FROM " + str(k) + ";")
            db.commit()
        print "Done."

    def _clear_rankings(self):
        print "Clearing Rankings..."
        db = MySQLdb.connect(host='localhost', user='fred', db='startwinning')
        cur = db.cursor()
        for k in self.ranking_dict:
            cur.execute("DELETE FROM " + str(k) + ";")
            db.commit()
        print "Done."

    # 'This Season' is 2015. update this function later to have the year passed in as parameter
    def _getPlayerIDsThatPlayedThisSeason(self):
        db = MySQLdb.connect(host='localhost', user='fred', db='startwinning')
        cur = db.cursor()
        cur.execute("SELECT player_id FROM id_player WHERE to_year = 2015 && from_year <= 2015;")
        player_id_list = []
        for player_id in cur:
            player_id_list.append(player_id[0])
        return player_id_list

    def _strDateToInteger(self, date):
        date = date.upper()
        if date == "JAN":
            return 1
        elif date == "FEB":
            return 2
        elif date == "MAR":
            return 3
        elif date == "APR":
            return 4
        elif date == "MAY":
            return 5
        elif date == "JUN":
            return 6
        elif date == "JUL":
            return 7
        elif date == "AUG":
            return 8
        elif date == "SEP":
            return 9
        elif date == "OCT":
            return 10
        elif date == "NOV":
            return 11
        elif date == "DEC":
            return 12
        else:
            print "Error in converting string date into integer"
            return None

    def _getTodaysDate(self):
        return str(dt.date.today())

    def _getDateOffSet(self, timeframe):
        today = dt.date.today()
        if  timeframe == "ALL":
            return "2015-10-01"
        elif timeframe == '3M':
            return str(today - dt.timedelta(days=90))
        elif timeframe == '2M':
            return str(today - dt.timedelta(days=60))
        elif timeframe == '1M':
            return str(today - dt.timedelta(days=30))
        elif timeframe == '3W':
            return str(today - dt.timedelta(days=21))
        elif timeframe == '2W':
            return str(today - dt.timedelta(days=14))
        elif timeframe == "1W":
            return str(today - dt.timedelta(days=7))
        else:
            print "Error calculating date offset"
            return None
