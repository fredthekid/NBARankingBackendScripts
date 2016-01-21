# Future fixes: instead of using return arrays for the functions, change it to key-value pairs
import MySQLdb
from ScalingFactor import ScalingFactor


class RankingCalc(object):

    def __init__(self):
        self.tablename = None
        self.averages = None
        self.stddev = None

    def refactorRankings(self, tablename):
        self.tablename = tablename
        self.getAllAverages()
        self.getSTDEV()

    def getPlayerIDs(self, season, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        idList = []
        cur.execute("SELECT * FROM id_player WHERE to_year=" + str(season) + " AND from_year<=" + str(season) + ";")
        for row in cur:
            idList.append(int(row[0]))
        return idList

    # This function will get the averages of all players
    def getAllAverages(self, host_name="localhost", user_name="fred", database="startwinning"):
        if self.tablename is None:
            print "update tablename"
            return

        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        avg = []
        query = ("SELECT AVG(games_played), AVG(min), AVG(fgm),"
                    "AVG(fg_pct), AVG(fg3m),"
                    "AVG(fg3_pct),AVG(ftm), AVG(ft_pct),"
                    "AVG(reb), AVG(ast), AVG(stl), AVG(blk),"
                    "AVG(tov), AVG(aot), AVG(pts) "
                    "FROM " + self.tablename +
                     ";")
        cur.execute(query)
        db_get = cur.fetchone()
        for stat in db_get:
            avg.append(float(stat))
        self.averages = avg
        return avg

    # This function will get the standard deviation of all players
    def getSTDEV(self, host_name="localhost", user_name="fred", database="startwinning"):
        if self.tablename is None:
            print "update tablename"
            return

        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        std = []
        query = ("SELECT STDDEV(games_played), STDDEV(min), STDDEV(fgm),"
                "STDDEV(fg_pct), STDDEV(fg3m),"
                "STDDEV(fg3_pct),STDDEV(ftm), STDDEV(ft_pct),"
                "STDDEV(reb),  STDDEV(ast), STDDEV(stl), STDDEV(blk),"
                "STDDEV(tov), STDDEV(aot), STDDEV(pts) "
                "FROM " + self.tablename +
                 ";")
        cur.execute(query)
        db_get = cur.fetchone()
        for stat in db_get:
            std.append(float(stat))
        self.stddev = std
        return std

    def calculateRanking(self, playerID, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        cur.execute("SELECT first_name, last_name FROM id_player WHERE player_id=" + str(playerID) + ";")
        ranking_schema = []
        db_read = cur.fetchone()
        ranking_schema.append(playerID)
        ranking_schema.append(db_read[0])
        ranking_schema.append(db_read[1])

        query = ("SELECT games_played, min, fgm,"
                "fg_pct, fg3m,"
                "fg3_pct, ftm, ft_pct,"
                "reb, ast, stl, blk,"
                "tov, aot, pts "
                "FROM " + self.tablename +
                " WHERE player_id=" + str(playerID) + ";")
        cur.execute(query)
        db_read = cur.fetchone()
        player_avg = []
        for stat in db_read:
            if stat is not None:
                player_avg.append(float(stat))
            else:
                player_avg.append(None)

        sf = ScalingFactor(self.tablename)
        fgm = sf.getScalingFactor('fgm', playerID)
        fg3m = sf.getScalingFactor('fg3m', playerID)
        ftm = sf.getScalingFactor('ftm', playerID)

        for i in range(0, len(player_avg)):
            if player_avg[i] is not None:
                rank = float((float(player_avg[i]) - self.averages[i])/self.stddev[i])
                # if stat is FG%, scale by FGM
                if i == 3:
                    rank *= fgm
                # if stat is FG%, scale by FGM
                if i == 5:
                    rank *= fg3m
                # if stat is FG%, scale by FGM
                if i == 7:
                    rank *= ftm
                # if stat is TOV
                if i == 12:
                    rank *= -1
                ranking_schema.append(rank)
            else:
                ranking_schema.append(0.0000)
        return ranking_schema

    def storeRankingInDB(self, ranking, table_name, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        ranking_refact = []
        for rank in ranking:
            ranking_refact.append(str(rank))
        print "Storing " + ranking_refact[1] + " " + ranking_refact[2]
        try:
            cur.execute( "INSERT INTO " + table_name + "("
                        "player_id, first_name, last_name, games_played, min,"
                        "fgm, fg_pct, fg3m, fg3_pct,"
                        "ftm, ft_pct, reb,"
                        "ast, stl, blk, tov, aot, pts)"
                        "VALUES("
                        "%s, %s, %s, %s, %s,"
                        " %s, %s, %s, %s,"
                        " %s, %s, %s,"
                        " %s, %s, %s, %s, %s, %s);",
                         (ranking_refact[0], ranking_refact[1], ranking_refact[2], ranking_refact[3], ranking_refact[4],
                          ranking_refact[5], ranking_refact[6], ranking_refact[7], ranking_refact[8], ranking_refact[9], ranking_refact[10],
                          ranking_refact[11], ranking_refact[12], ranking_refact[13], ranking_refact[14], ranking_refact[15], ranking_refact[16],
                          ranking_refact[17]))
            db.commit()
        except:
            print "Unable to store " + ranking_refact[1] + " " + ranking_refact[2]
