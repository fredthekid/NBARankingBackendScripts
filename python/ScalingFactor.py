import MySQLdb

class ScalingFactor(object):
    def __getAverages(self, stat, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        query = ("SELECT AVG(" + stat + ")"
                    "FROM " + self.tablename +
                     ";")
        cur.execute(query)
        db_get = cur.fetchone()
        return db_get[0]

    def __getSTDEV(self, stat, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        std = []
        query = ("SELECT STDDEV(" + stat + ") "
            "FROM " + self.tablename +
            ";")
        cur.execute(query)
        db_get = cur.fetchone()
        return db_get[0]

    def __init__(self, tablename=None):
        self.stats = ['fgm', 'fga', 'ftm', 'fta', 'fg3m', 'fg3a']
        self.stat_avg = {}
        self.stat_std = {}
        self.fgm_dict = {}
        self.fga_dict = {}
        self.ftm_dict = {}
        self.fta_dict = {}
        self.fg3m_dict = {}
        self.fg3a_dict = {}
        self.min_offset = 0
        self.tablename = tablename
        if tablename is not None:
            self.calculateScalingFactors(tablename)

    # pass in table name as parameter
    def calculateScalingFactors(self, tablename=None):
        if tablename is None:
            print "This function requires a 'tablename' parameter"
            return
        self.tablename = tablename
        self.stat_avg.clear()
        self.stat_std.clear()
        self.fgm_dict.clear()
        self.fga_dict.clear()
        self.ftm_dict.clear()
        self.fta_dict.clear()
        self.fg3m_dict.clear()
        self.fg3a_dict.clear()
        for stat in self.stats:
            self.stat_avg[str(stat)] = self.__getAverages(stat)
            self.stat_std[str(stat)] = self.__getSTDEV(stat)
        self.__calcFGMDict()
        self.__calcFGADict()

        self.__calcFG3MDict()
        self.__calcFG3ADict()

        self.__calcFTMList()
        self.__calcFTMList()

    def __getPlayerIDs(self, season, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        idList = []
        cur.execute("SELECT * FROM id_player WHERE to_year=" + str(season) + " AND from_year<=" + str(season) + ";")
        for row in cur:
            idList.append(int(row[0]))
        return idList

    def __calcFTMList(self, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        player_id = self.__getPlayerIDs(2015)
        for playerID in player_id:
            query = ("SELECT ftm FROM " + str(self.tablename) + " WHERE player_id=" + str(playerID) + ";")
            cur.execute(query)
            player_avg = cur.fetchone()[0]
            val = 0.000
            if player_avg is not None:
                val = (player_avg - self.stat_avg['ftm']) / self.stat_std['ftm']

            else:
                val = 0.0000 - self.stat_avg['ftm'] / self.stat_std['ftm']

            self.ftm_dict[str(playerID)] = val
        minimum = min(self.ftm_dict, key=self.ftm_dict.get)
        minimum = abs(self.ftm_dict[minimum]) + self.min_offset
        for playerID in player_id:
            self.ftm_dict[str(playerID)] += minimum

    def __calcFTAList(self, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        player_id = self.__getPlayerIDs(2015)
        for playerID in player_id:
            query = ("SELECT fta FROM " + str(self.tablename) + " WHERE player_id=" + str(playerID) + ";")
            cur.execute(query)
            player_avg = cur.fetchone()[0]
            val = 0.000
            if player_avg is not None:
                val = (player_avg - self.stat_avg['fta']) / self.stat_std['fta']

            else:
                val = 0.0000 - self.stat_avg['fta'] / self.stat_std['fta']
            self.fta_dict[str(playerID)] = val
        minimum = min(self.fta_dict, key=self.fta_dict.get)
        minimum = abs(self.fta_dict[minimum]) + self.min_offset
        for playerID in player_id:
            self.fta_dict[str(playerID)] += minimum

    def __calcFGMDict(self, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        player_id = self.__getPlayerIDs(2015)
        for playerID in player_id:
            query = ("SELECT fgm FROM " + str(self.tablename) + " WHERE player_id=" + str(playerID) + ";")
            cur.execute(query)
            player_avg = cur.fetchone()[0]
            val = 0.000
            if player_avg is not None:
                val = (player_avg - self.stat_avg['fgm']) / self.stat_std['fgm']

            else:
                val = 0.0000 - self.stat_avg['fgm'] / self.stat_std['fgm']
            self.fgm_dict[str(playerID)] = val
        minimum = min(self.fgm_dict, key=self.fgm_dict.get)
        minimum = abs(self.fgm_dict[minimum]) + self.min_offset
        for playerID in player_id:
            self.fgm_dict[str(playerID)] += minimum

    def __calcFGADict(self, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        player_id = self.__getPlayerIDs(2015)
        for playerID in player_id:
            query = ("SELECT fga FROM " + str(self.tablename) + " WHERE player_id=" + str(playerID) + ";")
            cur.execute(query)
            player_avg = cur.fetchone()[0]
            val = 0.000
            if player_avg is not None:
                val = (player_avg - self.stat_avg['fga']) / self.stat_std['fga']

            else:
                val = 0.0000 - self.stat_avg['fga'] / self.stat_std['fga']
            self.fga_dict[str(playerID)] = val
        minimum = min(self.fga_dict, key=self.fga_dict.get)
        minimum = abs(self.fga_dict[minimum]) + self.min_offset
        for playerID in player_id:
            self.fga_dict[str(playerID)] += minimum

    def __calcFG3MDict(self, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        player_id = self.__getPlayerIDs(2015)
        for playerID in player_id:
            query = ("SELECT fg3m FROM " + str(self.tablename) + " WHERE player_id=" + str(playerID) + ";")
            cur.execute(query)
            player_avg = cur.fetchone()[0]
            val = 0.000
            if player_avg is not None:
                val = (player_avg - self.stat_avg['fg3m']) / self.stat_std['fg3m']

            else:
                val = 0.0000 - self.stat_avg['fg3m'] / self.stat_std['fg3m']
            self.fg3m_dict[str(playerID)] = val
        minimum = min(self.fg3m_dict, key=self.fg3m_dict.get)
        minimum = abs(self.fg3m_dict[minimum]) + self.min_offset
        for playerID in player_id:
            self.fg3m_dict[str(playerID)] += minimum

    def __calcFG3ADict(self, host_name="localhost", user_name="fred", database="startwinning"):
        db = MySQLdb.connect(host=host_name,  user=user_name,  db=database)
        cur = db.cursor()
        player_id = self.__getPlayerIDs(2015)
        for playerID in player_id:
            query = ("SELECT fg3a FROM " + str(self.tablename) + " WHERE player_id=" + str(playerID) + ";")
            cur.execute(query)
            player_avg = cur.fetchone()[0]
            val = 0.000
            if player_avg is not None:
                val = (player_avg - self.stat_avg['fg3a']) / self.stat_std['fg3a']

            else:
                val = 0.0000 - self.stat_avg['fg3a'] / self.stat_std['fg3a']
            self.fg3a_dict[str(playerID)] = val
        minimum = min(self.fg3a_dict, key=self.fg3a_dict.get)
        minimum = abs(self.fg3a_dict[minimum]) + self.min_offset
        for playerID in player_id:
            self.fg3a_dict[str(playerID)] += minimum

    def getScalingFactor(self, stat, playerID):
        if stat == 'fgm':
            return self.fgm_dict[str(playerID)]
        elif stat == 'fga':
            return self.fga_dict[str(playerID)]
        elif stat == 'ftm':
            return self.ftm_dict[str(playerID)]
        elif stat == 'fta':
            return self.fta_dict[str(playerID)]
        elif stat ==  'fg3m':
            return self.fg3m_dict[str(playerID)]
        elif stat == 'fg3a':
            return self.fg3a_dict[str(playerID)]
