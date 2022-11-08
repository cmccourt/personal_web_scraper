import traceback

import pandas as pd
import psycopg2
from psycopg2 import _psycopg, sql, extras, pool

from settings.settings import EIHLStatsDBConfig


class PostgresDB:
    db_conn_pool = None

    def __init__(self):
        print("Create Postgres DB object")
        if PostgresDB.db_conn_pool is None:
            self.create_db_conn_pool()
        self.db_conn = self.db_conn_pool.getconn()

    def __del__(self):
        print("Delete Postgres DB object")
        self.db_conn_pool.putconn(self.db_conn)

    @classmethod
    def create_db_conn_pool(cls):
        cls.db_conn_pool = pool.SimpleConnectionPool(1, 20, user=EIHLStatsDBConfig.un,
                                                     password=EIHLStatsDBConfig.pw,
                                                     host=EIHLStatsDBConfig.hostname,
                                                     port="5432",
                                                     database=EIHLStatsDBConfig.db)

    @classmethod
    def release_db_conn_pool(cls):
        if cls.db_conn_pool is not None:
            cls.db_conn_pool.closeall()
        print("Postgres DB Pool is closed")

    def as_string(self, _sql):
        if self.db_conn:
            text_string = _sql.as_string(self.db_conn)
            return text_string

    def print_sql_query(self, query, params):
        if self.db_conn:
            print(self.db_conn.mogrify(query, params))
        else:
            print("ERROR unable to print SQL! No DB connection available!\n")

    def read_all_data(self, query, params=None):
        db_cur = None
        try:
            if self.db_conn:
                db_cur = self.db_conn.cursor(cursor_factory=extras.DictCursor)
                db_cur.execute(query, params)
                result = db_cur.fetchall()
                return result
        except Exception:
            traceback.print_exc()
        finally:
            if db_cur is not None:
                db_cur.close()

    def execute_query(self, query, params=None):
        db_cur = None
        try:
            if self.db_conn:
                db_cur = self.db_conn.cursor(cursor_factory=extras.DictCursor)
                db_cur.execute(query, params)
        except Exception:
            traceback.print_exc()
        else:
            self.db_conn.commit()
        finally:
            if db_cur is not None:
                db_cur.close()

    @staticmethod
    def get_db_conn():
        db_conn = PostgresDB.db_conn_pool.getconn()
        try:
            yield db_conn
        except Exception:
            db_conn.rollback()
        finally:
            if db_conn is not None:
                PostgresDB.db_conn_pool.putconn(db_conn)

    @staticmethod
    def db_cursor(db_conn: _psycopg.connection = None) -> _psycopg.cursor:
        db_cur = None
        if db_conn is None:
            db_conn = PostgresDB.get_db_conn()

        try:
            db_cur = db_conn.cursor(cursor_factory=extras.DictCursor)
        except psycopg2.Error:
            print("ERROR creating DB Cursor!")
        try:
            yield db_cur
        except Exception:
            traceback.print_exc()
        finally:
            if db_cur is not None:
                db_cur.close()

def get_next_match(db_cur: _psycopg.cursor = None, db_conn: _psycopg.connection = None):
    if db_conn is None:
        db_conn = PostgresDB.get_db_conn()
    if db_cur is None:
        db_cur = PostgresDB.db_cursor(db_conn)

    try:
        db_cur.execute("SELECT * FROM match;")
        while True:
            match = db_cur.fetchone()
            if match is None:
                break
            else:
                yield match
        yield None
    except Exception:
        traceback.print_exc()
    finally:
        if db_cur is not None:
            db_cur.close()


def get_eihl_season_ids(db_object=None):
    if db_object is None:
        db_object = PostgresDB()
    try:
        seasons = db_object.read_all_data("SELECT champion_id, eihl_web_id FROM championship")
    except psycopg2.ProgrammingError:
        seasons = None
    return seasons


def insert_championship(champ: dict, db_object=None):
    if db_object is None:
        db_object = PostgresDB()

    where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                     sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                       champ.items()]))
    dup_match_sql = sql.SQL("SELECT * FROM championship WHERE {}".format(
        db_object.as_string(where_clause)
    ))
    dup_matches = db_object.read_all_data(dup_match_sql, champ)

    try:
        if len(dup_matches) == 0:
            insert_sql = sql.SQL("INSERT INTO championship ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, champ)),
                sql.SQL(", ").join(map(sql.Placeholder, champ))
            )
            db_object.execute_query(insert_sql, champ)
    except Exception:
        traceback.print_exc()
    else:
        print("Championship has been inserted!")
    finally:
        del db_object


def insert_team_match_stats(team_match_stats: dict, db_object=None, update_exist_data: bool = False):
    if db_object is None:
        db_object = PostgresDB()

    team_name = team_match_stats.get("team_name", None)
    match_id = team_match_stats.get("match_id", None)
    where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                     sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                       team_match_stats.items()]))
    dup_match_sql = sql.SQL("SELECT * FROM match_team_stats WHERE {}".format(
        db_object.as_string(where_clause)
    ))
    dup_matches = db_object.read_all_data(dup_match_sql, team_match_stats)

    query = None
    try:
        if len(dup_matches) == 0:
            # Find duplicates using match ID and team name only
            # Change where clause in case there is dup matches

            where_clause = """"match_id"=%(match_id)s AND "team_name"=%(team_name)s"""
            dup_match_sql = sql.SQL("""SELECT * FROM match_team_stats WHERE {}""".format(where_clause))
            # print(cursor.mogrify(dup_match_sql, match_team_stats))
            dup_matches = db_object.read_all_data(dup_match_sql, {"match_id": match_id,
                                                                  "team_name": team_name})

        if len(dup_matches) == 0:
            query = sql.SQL("INSERT INTO match_team_stats ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, team_match_stats)),
                sql.SQL(", ").join(map(sql.Placeholder, team_match_stats))
            )
            print(f"Match ID: {match_id} team: {team_name} stats to be inserted!")
        elif len(dup_matches) == 1 and update_exist_data:
            update_cols = sql.SQL(", ").join(sql.Composed(
                [sql.Composed([sql.Identifier(k),
                               sql.SQL("="),
                               sql.Placeholder(k)]) for k, v in team_match_stats.items()]))
            if isinstance(where_clause, sql.Composable):
                where_clause = db_object.as_string(where_clause)
            test_where = sql.SQL(where_clause)

            query = sql.SQL("UPDATE match_team_stats SET {} WHERE {}").format(update_cols, test_where)
            print(f"Updating existing stats data for match ID: {match_id}, team: {team_name}")
        elif len(dup_matches) > 1:
            print(f"THERE ARE MULTIPLE records for team stats for match ID {match_id}, team: {team_name}.")
        else:
            print(f"Match ID {match_id} team: {team_name} stats already exists in DB. Not updating it!")
        if query is not None:
            try:
                #print(db_cur.mogrify(query, team_match_stats))
                db_object.execute_query(query, team_match_stats)
            except TypeError:
                traceback.print_exc()
    except Exception:
        traceback.print_exc()
    finally:
        del db_object


def insert_player_match_stats(player_match_stats: dict, db_object=None, update_exist_data: bool = False):
    if db_object is None:
        db_object = PostgresDB()

    team_name = player_match_stats.get("team_name", None)
    player_name = player_match_stats.get("player_name", None)
    match_id = player_match_stats.get("match_id", None)
    # TODO find better solution to replace NaNs with Nones
    player_match_stats = {k: (v if not isinstance(v, float) or not pd.isna(v) else None)
                          for k, v in player_match_stats.items()}
    where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                     sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                       player_match_stats.items()]))
    dup_match_sql = sql.SQL("SELECT * FROM match_player_stats WHERE {}".format(
        db_object.as_string(where_clause)
    ))
    dup_matches = db_object.read_all_data(dup_match_sql, player_match_stats)
    query = None
    try:
        if len(dup_matches) == 0:
            query = sql.SQL("INSERT INTO match_player_stats ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, player_match_stats)),
                sql.SQL(", ").join(map(sql.Placeholder, player_match_stats))
            )
            print(f"Match ID: {match_id}, team: {team_name}, player {player_name} stats to be inserted!")
        elif len(dup_matches) == 1 and update_exist_data:
            update_cols = sql.SQL(", ").join(sql.Composed(
                [sql.Composed([sql.Identifier(k),
                               sql.SQL("="),
                               sql.Placeholder(k)]) for k, v in player_match_stats.items()]))
            if isinstance(where_clause, sql.Composable):
                where_clause = db_object.as_string(where_clause)
            test_where = sql.SQL(where_clause)

            query = sql.SQL("UPDATE match_player_stats SET {} WHERE {}").format(update_cols, test_where)
            print(f"Updating existing stats data for match ID: {match_id}, team: {team_name}, player: {player_name}")
        elif len(dup_matches) > 1:
            print(
                f"THERE ARE MULTIPLE player stats records for match ID {match_id}, team: {team_name}, player: {player_name}.")
        else:
            print(f"Match ID {match_id} team: {team_name}, player: {player_name} stats already exists in DB. "
                  f"Not updating it!")
        if query is not None:
            try:
                # print(db_cur.mogrify(query, player_match_stats))
                db_object.execute_query(query, player_match_stats)
                # db_cur.execute(query, player_match_stats)
            except TypeError:
                traceback.print_exc()
    except Exception:
        traceback.print_exc()
    finally:
        del db_object


def insert_match(match: dict, db_object=None, update_exist_data: bool = False):
    if db_object is None:
        db_object = PostgresDB()

    where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                     sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                       match.items()]))
    dup_match_sql = sql.SQL("SELECT * FROM match WHERE {}".format(
        db_object.as_string(where_clause)
    ))
    dup_matches = db_object.read_all_data(dup_match_sql, match)
    query = None
    try:
        if len(dup_matches) == 0:
            # Find duplicates using datetime home team and away team only
            # Change where clause in case there is dup matches
            where_clause = "\"match_date\"=%(match_date)s AND \"home_team\"=%(home_team)s AND \"away_team\"=%(away_team)s"
            dup_match_sql = sql.SQL("""SELECT * FROM match WHERE {}""".format(where_clause))
            # print(cursor.mogrify(dup_match_sql, match))
            db_object.read_all_data(dup_match_sql, {"match_date": match.get("match_date", None),
                                                    "home_team": match.get("home_team", None),
                                                    "away_team": match.get("away_team", None)})
        if len(dup_matches) == 0:
            query = sql.SQL("INSERT INTO match ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, match)),
                sql.SQL(", ").join(map(sql.Placeholder, match))
            )
            print("MATCH to be inserted!")
        elif len(dup_matches) == 1 and update_exist_data:
            update_cols = sql.SQL(", ").join(sql.Composed(
                [sql.Composed([sql.Identifier(k),
                               sql.SQL("="),
                               sql.Placeholder(k)]) for k, v in match.items()]))
            query = sql.SQL("UPDATE match SET {} WHERE {}").format(update_cols, where_clause)
            print("Match already exists")
        elif len(dup_matches) > 1:
            print(f"THERE ARE MULTIPLE MATCHES")
        else:
            print("Match already exists in DB. Not updating it!")
        if query is not None:
            try:
                db_object.execute_query(query, match)
            except TypeError:
                traceback.print_exc()
    except Exception:
        traceback.print_exc()
    finally:
        del db_object
