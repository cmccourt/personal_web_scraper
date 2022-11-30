import traceback

import pandas as pd
import psycopg2
from psycopg2 import _psycopg, sql, extras, pool

from settings.settings import EIHLStatsDBConfig


class PostgresDBPool:
    db_conn_pool = None

    def __init__(self):
        print("Create Postgres DB object")
        self.db_conn_pool = pool.ThreadedConnectionPool(1, 20, user=EIHLStatsDBConfig.un,
                                                        password=EIHLStatsDBConfig.pw,
                                                        host=EIHLStatsDBConfig.hostname,
                                                        port="5432",
                                                        database=EIHLStatsDBConfig.db)

    def __del__(self):
        self.release_pool()

    def release_pool(self):
        if self.db_conn_pool is not None:
            self.db_conn_pool.closeall()
        print("Postgres DB Pool is closed")


class EIHLDBHandler:

    def __init__(self, db_conn=None, db_cur=None):
        if not db_conn:
            db_conn = PostgresDBPool.db_conn_pool.getconn()
        self.db_conn = db_conn
        try:
            if not db_cur:
                db_cur = db_conn.cursor(cursor_factory=extras.DictCursor)
            self.db_cur = db_cur
        except psycopg2.Error:
            print("ERROR creating DB Cursor!")

    def shut_down(self):
        if self.db_conn:
            self.put_conn()
        if self.db_cur:
            self.db_cur.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shut_down()

    def __del__(self):
        self.shut_down()

    def put_conn(self):
        print("Put connection back into queue")
        PostgresDBPool.db_conn_pool.putconn(self.db_conn)

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
        try:
            self.db_cur.execute(query, params)
            result = self.db_cur.fetchall()
            return result
        except Exception:
            traceback.print_exc()

    def execute_query(self, query, params=None):
        try:
            self.db_cur.execute(query, params)
        except Exception:
            self.db_conn.rollback()
            traceback.print_exc()
        else:
            self.db_conn.commit()


def get_next_match():
    with EIHLDBHandler() as eihl_db:
        try:
            eihl_db.db_cur.execute("SELECT * FROM match;")
            while True:
                match = eihl_db.db_cur.fetchone()
                if match is None:
                    break
                else:
                    yield match
        except Exception:
            traceback.print_exc()


def get_eihl_season_ids():
    with EIHLDBHandler() as eihl_db:
        try:
            seasons = eihl_db.db_cur.read_all_data("SELECT champion_id, eihl_web_id FROM championship")
        except psycopg2.ProgrammingError:
            seasons = None
    return seasons


def get_db_match(params, where_clause=None):
    with EIHLDBHandler() as eihl_db:
        if where_clause is None:
            where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                             sql.SQL("="), sql.Placeholder(k)]) for k, v
                                                               in
                                                               params.items()]))
        dup_match_sql = sql.SQL("SELECT * FROM match WHERE {}".format(
            eihl_db.as_string(where_clause)
        ))
        dup_matches = eihl_db.read_all_data(dup_match_sql, params)
    return dup_matches


def get_db_match_stats(params):
    with EIHLDBHandler() as eihl_db_hndlr:
        where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                         sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                           params.items()]))
        dup_match_sql = sql.SQL("SELECT * FROM match_team_stats WHERE {}".format(
            eihl_db_hndlr.as_string(where_clause)
        ))
        try:
            match = eihl_db_hndlr.read_all_data(dup_match_sql, params)[0]
        except (psycopg2.Error, Exception):
            match = None
    return match


def insert_championship(champ: dict):
    with EIHLDBHandler() as eihl_db:
        where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                         sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                           champ.items()]))
        dup_match_sql = sql.SQL("SELECT * FROM championship WHERE {}".format(
            eihl_db.as_string(where_clause)
        ))
        dup_matches = eihl_db.read_all_data(dup_match_sql, champ)

        try:
            if len(dup_matches) == 0:
                insert_sql = sql.SQL("INSERT INTO championship ({}) VALUES ({})").format(
                    sql.SQL(", ").join(map(sql.Identifier, champ)),
                    sql.SQL(", ").join(map(sql.Placeholder, champ))
                )
                eihl_db.execute_query(insert_sql, champ)
        except Exception:
            traceback.print_exc()
        else:
            print("Championship has been inserted!")


def insert_team_match_stats(team_match_stats: dict, update_exist_data: bool = False):
    team_name = team_match_stats.get("team_name", None)
    match_id = team_match_stats.get("match_id", None)

    with EIHLDBHandler() as eihl_db:
        where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                         sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                           team_match_stats.items()]))
        dup_match_sql = sql.SQL("SELECT * FROM match_team_stats WHERE {}".format(
            eihl_db.as_string(where_clause)
        ))
        dup_matches = eihl_db.read_all_data(dup_match_sql, team_match_stats)

        query = None
        try:
            if len(dup_matches) == 0:
                # Find duplicates using match ID and team name only
                # Change where clause in case there is dup matches

                where_clause = """"match_id"=%(match_id)s AND "team_name"=%(team_name)s"""
                dup_match_sql = sql.SQL("""SELECT * FROM match_team_stats WHERE {}""".format(where_clause))
                # print(cursor.mogrify(dup_match_sql, match_team_stats))
                dup_matches = eihl_db.read_all_data(dup_match_sql, {"match_id": match_id,
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
                    where_clause = eihl_db.as_string(where_clause)
                test_where = sql.SQL(where_clause)

                query = sql.SQL("UPDATE match_team_stats SET {} WHERE {}").format(update_cols, test_where)
                print(f"Updating existing stats data for match ID: {match_id}, team: {team_name}")
            elif len(dup_matches) > 1:
                print(f"THERE ARE MULTIPLE records for team stats for match ID {match_id}, team: {team_name}.")
            else:
                print(f"Match ID {match_id} team: {team_name} stats already exists in DB. Not updating it!")
            if query is not None:
                try:
                    # print(db_cur.mogrify(query, team_match_stats))
                    eihl_db.execute_query(query, team_match_stats)
                except TypeError:
                    traceback.print_exc()
        except Exception:
            traceback.print_exc()



def insert_player_match_stats(player_match_stats: dict, update_exist_data: bool = False):
    team_name = player_match_stats.get("team_name", None)
    player_name = player_match_stats.get("player_name", None)
    match_id = player_match_stats.get("match_id", None)
    # TODO find better solution to replace NaNs with Nones
    player_match_stats = {k: (v if not isinstance(v, float) or not pd.isna(v) else None)
                          for k, v in player_match_stats.items()}
    with EIHLDBHandler() as eihl_db:
        where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                         sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                           player_match_stats.items()]))
        dup_match_sql = sql.SQL("SELECT * FROM match_player_stats WHERE {}".format(
            eihl_db.as_string(where_clause)
        ))
        dup_matches = eihl_db.read_all_data(dup_match_sql, player_match_stats)
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
                    where_clause = eihl_db.as_string(where_clause)
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
                    eihl_db.execute_query(query, player_match_stats)
                    # db_cur.execute(query, player_match_stats)
                except TypeError:
                    traceback.print_exc()
        except Exception:
            traceback.print_exc()


def insert_match(match: dict, update_exist_data: bool = False):
    with EIHLDBHandler() as eihl_db_hndlr:
        where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                         sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                           match.items()]))
        dup_matches = get_db_match(match, where_clause)
        query = None
        try:
            if len(dup_matches) == 0:
                # Find duplicates using datetime home team and away team only
                # Change where clause in case there is dup matches
                # TODO SQL INJECTION ALERT!
                where_clause = "\"match_date\"=%(match_date)s AND \"home_team\"=%(home_team)s AND \"away_team\"=%(away_team)s"
                dup_matches = get_db_match(match, where_clause)

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
                    eihl_db_hndlr.execute_query(query, match)
                except TypeError:
                    traceback.print_exc()
        except Exception:
            traceback.print_exc()
