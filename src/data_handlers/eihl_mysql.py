import traceback
from typing import Any, Sequence

from mysql.connector import connect, connection
from pypika import MySQLQuery, Field, Criterion
from sqlalchemy import create_engine

from settings.settings import mysql_db_config


class EIHLMysqlHandler:
    match_player_stats_cols = {
        "Jersey": "jersey",
        "Player name": "player_name",
        "Position": "position",
        "G": "goals",
        "A": "assists",
        "PTS": "points",
        "PIM": "penalty_mins",
        "PPG": "power_play_goals",
        "SHG": "short_hand_goals",
        "+/-": "plus_minus",
        "SOG": "shots_on_goal",
        "S": "shots",
        "FOW": "face_offs_won",
        "FOL": "face_offs_lost",
        "W": "wins",
        "L": "losts",
        "SO": "shutouts",
        "SA": "shots_against",
        "GA": "goals_against",
        "MIN": "mins_played",
        "SVS%": "save_percentage"
    }
    match_team_stats_cols = {
        "Shots": "shots",
        "Shots on goal": "shots_on_goal",
        "Shots efficiency": "shot_efficiency",
        "Power plays": "power_plays",
        "Power play efficiency": "power_play_efficiency",
        "Penalty minutes": "penalty_minutes",
        "Penalty kill efficiency": "penalty_kill_efficiency",
        "Saves": "saves",
        "Save percentage": "save_percentage",
        "Faceoffs won": "faceoffs_won"
    }

    def __init__(self, db_conn: connection = None, db_config=None):
        if db_config is None:
            db_config = mysql_db_config
        self.db_conn: connection = db_conn
        if not self.db_conn:
            self.db_conn = connect(**db_config)
            self.db_engine = create_engine(
                f"mysql+pymysql://{db_config.get('un', '')}:{db_config.get('pw', '')}@"
                f"{db_config.get('host', '')}:{db_config.get('port', '')}/{db_config.get('db', '')}",
                echo=True)

    def shut_down(self):
        if self.db_conn:
            self.db_conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shut_down()

    def __del__(self):
        self.shut_down()

    def print_sql_query(self, query, params):
        if self.db_conn:
            print(self.db_conn.mogrify(query, params))
        else:
            print("ERROR unable to print SQL! No DB connection available!\n")

    def fetch_all_data(self, query: MySQLQuery or str = None, params: dict = None,
                       table: str = None, columns: list = None):
        try:
            if query is None:
                query = MySQLQuery.from_(table)
                query = query.select(*columns) if columns is not None else query.select("*")
            if not isinstance(query, str):
                query = str(query)
            with self.db_conn.cursor(dictionary=True) as db_cur:
                db_cur.execute(query, params)
                result = db_cur.fetchall()
                return result
        except Exception:
            traceback.print_exc()

    def execute_query(self, query, params=None):
        with self.db_conn.cursor(dictionary=True) as db_cur:
            try:
                db_cur.execute(query, params)
            except Exception:
                self.db_conn.rollback()
                traceback.print_exc()
                print(f"Query -> {query} \n Params -> {params}")
                # self.print_sql_query(query, params)
            else:
                self.db_conn.commit()

    def insert_data(self, table_name: str, new_val_dict: dict):

        query = str(MySQLQuery.into(table_name).columns(list(new_val_dict.keys())).insert(list(new_val_dict.values())))
        try:
            # print(db_cur.mogrify(query, player_match_stats))
            self.execute_query(query)
        except TypeError:
            traceback.print_exc()

    def update_data(self, table_name: str, update_values: dict, where_clause: Criterion = None):
        try:
            new_query = MySQLQuery.update(table_name)
            for key in update_values:
                new_query = new_query.set(key, update_values[key])
            if where_clause:
                new_query = new_query.where(where_clause)
            else:
                for col in update_values:
                    new_query = new_query.where(Field(col) == update_values[col])
            print(f"Updating existing stats data for Values: {update_values}")
            # print(db_cur.mogrify(query, player_match_stats))
            self.execute_query(str(new_query), update_values)
        except TypeError:
            traceback.print_exc()

    def get_dup_records(self, params: dict = None, query=None, table: str = None,
                        where_clause: Criterion = None) -> Sequence[Any]:
        if params is None:
            params = {}
        if query is not None:
            records = self.fetch_all_data(query)
        else:
            dup_match_query = MySQLQuery.from_(table).select("*")
            if where_clause is not None:
                dup_match_query = dup_match_query.where(where_clause)
            for col, val in params.items():
                dup_match_query = dup_match_query.where(Field(col) == val)

            records = self.fetch_all_data(str(dup_match_query), params)
        return records
