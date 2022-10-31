import traceback

import psycopg2
from psycopg2 import _psycopg, sql, extras

from settings.settings import EIHLStatsDBConfig


def db_connection() -> _psycopg.connection:
    db_conn = None
    try:
        db_conn = psycopg2.connect(dbname=EIHLStatsDBConfig.db,
                                   user=EIHLStatsDBConfig.un,
                                   password=EIHLStatsDBConfig.pw,
                                   host=EIHLStatsDBConfig.hostname)
    except psycopg2.DatabaseError:
        print("ERROR connecting to EIHL Stats Database!!")
    return db_conn


def db_cursor(db_conn: _psycopg.connection) -> _psycopg.cursor:
    db_cur = None
    try:
        db_cur = db_conn.cursor(cursor_factory=extras.DictCursor)
    except psycopg2.Error:
        print("ERROR creating DB Cursor!")
    return db_cur


def insert_championship(champ: dict, cursor: _psycopg.cursor, db_conn: _psycopg.connection = None):
    where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                     sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                       champ.items()]))
    dup_match_sql = sql.SQL("SELECT * FROM championship WHERE {}".format(
        where_clause.as_string(db_conn)
    ))
    print(cursor.mogrify(dup_match_sql, champ))
    cursor.execute(dup_match_sql, champ)
    dup_matches = cursor.fetchall()
    try:
        if len(dup_matches) == 0:
            insert_sql = sql.SQL("INSERT INTO championship ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, champ)),
                sql.SQL(", ").join(map(sql.Placeholder, champ))
            )
            cursor.execute(insert_sql, champ)
    except Exception as e:
        print(e)
    else:
        db_conn.commit()
    print("Championship has been inserted!")


def insert_match(match: dict, cursor: _psycopg.cursor, db_conn: _psycopg.connection = None,
                 update_exist_data: bool = False):
    where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                     sql.SQL("="), sql.Placeholder(k)]) for k, v in
                                                       match.items()]))
    dup_match_sql = sql.SQL("SELECT * FROM match WHERE {}".format(
        where_clause.as_string(db_conn)
    ))
    cursor.execute(dup_match_sql, match)
    dup_matches = cursor.fetchall()
    query = None
    try:
        if len(dup_matches) == 0:
            # Find duplicates using datetime home team and away team only
            where_clause = sql.SQL(" AND ").join(sql.Composed([sql.Composed([sql.Identifier(k),
                                                                             sql.SQL("="), sql.Placeholder(k)]) for k, v
                                                               in
                                                               match.items()]))
            dup_match_sql = sql.SQL("""SELECT * FROM match WHERE "match_date" = %(match_date)s AND
             "home_team" = %(home_team)s AND "away_team" = %(away_team)s """
                                    )
            # print(cursor.mogrify(dup_match_sql, match))
            cursor.execute(dup_match_sql, {"match_date": match.get("match_date", None),
                                           "home_team": match.get("home_team", None),
                                           "away_team": match.get("away_team", None)})
            dup_matches = cursor.fetchall()

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
                print(cursor.mogrify(query, match))
                cursor.execute(query, match)
            except TypeError:
                traceback.print_exc()
    except Exception:
        traceback.print_exc()
        raise
    else:
        db_conn.commit()

