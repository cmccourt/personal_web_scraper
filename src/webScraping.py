import re
import traceback
from collections import defaultdict
from datetime import datetime
from typing import Callable

import pandas as pd
import bs4
import requests
from bs4 import BeautifulSoup
from psycopg2 import _psycopg

from settings.settings import eihl_match_url
from src.eihl_stats_db import db_connection, db_cursor, insert_match


def get_eihl_championship_options():
    res_beaus = get_html_content(eihl_match_url)
    html_id_season = res_beaus.body.find(id="id_season")
    champ_list = []
    id_search = "id_season="
    for s_id in html_id_season.find_all("option"):
        champ_id = s_id.get("value")
        if not champ_id:
            continue
        try:
            champ_id = int(champ_id[champ_id.find(id_search) + len(id_search):])
            champ_list.append({"eihl_web_id": champ_id, "name": s_id.get_text()})
        except ValueError:
            print(traceback.print_exc())
        print(s_id)
    return champ_list


def get_date_format(text: str, format: str) -> datetime or None:
    try:
        text_dt = datetime.strptime(text, format)
        return text_dt
    except ValueError:
        return None


def get_html_content(url: str) -> BeautifulSoup:
    response = requests.get(url)
    res_beaus = BeautifulSoup(response.content, 'html.parser')
    return res_beaus


def populate_all_eihl_matches(db_cur: _psycopg.cursor = None, db_conn: _psycopg.connection = None):
    month_id = 999
    team_id = 0
    matches = []
    if db_cur is None:
        if db_conn is None:
            db_conn = db_connection()
        db_cur = db_cursor(db_conn)
    try:
        # get all EIHL season ids to iterate through
        db_cur.execute("SELECT champion_id, eihl_web_id FROM championship WHERE eihl_web_id <> 36")
        seasons = db_cur.fetchall()
        for season in seasons:
            season_url = f"{eihl_match_url}?id_season={season[1]}&id_team={team_id}&id_month={month_id}"
            season_matches = get_matches(season_url)
            for match in season_matches:
                match.update({"championship_id": season[1]})
                insert_match(match, db_cur, db_conn, True)

    except Exception as e:
        traceback.print_exc()
        print(f"{e}")
        raise e


def get_eihl_web_match_id(match_html) -> int or None:
    a_tag = match_html.find("a")
    game_web_id = re.findall(r"(?<=/game/).*$", a_tag.get('href', None))[0]
    try:
        if game_web_id:
            return game_web_id
    except Exception:
        print(f"{game_web_id} is not a valid number")
        traceback.print_exc()
    return None


def get_matches(url: str, filt: Callable[[str], bool] = lambda x: True):
    res_beaus = get_html_content(url)

    html_content = res_beaus.find("body").find("main").find(class_="wrapper")
    html_content = html_content.find(class_="container-fluid text-center text-md-left")

    matches = []
    match_date = None
    for tag in html_content:

        game_date_text = tag.get_text()
        game_date = get_date_format(game_date_text, "%A %d.%m.%Y")
        if game_date is not None and match_date != game_date_text:
            match_date = game_date

        if tag.name == "div" and len(tag.find_all()) > 0:
            match_info = defaultdict()
            match_info["eihl_web_match_id"] = get_eihl_web_match_id(tag)
            match_details = [r.text.strip() for r in tag.contents]

            match_details = [x for x in match_details if x.lower() not in ("", "details")]
            try:
                match_info["match_date"] = datetime.combine(match_date,
                                                            get_date_format(match_details[0], "%H:%M").time())
            except AttributeError:
                # There was no time present. Created dummy placeholder
                match_info["match_date"] = match_date
                match_details.insert(0, None)

            # EIHL website for older seasons have game numbers or match type between time and home team
            if str.isdigit(match_details[1]) or len(match_details) > 2:
                del match_details[1]

            match_details[1] = match_details[1].replace("\n", "").strip()
            match_details[1] = re.sub('  +', '\t', match_details[1])
            match_details[1] = (match_details[1].split("\t"))
            match_info["home_team"] = match_details[1][0]
            match_info["away_team"] = match_details[1][-1]

            # if match went to OT or SO then we need to separate that
            try:
                score = match_details[1][1].split(":")
            except IndexError:
                print(type(match_details[1]))
                traceback.print_exc()
                print(match_details)
            else:
                if score[0] != "-":
                    match_info["home_score"] = int(score[0])
                    # OT or SO could be in string
                    try:
                        away_score, match_type = score[1].split(" ")
                    except ValueError:
                        away_score = score[1]
                        match_type = "R"
                    match_info["away_score"] = int(away_score)
                    match_info["match_win_type"] = match_type
                else:
                    match_info["home_score"] = None
                    match_info["away_score"] = None
                    match_info["match_win_type"] = None
                matches.append(match_info)
    return matches


def get_ignore_words():
    ignore_words = []
    try:
        with open("../settings/ignore_list.txt", "r") as ign_words_file:
            # Create a list of ignore words for comparison
            # convert word to lowercase and remove any whitespace
            ignore_words = [f"{word.lower().strip()} " for word in ign_words_file]
    except IOError:
        print("Could not find the file")
    return ignore_words


def get_page_stats(url: str):
    page_html_tables = pd.read_html(url)
    if len(page_html_tables) > 0:
        return pd.concat(page_html_tables, ignore_index=False)
    else:
        return page_html_tables


def get_page_text(url: str, ignore_words: list = None) -> str:
    response = requests.get(url)
    res_beaus = BeautifulSoup(response.content, 'html.parser')
    page_words: str = res_beaus.body.get_text()
    html_content = page_words.strip('\t\r\n')
    page_words = re.sub(r'\W', ' ', html_content)

    if ignore_words is None:
        ignore_words = get_ignore_words()
    ignore_reg = '|'.join(ignore_words)
    page_words = re.sub(ignore_reg, "", page_words)

    return page_words
