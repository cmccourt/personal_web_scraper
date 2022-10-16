import re
from collections import defaultdict
from datetime import datetime
from typing import Callable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from settings.settings import eihl_team_url, eihl_match_url
from webScraping import get_page_stats

"""@dataclass
class Player:
    name: str
    country: str
    dob: datetime
    stats: pd.DataFrame


@dataclass
class Team:
    name: str
    stats: pd.DataFrame
    players: [Player]
"""


def get_date_format(text: str, format: str) -> datetime or None:
    try:
        text_dt = datetime.strptime(text, format)
        return text_dt
    except ValueError:
        return None


def get_team_stats(team_name: str):
    # "https://www.eliteleague.co.uk/team/4-belfast-giants/player-stats?id_season=37"

    team_url = f"{eihl_team_url}4-belfast-giants/player-stats?id_season=37"
    response = requests.get(team_url)
    res_beaus = BeautifulSoup(response.content, 'html.parser')
    res_beaus_id_season = res_beaus.body.find(id="id_season")
    season_options = {i: x.get_text() for i, x in enumerate(res_beaus_id_season.find_all("option"))}

    # for s_id in res_beaus_id_season.find_all("option"):
    #     print(s_id)
    team_stats = get_page_stats(team_url)
    return team_stats


def get_players_stats(player_name: str, team_stats: pd.DataFrame = None):
    pass


def get_matches(url: str, filt: Callable[[str], bool]):
    if filt is None:
        filt = lambda x: True
    response = requests.get(url)
    res_beaus = BeautifulSoup(response.content, 'html.parser')
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
            match_details = [r.text.strip() for r in tag.contents]

            match_details = [x for x in match_details if x.lower() not in ("", "details")]
            match_info["match_datetime"] = datetime.combine(match_date,
                                                            get_date_format(match_details[0], "%H:%M").time())
            match_details[1] = match_details[1].replace("\n", "").strip()
            match_details[1] = re.sub('  +', '\t', match_details[1])
            match_details[1] = (match_details[1].split("\t"))
            match_info["home_team"] = match_details[1][0]
            match_info["away_team"] = match_details[1][-1]

            # if match went to OT or SO then we need to separate that
            score = match_details[1][1].split(":")
            if score[0] != "-":
                match_info["home_score"] = int(score[0])
                # OT or SO could be in string
                try:
                    away_score, match_type = score[1].split(" ")
                except ValueError:
                    away_score = score[1]
                    match_type = "R"
                match_info["away_score"] = int(away_score)
                match_info["match_type"] = match_type
            else:
                match_info["home_score"] = None
                match_info["away_score"] = None
                match_info["match_type"] = None

            print(match_info)
            matches.append(match_info)



    print("COMPLETE")

get_matches("https://www.eliteleague.co.uk/schedule?id_season=36&id_team=0&id_month=999", lambda x: True)
