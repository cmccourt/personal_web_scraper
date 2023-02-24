import re
import traceback
from collections import defaultdict
from datetime import datetime

import bs4
import pandas as pd
import requests
from bs4 import BeautifulSoup

from settings.settings import eihl_schedule_url, eihl_match_url
from src.utils import extract_date_from_str, extract_float_from_str, get_html_content, get_start_end_dates_from_str_list


def get_eihl_championship_options(schedule_url: str = eihl_schedule_url):
    res_beaus = get_html_content(schedule_url)
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


def get_start_end_dates_from_gamecentre(schedule_url: str) -> tuple[datetime or None, datetime or None]:
    start_date = None
    end_date = None
    # Get container that holds matches
    res_beaus = get_html_content(schedule_url)
    html_content = res_beaus.find("body").find("main").find(class_="wrapper")
    html_content = html_content.find(class_="container-fluid text-center text-md-left")
    html_text: str = html_content.get_text(separator=",", strip=True)
    start_date, end_date = get_start_end_dates_from_str_list(html_text)
    return start_date, end_date


def extract_team_match_stats(match_html: str) -> dict:
    # find div class called container
    # Then find an H2 called Team Stats
    # Iterate through the stats and assign them to the right team
    match_html_content = get_html_content(match_html)
    home_team_stats = defaultdict()
    away_team_stats = defaultdict()

    html_container = match_html_content.findAll('div', attrs={'class': 'container'})[0]
    stats_html: bs4.Tag = html_container.find("div")
    # E.g. of values that match Regex: 20, 20.0, 20%, 20.06%
    stat_float_regex = r"(\d+(\.\d+)?%)|(\d+(\.\d+))|(\d+)"
    if "TEAM STATS" in stats_html.find("h2").get_text().upper():
        # Found the team stats section
        stat_list = list(stats_html.get_text("|", strip=True).split("|"))
        if stat_list[0].lower() == "team stats":
            del stat_list[0]
        get_team_stats_from_list(away_team_stats, home_team_stats, stat_float_regex, stat_list)

    match_team_stats = {"home_team": home_team_stats, "away_team": away_team_stats}
    return match_team_stats


def get_team_stats_from_list(away_team_stats, home_team_stats, stat_float_regex, stat_list):
    def assign_stat_to_team(header, team_stats, stats=None, stat_index=None, value=None):
        if stats is not None and stat_index is not None:
            try:
                value = stats[stat_index]
            except IndexError:
                value = None
        team_stats[header] = extract_float_from_str(value) or 0.0

    stat_list_len = len(stat_list)
    i = 0
    stat_header = None
    while i < stat_list_len:
        stat_value = stat_list[i]
        if not re.findall(stat_float_regex, stat_value):
            stat_header = stat_value
            # Add stats for category
            assign_stat_to_team(stat_value, home_team_stats, stat_list, i + 1)
            assign_stat_to_team(stat_value, away_team_stats, stat_list, i + 2)
        elif stat_header is None:
            # find the category this number belongs too
            next_header_index = next((j for j in range(i, stat_list_len)
                                      if not re.findall(stat_float_regex, stat_list[j])))
            stat_header = stat_list[next_header_index]
            assign_stat_to_team(stat_header, home_team_stats, value=stat_value)
            assign_stat_to_team(stat_header, away_team_stats, stat_list, i + 1)
            i = next_header_index + 1
            stat_header = None
            continue
        i += 1


def get_eihl_web_match_id(match_row_html: bs4.Tag) -> int or None:
    a_tag = match_row_html.find("a")
    try:
        game_web_id = re.findall(r"(?<=/game/).*$", a_tag.get('href', None))[0]
        return game_web_id
    except Exception:
        print(f"{game_web_id} is not a valid number")
        traceback.print_exc()
    return None


def get_match_html_tags(url: str, html_content: BeautifulSoup = None) -> list[(bs4.Tag, datetime.date)]:
    if html_content is None:
        res_beaus = get_html_content(url)

        html_content = res_beaus.find("body").find("main").find(class_="wrapper")
        html_content = html_content.find(class_="container-fluid text-center text-md-left")

    matches = []
    match_date = None
    for tag in html_content:
        game_date_text = tag.get_text()
        game_date = extract_date_from_str(game_date_text, "%A %d.%m.%Y")
        if game_date is not None and match_date != game_date_text:
            match_date = game_date

        if tag.name == "div" and len(tag.find_all()) > 0:
            matches.append((tag, match_date))
    return matches


def get_matches(url: str, html_content: BeautifulSoup = None, start_date: datetime = datetime.min,
                end_date: datetime = datetime.max, teams: list or tuple = None):
    if teams is None:
        teams = []
    if html_content is None:
        res_beaus = get_html_content(url)

        html_content = res_beaus.find("body").find("main").find(class_="wrapper")
        html_content = html_content.find(class_="container-fluid text-center text-md-left")

    gamecentre_date_fmt = "%A %d.%m.%Y"
    matches = []
    match_date = None
    for tag in html_content:
        tag_text = tag.get_text()

        game_date = extract_date_from_str(tag_text, gamecentre_date_fmt)
        if game_date is not None and match_date != tag_text:
            match_date = game_date
        if match_date is not None:
            if start_date > match_date or (teams and not [x for x in teams if x.lower() in tag_text.lower()]):
                continue
            elif match_date > end_date:
                break
        if tag.name == "div" and len(tag.find_all()) > 0:
            match_info = extract_match_info_from_tag(tag)
            match_info["match_date"] = match_date
            try:
                match_info["match_date"] = datetime.combine(match_date, match_info.pop("match_time"))
            except (AttributeError, TypeError, KeyError):
                # no time present. Created dummy placeholder
                match_info["match_date"] = match_date
            matches.append(match_info)
    return matches


def extract_match_info_from_tag(tag) -> dict:
    match_info = defaultdict()
    match_info["eihl_web_match_id"] = get_eihl_web_match_id(tag)
    match_details = [r.text.strip() for r in tag.contents]
    match_details = [x for x in match_details if x.lower() not in ("", "details")]

    try:
        match_info["match_time"] = extract_date_from_str(match_details[0], "%H:%M").time()
    except AttributeError:
        # no time present. Created dummy placeholder
        match_info["match_time"] = None
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
    return match_info


def get_match_player_stats(url: str) -> defaultdict[pd.DataFrame]:
    response = requests.get(url)
    res_beaus = BeautifulSoup(response.content, 'html.parser')
    html_container = res_beaus.find('div', attrs={'class': 'container'})
    # html_container: bs4.Tag = html_container.find("div")
    game_stats = defaultdict(pd.DataFrame)
    pg_header_regex = re.compile("(?<= -).players|(?<= -).goalies")
    all_player_headers = [x for x in html_container.find_all("h2")
                          if re.search(pg_header_regex, x.get_text()) is not None]
    head_index = 0
    player_head_len = len(all_player_headers)
    while head_index < player_head_len:
        next_head_tag = None
        table_tag = None
        player_stat_dtf = pd.DataFrame()
        team_name = all_player_headers[head_index].get_text().strip().split("-")[0]
        team_name = team_name.strip()
        # Working with 0 index notation
        if head_index + 1 < player_head_len:
            next_head_tag = all_player_headers[head_index + 1]
        for tag in all_player_headers[head_index].next_siblings:
            if next_head_tag is not None and tag is next_head_tag:
                break
            elif isinstance(tag, bs4.Tag):
                table_tag = tag.findChildren("table", limit=1)
                if table_tag is not None:
                    table_tag = tag
                    break
        # TODO Change if statement to try except
        if isinstance(table_tag, bs4.Tag):
            try:
                player_stat_dtf = pd.read_html(str(table_tag))[0]
            except ValueError:
                print(f"ERROR No tables found for: {table_tag}")

            game_stats[team_name] = pd.concat([game_stats[team_name], player_stat_dtf], ignore_index=False)
            try:
                game_stats[team_name]["Position"] = game_stats[team_name]["Position"].fillna("GW")
            except KeyError:
                pass
        head_index += 1
    return game_stats


def get_gamecentre_month_id(month_num: int = None) -> int:
    """

    Args:
        month_num: the month of the year in number form (e.g. January = 1, March = 3, October = 10 etc)

    Returns: month ID

    """
    if month_num is None:
        return 999
    elif month_num > 12:
        print(f"Month number {month_num} is Invalid")
    return month_num


# TODO finish function
def get_gamecentre_team_id(team_name: str = None):
    if team_name is None:
        return 0
    else:
        return team_name


def get_gamecentre_url(season_id: int, team_id: int = 0, month_id: int = 999, base_url: str = eihl_schedule_url) -> str:
    season_url = f"{base_url}?id_season={season_id}&id_team={team_id}&id_month={month_id}"
    return season_url


def get_eihl_match_url(match_id: int, base_url: str = eihl_match_url) -> str:
    match_url = f"{base_url}{match_id}/team-stats"
    return match_url


def extract_match_stats(match_stats_url):
    print(f"\nNext match is {match_stats_url}\n")
    match_stats = get_match_player_stats(match_stats_url)
    # Check if the team score table came through
    if len(match_stats) > 4 and len(match_stats[0].columns) <= 4:
        del match_stats[0]
    return match_stats
