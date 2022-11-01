from dataclasses import dataclass


@dataclass
class EIHLStatsDBConfig:
    hostname = "localhost"
    un = "eihlstats"
    pw = "20?6Lo5weLL"
    db = "eihlstats"


# TODO create team URLs here
eihl_base_url = "https://www.eliteleague.co.uk/"
eihl_team_url = f"{eihl_base_url}team/"
eihl_schedule_url = f"{eihl_base_url}schedule/"
eihl_match_url = f"{eihl_base_url}game/"

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

eihl_teams = [
    "Belfast Giants",
    "Sheffield Steelers",
    "Cardiff Devils",
    "Coventry Blaze",
    "Dundee Stars",
    "Glasgow Clan",
    "Manchester Storm",
    "Fife Flyers",
    "Guildford Flames",
    "Nottingham Panthers"
]
