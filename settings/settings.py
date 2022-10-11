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
