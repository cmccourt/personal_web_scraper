import traceback
from pprint import pprint
from settings.settings import eihl_schedule_url
from src.eihl_stats_db import get_eihl_season_ids, insert_match
from src.webScraping import get_matches


def populate_all_eihl_matches():
    month_id = 999
    team_id = 0
    try:
        # get all EIHL season ids to iterate through
        seasons = get_eihl_season_ids()
        for season in seasons:
            season_url = f"{eihl_schedule_url}?id_season={season[1]}&id_team={team_id}&id_month={month_id}"
            season_matches = get_matches(season_url)
            for match in season_matches:
                pprint(match)
                match.update({"championship_id": season[1]})
                insert_match(match, update_exist_data=True)
    except Exception:
        traceback.print_exc()

